import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F 
from pyspark.sql.types import StructType, StringType, StructField, TimestampType
from pyspark.sql.window import Window

from common.context import JobContext
from common.databricks1 import NotebookConfig
from common.logger import get_logger
from monitoring.utils import alert_heartbeat_failure, configure_alert, \
validate_db_replicat_map, evaluate_event_time_diff


# computes time difference between the arrival of the latest heartbeat event and the current time 
@F.udf
def compute_time_diff(bronze_created_at):
    time_diff = (datetime.utcnow() - bronze_created_at).total_seconds()
    return time_diff


# before and after fields in parsed payload are jsons stored as strings 
# this udf gets the replicat, db, and heartbeat timestamp from the after field 
@F.udf(
    returnType = StructType(
        [
            StructField("incoming_replicat", StringType(), True),
            StructField("remote_database", StringType(), True),
            StructField("heartbeat_timestamp", StringType(), True),
        ]
    )
)
def get_replicat_db(payload):
    payload_dict = json.loads(payload['after'])
    replicat = payload_dict['incoming_replicat']
    db = payload_dict['remote_database']
    heartbeat_ts = payload_dict['heartbeat_timestamp']
    heartbeat_ts = str(datetime.fromtimestamp(float(heartbeat_ts)))

    result = {
        "incoming_replicat" : replicat,
        "remote_database" : db,
        "heartbeat_timestamp" : heartbeat_ts
    }
    return result

if __name__ == "__main__":
    job_context = JobContext.from_notebook_config()
    app = job_context.app
    threshold = NotebookConfig.get_arg('threshold', '300')
    # for comparision, we need threshold in float 
    threshold = float(threshold)
    env = job_context.env 
    bronze_db = job_context.get_app_spec().delta_db
    meta_db = job_context.get_control_db()

    get_logger('property_logger')\
    .info(f"app : {app}, env: {env}, bronze_db: {bronze_db}, meta_db: {meta_db}, threshold: {threshold}s")

    current_time = datetime.now()
    new_time = current_time - relativedelta(months=1)

    max_year, max_month, min_year, min_month, = current_time.year, current_time.month, new_time.year, new_time.month

    if min_year == max_year:
        year_month_condition = (
            (F.col("year") == min_year) &
            (F.col("month") >= min_month) &
            (F.col("month") <= max_month) 
        )
    else:
        year_month_condition = (
            ((F.col("year") == min_year) & (F.col("month") >= min_month)) |
            ((F.col("year") == max_year) & (F.col("month") <= max_month)) |
            ((F.col("year") > min_year) & (F.col("year") < max_year))
        )
    bronze_df = job_context.get_bronze_df().where((F.col("src_table") == "gg_heartbeat_history") & year_month_condition)
    bronze_df = bronze_df.select(bronze_df.bronze_created_at, get_replicat_db(bronze_df.parsed_payload).alias("db_replicat"))
    bronze_df = bronze_df.select("bronze_created_at", ("db_replicat.*"))

    # grouping by db, replicat, and adding row_num ordered by bronze_current_ts
    windowSpec = \
    Window \
        .partitionBy("remote_database", "incoming_replicat") \
        .orderBy(F.col("bronze_created_at").desc())
    
    latest_heartbeat_df = bronze_df.withColumn("row_number", F.row_number().over(windowSpec))


    # getting the latest current_ts for every db, replicat combination
    latest_heartbeat_df = latest_heartbeat_df.where(F.col("row_number") == 1)
    latest_heartbeat_df = latest_heartbeat_df.withColumn("time_diff", compute_time_diff(F.col("bronze_created_at")))

    db_replicat_df = spark.read.table(f"{meta_db}.db_replicat_map")\
                          .filter(F.lower(F.col("SORDB")) == f"{app}")\
                          .filter(F.lower(F.col("INGESTION_MODE")) == "dataflow")
    
    active_df = db_replicat_df.filter(F.col("ACTIVE") == "Y").withColumnRenamed("remote_database","remote_db")

    validate_db_replicat_map(active_df) #Makes sure that there exists a unique db-schema, replicat combination

    # Joining latest_heartbeat_df with db replicat map 

    joined_df = F.broadcast(latest_heartbeat_df)\
                 .join(active_df,
                       (latest_heartbeat_df['incoming_replicat'] == active_df['REPLICAT']) &
                       (latest_heartbeat_df['remote_database'] == active_df['remote_db']),
                       "right")
    
    # heartbeat delay is computed 
    # checks if alert need to be sent
    alert_df = joined_df.withColumn("alert_status", evaluate_event_time_diff(F.col("time_diff"), F.lit(threshold)))\
                .withColumn("status", F.when(F.col("alert_status") == "1", "RED").otherwise("GREEN"))\
                .withColumn("is_alert_raised", configure_alert(F.col("alert_status"), F.col("remote_database"), F.col(
                    "incoming_replicat"), F.lit(env)))\
                .withColumn("record_date", F.current_timestamp())\
                .withColumn("app", F.lit(app))
    
    # preparing df to be written to state table
    state_append_df = alert_df.select(
        F.lower(F.col("remote_db")).alias("db"),
        F.lower(F.col("REPLICAT")).alias("replicat_name"),
        F.col("app"),
        F.col("is_alert_raised"),
        F.col("heartbeat_timestamp").alias("heartbeat_ts").cast(TimestampType()),
        F.col("record_date").alias("created_at").cast(TimestampType()),
        F.lit("dataflow").alias("ingestion_mode"),
        F.lit(F.date_format(F.current_timestamp(), "yyyyMMdd")).alias("yyyymmdd")
    )

    # caching result_df so that it doesnt get computed twice
    state_append_df.cache()

    state_append_df.display()

    # updating state table
    state_append_df.write\
        .mode("append")\
        .saveAsTable(f"{meta_db}.__gg_heartbeat_state")
    
    # getting unhealthy replicats and raising alert 
    unhealthy_replicats_collection = state_append_df.select("db", "replicat_name").filter(F.col("is_alert_raised") == "true").\
        distinct().collect()
    
    if len(unhealthy_replicats_collection) > 0:
        alert_heartbeat_failure(unhealthy_replicats_collection, env, app)
