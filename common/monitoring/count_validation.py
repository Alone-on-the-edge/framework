#Databricks notebook source
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pyspark.sql.functions as F
from databricks_cli.sdk.api_client import ApiClient
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Longtype, TimestampType, IntegerType, DoubleType, StructType, StructField, StringType

from common.context import Jobcontext
from common.databricks1 import NotebookConfig, get_db_utils
from monitoring.metadata_handler import DeltaDbMetadata, SorConfigReader, SorMetadata, get_sor_delta_tables, \
    check_l2l_status
from monitoring.parallel_query_runner import ParallelQueryRunner
from monitoring.utils import get_job_metadata, optimize_table, get_yyyymmdd, remove_void_dbs, with_duration_logger, \
    get_error_msg_map


@F.udf
def calculate_count_diff(src_count: int, delta_count: int):
    if src_count is None or delta_count is None:
    #src_count is  None. when a table is dropped in sor db
    #delta_count is None when it is not ingested
    #in both these cases, there is no need to track count diff
        return None
    
    return int(src_count - delta_count)


@F.udf
def get_mismatch_percentage(mismatch_count: int, src_count: int):
    if mismatch_count is None:
        #cases where calculate_count_diff returns None
        return None
    
    if src_count != 0:
        return float((mismatch_count) / src_count ) * 100
    else:
        if mismatch_count == 0:
            return 0
        return float(sys.maxsize)
    

@F.udf
def is_error(is_error_delta, is_error_src):
    if is_error_delta is None:
        return is_error_src
    
    if is_error_src == 1 or is_error_delta == 1:
        return 1
    else:
        return 0
    

@F.udf(returnType=Longtype())
def correct_delta_count(delta_count, is_error_delta):
    #when delta table has 0 records for a schema, it returns null for delta count
    #the sor count, when table is empty wil be 0
    # making delta count consistent
    # null count should only be reported when the query fails
    if is_error_delta is None or is_error_delta == 0:
        if delta_count is None:
            return 0
        else:
            return delta_count
    else:
        return delta_count
    

def get_delta_count_schema(ingestion_mode):
    if ingestion_mode == "s3":
        schema_column = "schema_id"
    elif ingestion_mode == "dataflow":
        schema_column = "__schema_id"
    else:
        raise ValueError("Unsupported value for ingestion_mode")
    
    delta_count_schema = StructType([
       StructField("delta_count", Longtype(), True),
       StructField("schema_column", StringType(), True),
       StructField("delta_table", StringType(), True),
       StructField("query", StringType(), True),
       StructField("is_error_delta", IntegerType(), True),
       StructField("error_msg_delta", StringType(), True)
    ])

    return delta_count_schema


def process_delta_count(ingestion_mode, delta_count_results):
    delta_count_schema = get_delta_count_schema(ingestion_mode)
    if ingestion_mode == "s3":

        delta_count_df = spark.createDataFrame(delta_count_results, get_delta_count_schema(ingestion_mode))
        delta_count_results_df = delta_count_df.withColumn("app", F.lit(app)) \
        .withColumnRenamed("query", "delta_query") \
        .withColumn("delta_schema_id",
                    F.lower(F.concat(F.col("schema_id"), F.lit("."), F.col("delta_table")))). \
                    withColumn("inflight_count", F.lit(0).cast(Longtype()))
        
    elif ingestion_mode == "dataflow":
        delta_count_schema.add('inflight_count', Longtype(), True)
        delta_count_df = spark.createDataFrame(delta_count_results, schema=delta_count_schema)
        delta_count_df = delta_count_df.withColumnRenamed("query", "delta_query")

        # dataflow has uuids for schema ids
        # to get schema names, joining with __db_schema_spec
        db_schema_spec_df = spark.read.table(f"{meta_db}.__db_schema_spec") \
            .filter(F.lower(F.col("app")) == F.lit(app))
        
        delta_count_results_df = delta_count_df.join(F.broadcast(db_schema_spec_df), 
                                                     delta_count_df["__schema_id"] == db_schema_spec_df["id"], "left") \
                .withColumn("delta_schema_id_table",
                            F.concat(F.col("src_db"), F.lit("."), F.col("src_schema"), F.lit("."), F.col("delta_table"))) \
                .drop("id", "src_db", "src_schema")
    
    else:
        raise ValueError(f"Unsupported value for ingestion_mode: {ingestion_mode}")
    
    return delta_count_results_df

def split_delta_count_results(delta_count_results_df: DataFrame) -> tuple:
    succeeded_delta_count_df = delta_count_results_df.filter(F.col("is_error_delta") != 1) \
        .drop("error_msg_delta")
    
    failed_delta_count_df = delta_count_results_df.filter(F.col("is_error_delta") == 1) \
        .select(
            F.col("delta_table").alias("failed_delta_table"),
            F.col("is_error_delta"),
            F.col("error_msg_delta")
        )

    return (succeeded_delta_count_df, failed_delta_count_df)


def normanize_delta_counts(succeeded_delta_count_df: DataFrame) -> DataFrame:
    #Multiple clients can exist in single schema
    #delta counts are grouped by sgdp_org_id for the sors where it is enabled in s3 ingestion.
    #summing counts grouping by schema_id.table so that we can report counts on schema.table level
    delta_schema_level_count_df = (
        succeeded_delta_count_df.groupBy(F.col("delta_schema_id_table"))
        .agg(F.sum(F.col("delta_count")).alias("schema_level_count"))
        .select(
            F.col("delta_schema_id_table").alias("schema_id_table"),
            F.col("schema_level_count").alias("delta_count")
        )
    )
    
    delta_schema_table_df = succeeded_delta_count_df.select(F.col("delta_schema_id_table"),
                                                            F.col("is_error_delta")).dropDuplicates()
    
    delta_count_df = delta_schema_level_count_df.join(
        delta_schema_table_df,
        delta_schema_table_df["delta_schema_id_table"] == delta_schema_level_count_df["schema_id_table"],
        "left",
    ).drop("schema_id_table")

    return delta_count_df


def join_sor_delta_results(sor_count_results_df: DataFrame, delta_count_df: DataFrame,
                           delta_group_by_col: str) -> DataFrame:
    delta_join_column = "delta_schema_id_table"

    if ingestion_mode == "s3" and delta_group_by_col not in ('sgdp_org_id', 'client_id'):
        # tables ingested through s3 ingestion has schema_id = schema name
        # db name for a row in the table cant be figured out
        sor_join_column = "schema_table"
    else:
        # tables ingested through dataflow ingestion has schema_id = db.schema
        # we can construct a db.schema.table field and join on it
        sor_join_column = "db_schema_table"

        # empty delta tables will return null
        # processing such rows so that delta counts show up as 0 and not nulls
        join_count_df = sor_count_results_df.join(
            F.broadcast(delta_count_df),
            F.lower(delta_count_df[delta_join_column]) == F.lower(sor_count_results_df[sor_join_column]), "left") \
            .withColumn("delta_count", correct_delta_count(F.col("delta_count"), F.col("is_error_delta"))) \
            .drop("is_error_delta")
        
        return join_count_df
    

def get_sor_count_schema(client_id_col: str):
    # count query result can have one or two columns
    # adding the client id column to the schema if it exists

    qry_result_schema = StructType([
        StructField("src_count", Longtype(), True),
        StructField("arc_count", Longtype(), True)
    ])

    if client_id_col is not None:
        qry_result_schema.add(StructField(client_id_col, StringType(), True))

    qry_metadata_schema = StructType([
        StructField("id", StringType(), True),
        StructField("src_db", StringType(), True),
        StructField("src_schema", StringType(), True),
        StructField("src_table", StringType(), True),
        StructField("query", StringType(), True),
        StructField("is_error_src", IntegerType(), True),
        StructField("error_msg_src", StringType(), True),
        StructField("attempt_num", StringType(), True),
        StructField("query_duration_seconds", StringType(), True)
    ])

    sor_count_schema = StructType(qry_result_schema.fields + qry_metadata_schema.fields)

    return sor_count_schema
    

def process_sor_counts(sor_count_results):
    sor_count_schema = get_sor_count_schema(client_id_col)
    sor_count_results_df = spark.createDataframe(sor_count_results, schema=sor_count_schema)

    sor_count_results_df = sor_count_results_df \
        .withColumnRenamed("query", "src_query") \
        .withColumn(
            "db_schema_table",
            F.lower(
                F.concat(
                    F.col("src_db"),
                    F.lit("."),
                    F.col("src_schema"),
                    F.lit("."),
                    F.col("src_table")
                )
            ),
        ).withColumn("schema_table", F.lower(F.concat(F.col("src_schema"), F.lit("."), F.col("src_table"))),)
    
    return sor_count_results_df


def get_job_triggered_at(run_id):
    workspace = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
    notebook_context = get_db_utils().notebook.entry_point.getDbutils().notebook().getContext()
    token = notebook_context.apiToken().get()
    api_client = ApiClient(host=workspace, token=token)
    job_details = api_client.perform_query('GET', f"jobs/runs/get", data={'run_id': run_id}, headers=None)
    start_time = datetime.utcfromtimestamp(job_details['start_time'] / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')
    return start_time

def get_cursor_df(replication_lag_df: DataFrame, delta_results_df: DataFrame) -> DataFrame:
    cursor_df = (
        replication_lag_df.alias("r")
        .join(
            F.broadcast(delta_results_df.alias("d")),
            (F.col("d.delta_table") == F.col("r.table_name")) & (F.col("d.__schema_id") == F.col("r.schema_id")),
            "left",
        )
        .where(
            "cast(r.metadata.silver_max_bronze_created_at as timestamp) <= cast(d.query_exec_time as timestamp)"
        )
        .groupBy("r.table_name", "r.schema_id")
        .agg(F.max(F.coalesce("metadata.silver_max_op_ts", F.lit("1970-01-01 00:00:00"))).alias("max_op_ts"))
    )

    return cursor_df

def get_lower_bound(replication_lag_df: DataFrame) -> str:
    lower_bound = (
        replication_lag_df
        .where("metadata.silver_max_op_ts is not null and metadata.silver_max_op_ts != '1970-01-01 00:00:00'")
        .withColumn(
            "min_silver_op_ts", F.col("metadata.silver_max_op_ts").cast("timestamp")
        )
        .select ("min_silver_op_ts")
        .agg(F.min("min_silver_op_ts").alias("lower_bound"))
    ).first()['lower_bound']

    return lower_bound

def calculate_inflight_count(bronze_df: DataFrame, cursor_df: DataFrame, delta_results_df: DataFrame) -> DataFrame:
    bronze_df.createOrReplaceTempView("cdc_events")
    cursor_df.createOrReplaceTempView("op_ts_cursor")
    delta_results_df.createOrReplaceTempView("delta_count_results")

    inflight_count_df = spark.sql(
        """
        select
            src_table,
            c.schema_id,
            coalesce(
            sum(
                case
                when d.delta_count is null then 0
                when c.parsed_payload.op_ts <= greatest(d.max_op_ts, o.max_op_ts) then 0
                when parsed_payload.op_type = 'I' then 1
                when parsed_payload.op_type = 'D' then -1
                else 0
                end
            ),
            0
            ) as inflight_count
        from
            cdc_events c
            join delta_count_results d on c.src_table = d.delta_table and c.schema_id = d.__schema_id
            left join op_ts_cursor o on o.table_name = c.src_table and o.schema_id = c.schema_id
        group by src_table, c.schema_id
        """
    )
    return inflight_count_df


if __name__ == "__main__":
    spark = SparkSession.getActiveSession()
    job_context =  Jobcontext.from_notebook_config()

    ingestion_mode = NotebookConfig.get_arg("ingestion_mode", "dataflow")
    function = NotebookConfig.get_arg("function", "count")
    num_parallel_connections = int(NotebookConfig.get_arg("num_parallel_connections", 50))
    num_parallel_db_connections = int(NotebookConfig.get_arg("num_parallel_db_connections", 50))
    degree_of_parallelism = int(NotebookConfig.get_arg("degree_of_parallelism", 4))
    max_timeout_seconds = int(NotebookConfig.get_arg("max_timeout_seconds", 300))
    max_timeout_seconds_incr = int(NotebookConfig.get_arg("max_timeout_seconds_incr", 900))
    max_no_of_attempts = int(NotebookConfig.get_arg("max_no_of_attempts", 4))
    run_mode = NotebookConfig.get_arg("run_mode", "all_tables").lower()
    sor_config_path = NotebookConfig.get_arg("sor_config_path", "")

    env = job_context.env
    app = job_context.app

    meta_db = job_context.get_control_db()

    client_id_col = check_l2l_status(ingestion_mode, job_context, meta_db)

    # reading sor config managed by ingestion team in json files
    sor_config_reader = SorConfigReader(app=app, ingestion_mode=ingestion_mode, env=env,
                                        sor_config_path=sor_config_path)
    sor_config = sor_config_reader.get_sor_config()
    db_type = sor_config.get("db_type")

    sor_db_table_map, delta_tables = get_sor_delta_tables(meta_db=meta_db, function=function,
                                                          ingestion_mode=ingestion_mode, run_mode=run_mode,
                                                          job_context = job_context, client_id_col = client_id_col,
                                                          db_type = db_type)
    # getting metadata required to query delta
    delta_db_metadata = DeltaDbMetadata(app=app, env=env, function=function, ingestion_mode=ingestion_mode,
                                        tables = delta_tables, meta_db=meta_db, job_context=job_context,
                                        client_id_col=client_id_col)
    delta_db_metadata.set_properties()

    # getting metadata required to query sor
    sor_metadata = SorMetadata(
        app = app,
        db_type = db_type,
        jdbc_port = sor_config.get('jdbc_port'),
        database_property_list = sor_config.get('database_property_list'),
        secret_mode = sor_config.get('secret_mode'),
        db_table_map = sor_db_table_map,
        meta_db = meta_db,
        num_parallel_connections = num_parallel_connections,
        degree_of_parallelism = degree_of_parallelism,
        max_timeout_seconds = max_timeout_seconds,
        max_timeout_seconds_incr = max_timeout_seconds_incr,
        max_no_of_attempts = max_no_of_attempts,
        job_context = job_context,
        ingestion_mode = ingestion_mode
    )
    sor_db_list = sor_metadata.get_db_list()

    filtered_sor_db_list = remove_void_dbs(sor_db_list)

    num_parallel_db_connections = min(num_parallel_db_connections or len(filtered_sor_db_list),
                                      len(filtered_sor_db_list))
    
    parallel_query_runner = ParallelQueryRunner(target = 'sor_delta',
                                                function = function,
                                                delta_table_query_list = delta_db_metadata.query_config_list,
                                                sor_dbs = filtered_sor_db_list,
                                                num_parallel_db_connections = num_parallel_db_connections)
    
    sor_delta_count_map = with_duration_logger(parallel_query_runner.run_queries)

    # sor delta
    sor_count_results = sor_delta_count_map["sor"]
    delta_count_results = sor_delta_count_map["delta"]

    if ingestion_mode == "dataflow":
        replication_lag_df = spark.sql(f" select *"
                                       f" from {meta_db}.replication_lag where app = '{app}' "
                                       f" and ingestion_mode = 'dataflow' "
                                       f" and yyyymmdd >= date_format(now(), 'yyyymmdd') - 1")
        replication_lag_df.cache()

        schema = "delta_count long, __schema_id string, max_op_ts string, query_exec_time timestamp, delta_table string, query string, is_error_delta integer, error_msg_delta string"
        delta_results_df = spark.createDataFrame(sor_delta_count_map['delta'], schema)

        bronze_df = job_context.get_bronze_df()
        lower_bound = get_lower_bound(replication_lag_df)

        if lower_bound:
            bronze_df = bronze_df.where(f"year >= {lower_bound.year} and month >= {lower_bound.month}")

        cursor_df = get_cursor_df(replication_lag_df, delta_results_df)

        inflight_count_df = calculate_inflight_count(bronze_df, cursor_df, delta_results_df)

        delta_count_results_df = delta_results_df.alias("d").join(
            F.broadcast(inflight_count_df).alias("i"),
            (F.col("d.delta_table") == F.col("i.src_table"))
            & (F.col("d.__schema_id") == F.col("i.schema_id")),
            "left"
        ).select (
            "delta_count",
            "__schema_id",
            "delta_table",
            "query",
            "is_error_delta",
            "error_msg_delta",
            "inflight_count"
        )
        delta_count_results = delta_count_results_df.collect()

        group_by_column = delta_db_metadata.group_by_column

        delta_count_results_df = process_delta_count(ingestion_mode, app, delta_count_results)

        # sor and sor_delta
        sor_count_results_df = process_sor_counts(sor_count_results)

        # delta_count_df and sor_count_df are joined on schema_id_table group_by_column
        # failed delta queries wont have schema_id_table column so this wont come up in the join df
        # splitting succeeded and failed table results so that failure details on delta is captured
        succeeded_delta_count_df, failed_delta_count_df = split_delta_count_results(delta_count_results_df)

        # sor_delta
        join_count_df = join_sor_delta_results(sor_count_results_df, succeeded_delta_count_df,
                                               delta_db_metadata.group_by_column)
        
        # computing count difference , percentage of difference
        join_count_df = join_count_df.withColumn("src_delta_count_diff",
                                                 calculate_count_diff(F.col("src_count"), F.col("delta_count")).cast(
                                                 IntegerType())) \
                                     .withColumn("src_delta_count_diff_pct",
                                                 get_mismatch_percentage(F.col("src_delta_count_diff"), F.col("src_count")))
        
        # adding failed delta query results
        join_count_df = join_count_df.join(failed_delta_count_df,
                                           failed_delta_count_df["failed_delta_table"] == join_count_df["src_table"],
                                           "left")
        
        job_id, run_id = get_job_metadata()

        get_job_triggered_at = get_job_triggered_at(run_id)

        join_count_df = (
            join_count_df.withColumn("app", F.lit(app))
            .withColumn("created_at", F.current_timestamp())
            .withColumn("is_error", is_error(F.col("is_error_delta"), F.col("is_error_src")))
            .withColumn("error_msg", get_error_msg_map(F.col("error_msg_delta"), F.col("error_msg_rc")))
            .withColumn("yyyymmdd", get_yyyymmdd())
        )

        join_count_df = join_count_df.withColumn(
            "metadata",
            F.create_map(
                F.lit("job_id"), F.lit(job_id),
                F.lit("run_id"), F.lit(run_id),
                F.lit("src_query"), F.lit("src_query"),
                F.lit("delta_query"), F.lit("delta_query"),
                F.lit("arc_count"), F.lit("arc_count"),
                F.lit("sor_query_duration_seconds"), F.lit("query_duration_seconds"),
                F.lit("sor_attempt_num"), F.lit("attempt_num"),
                F.lit("ingestion_mode"), F.lit(ingestion_mode),
                F.lit("job_triggered_at"), F.lit(job_triggered_at)
            ),
        )

        join_count_df = join_count_df.fillna({'inflight_count' : 0})

        # constructing df to be written to the state table
        state_table_df = join_count_df.select(
            F.col("app"),
            F.lower(F.col("id")).alias("id"),
            F.lower(F.col("src_db")).alias("src_db"),
            F.lower(F.col("src_schema")).alias("src_schema"),
            F.lower(F.col("src_table")).alias("src_table"),
            F.col("src_count").cast(Longtype()),
            F.col("delta_count"),
            F.col("inflight_count"),
            F.col("src_delta_count_diff").cast(Longtype()),
            F.col("src_delta_count_diff_pct").cast(DoubleType()),
            F.col("created_at").cast(TimestampType()),
            F.col("error_msg"),
            F.col("metadata"),
            F.lit(get_yyyymmdd()).alias("yyyymmdd").cast(IntegerType())
        )

        state_table_df.cache()
        state_table_df.display()

        state_table_df.write.mode("append") \
            .saveAsTable(f"{meta_db}.datawatch_count_validation_results")
        
        optimize_table(f"{meta_db}.datawatch_count_validation_results", app, ["src_db", "src_schema", "src_table"])

        num_error_rows = state_table_df.filter(F.col("error_msg").isNotNull()) \
            .filter((~F.col("error_msg.error_msg_src").contains("table or view does not exist"))).count()
        
        if num_error_rows > 0:
            raise Exception(
                f"{num_error_rows} queries failed. please query {meta_db}.datawatch_count_validation_results for more details")
            