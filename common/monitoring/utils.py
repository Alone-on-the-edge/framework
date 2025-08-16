from datetime import datetime

from common.databricks1 import JobUtils
from common.logger import get_logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, MapType

from common.utils import raise_alert, to_yyyymm

spark = SparkSession.getActiveSession()

def alert_heartbeat_failure(unhealthy_replicats_collection, env, app):
    err_msg = f"{env.upper()}: Heartbeat Monitoring has identified unhealthy replicats for {app.upper()}\n"

    for row in unhealthy_replicats_collection:
        unhealthy_replicat = row[1]
        affected_db = row[0]

        err_msg += f"""\tReplicat: {unhealthy_replicat}, Affected Database: {affected_db}
                    """
    
    raise_alert(err_msg)

def validate_db_replicat_map(df):
    # Filter the combinations with more than one replicat 
    combinations = df.groupBy('DB', 'SCHEMA').agg(F.collection_list('REPLICAT').alias('replicat_list')) 
    join_udf = F.udf(lambda x: ",".join(x))
    combinations = combinations.withColumn("replicats", join_udf(F.col("replicat_list"))) \
                               .withColumn("DB_SCHEMA", F.concat_ws('_', F.col("DB"), F.col("SCHEMA"))) \
                               .withColumn("DB_SCHEMA_REPLICAT", F.concat_ws(', ', F.col("DB_SCHEMA"), F.col("replicats")))
    invalid_combinations = combinations.filter(F.size(F.col('replicat_list')) > 1)
    if invalid_combinations.count() > 0:
        # Invalid combination found, raise an exception
        db_schema_replicats = '\n'.join(invalid_combinations.rdd.map(lambda row: row['DB_SCHEMA_REPLICAT']).collect()) 
        raise Exception(f"Found multiple replicats for one db schema combination: \n{db_schema_replicats}")
    
def to_yyyymmdd(timestamp: datetime) -> int:
    if timestamp is None:
        raise ValueError("Unsupported none value for timestamp")
    
    return int(str(timestamp.year) + str(timestamp.month).rjust(2,0) + str(timestamp.day).rjust(2,0))

@F.udf
def get_yyyymmdd():
    return to_yyyymmdd(datetime.utcnow())

@F.udf
def get_yyyymm():
    return to_yyyymm(datetime.utcnow())

@F.udf
def configure_alert(alert_status, db, replicat, env):
    if (alert_status is not None) and int(alert_status) == 1:
        return True
    else:
        return False
    
# evaluate if the time difference between the arrival of the latest heartbeat event > threshold
@F.udf
def evaluate_event_time_diff(time_diff, threshold):
    if time_diff is None:
        return 1
    else:
        if float(time_diff) > threshold: # checking if the time difference is beyond threshold
            return 1 # alert
        else:
            return 0

def get_current_user():
    current_user = (spark.sql(select current_user)).collect()[0][0]
    return current_user

def get_job_metadata():
    metadata = JobUtils().get_current_job_details()
    run_id = metadata['run_id']
    job_id = metadata['job_id']

    return(job_id, run_id)

def optimize_table(table, app, optimize_cols_list):
    optimize_cols = (",").join(optimize_cols_list)
    spark.sql(f"""OPTIMIZE {table} WHERE app = '{app}' ZORDER BY ({optimize_cols})""")


def remove_void_dbs(sor_db_list):
    # some dbs might not have any tables to be queried
    # removing such dbs 
    non_empty_db_dicts = [db_dict for db_dict in sor_db_list if db_dict.get('tables') is not None]
    if len(non_empty_db_dicts) == 0:
        raise Exception("No tables to query")
    
    return non_empty_db_dicts


def with_duration_logger(function: callable, **args):
    # takea a function and its arguments 
    # executes the function, logs its execution time and returns its results 
    function_start_time = datetime.now()

    if len(args) == 0:
        result = function()
    else:
        result = function(**args)

    function_end_time = datetime.now()
    get_logger(__name__).info(
        f"Execution time of {function.__name__}: {(function_end_time - function_start_time).total_seconds()} seconds")
    
    return result


@F.udf(returnType=MapType(StringType(), StringType()))
def get_error_msg_map(error_msg_delta, error_msg_src):
    if error_msg_delta is None and error_msg_src is None:
        return None
    else:
        error_msg_map = {
            "error_msg_delta" : None,
            "error_msg_src" : None
        }

        if error_msg_delta is not None:
            error_msg_map["error_msg_delta"] = error_msg_delta
        
        if error_msg_src is not None:
            error_msg_map["error_msg_src"] = error_msg_src
        
        return error_msg_map