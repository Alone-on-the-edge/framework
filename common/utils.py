import sys
sys.path.append("d:\Project\ssot")

import json
import typing
import uuid
from datetime import datetime
from urllib.parse import urlparse

import boto3
import pyspark
import pytz
import requests
from botocore.exceptions import ClientError, NoCredentialsError
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType, StructField

from common.constants import GG_ILLEGAL_VS_REPLACED_CHAR
from common.context import JobContext
from common.control_spec import FlowSpec, AppSpec
from common.databricks1 import JobUtils
from common.logger import get_logger
from common.retry import retry, RetryOptions
from common.types.spark_struct import struct_from_ddl

spark = SparkSession.getActiveSession()

s3_retry_ops = RetryOptions(retryable_exception=(ClientError, NoCredentialsError))
GG_HEARTBEAT_EVENT_SCHEMA_ID = 'gg.ggs'
GG_HEARTBEAT_EVENT_TABLE = 'gg_heartbeat'
ACTIVE_BRONZE_TABLE_NAME = 'cdc_events'
INACTIVE_BRONZE_TABLE_NAME = 'inactive_cdc_events'


def get_sequencer_schema():
    return 'struct<csn decimal(38,0), pos decimal(38,0), is pk_change long>'


def get_additional_fields_schema():
    return f"""__schema_id string, __cdc_sequencer {get_sequencer_schema()}, __meta map<string, string>"""


def get_dlt_fields_schema():
    sequencer_schema = get_sequencer_schema()
    return f"""__Timestamp long, __DeleteVersion {sequencer_schema}, __UpsertVersion {sequencer_schema}"""


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
    

@retry(retry_options=s3_retry_ops)
def save_s3_object(bucket: str, key: str, content: str):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    obj.put(Body=content)


def get_region():
    ec2_metadata = JobContext.get_ec2_metadata()
    region = ec2_metadata['region']
    return region


def to_yyyymm(timestamp: datetime) -> int:
    if timestamp is None:
        raise ValueError("Unsupported none value for timestamp")

    return int(str(timestamp.year) + str(timestamp.month).rjust(2,'0'))    


def raise_alert(msg, priority= 'P3'):
    job_details = JobUtils().get_current_job_details()
    databricks_workspace = spark.conf.get("spark.datavricks.workspaceUrl").split('.')[0]

    job_metadata = f"""\n Job Details\n
    \tJob Id: {job_details["job_id"]}
    \tRun Id: {job_details["run_id"]}
    \tDatabricks Workspace: {databricks_workspace}\n
    """
    alert_description = msg[0:14500] + job_metadata
    payload = {
        "message" : "DataFlow Alert",
        "description" : alert_description,
        "priority" : priority
    }

    response = requests.post('https://api.opsgenie.com/v2/alerts',
                             headers = {"Content-Type" : "application/json",
                                        "Authorization" : "GenieKey dd345393-454353-353058305"},
                            data = json.dumps(payload))
    
    get_logger('alert-logger').info(f"alert created with requestId={response.json()['requestId']}")


class FlowErrorLogger:
    TBL_SCHEMA = """
    error_id STRING NOT NULL,
    app STRING NOT NULL,
    flow_id STRING NOT NULL,
    error_desc STRING NOT NULL,
    error_trace STRING NOT NULL,
    error_metadata MAP<STRING, STRING> NOT NULL,
    created_by STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    yyyymm INT NOT NULL,
    """

    def __init__(self, componenet_name: str, job_ctx: JobContext):
        self.job_ctx = job_ctx
        self.component_name = componenet_name
        self.job_details = JobUtils().get_current_job_details()
        self._spark = SparkSession.getActiveSession()
        self._logger = get_logger('flow-error-logger')

    def log(self, flow_id: str, error_desc: str, error_trace: str,
            error_metadata = None, should_alert: bool = True):
        if error_metadata is None:
            error_metadata = {}

        error_id = str(uuid.uuid4())
        now_ts = datetime.now(tz=pytz.UTC)
        error_meta = {**error_metadata, **self.job_details}
        error_logs_tbl = self.job_ctx.get_flow_errors_tbl()
        (
            self.spark.crateDataFrame([(error_id, self.job_ctx.app, flow_id, error_desc, error_trace,
                                        error_meta, self.component_name, now_ts, to_yyyymm(now_ts))],
                                        schema = FlowErrorLogger.TBL_SCHEMA)
            .write.mode("append")
            .partitionBy("app", "created_by", "yyyymm")
            .saveAsTable(error_logs_tbl)

        )
        self._logger.error(
            f"Flow={flow_id} failed with error. \n {error_desc} \n. See below for error trace. \n {error_trace}"
        )
        if should_alert:
            raise_alert(msg=f"Error Occured in {self.component_name} with error_id={error_id}."
                        f"Query table {error_logs_tbl} with this error_id for more details.",
                        priority='P1')
    

def resolve_schema_path(app_schemas_dir, db_schema_tbl, schema_fingerprint):
    tbl_name = resolve_table_name(db_schema_tbl)
    return f"{app_schemas_dir}/{tbl_name}/schema.{db_schema_tbl}.{schema_fingerprint}.avsc"


def is_active_flow(spec: FlowSpec):
    return spec.target_details.schrema_refresh_done is True and spec.is_active is True


def is_gg_heartbeat_event(schema_id: str, table_name: str):
    if schema_id is None or table_name is None:
        raise ValueError("Unsupported none value for schema_id and table")
    
    if schema_id == GG_HEARTBEAT_EVENT_SCHEMA_ID and table_name.startswith(GG_HEARTBEAT_EVENT_TABLE):
        return True
    else:
        return False
    

def resolve_table_name(normalized_key):
    if normalized_key is None:
        raise ValueError("Unsupported none value for normalized key")
    table_name: str = normalized_key.split('.')[-1]
    #chars like $ in table names are replaced in GG because it's not a valid char in avro schema
    #avro schema exception is thrown when $ is encountered.
    #we replace $ with  __DOL in GG to avoid this
    #undo the replacement here because in our metadata table we would have the table with $ sign.
    for illegal_char, replaced_char in GG_ILLEGAL_VS_REPLACED_CHAR.items():
        table_name = table_name.replace(replaced_char.lower(), illegal_char.lower())
    return table_name


def sanitize_column_name(col_name: str, preserve_case: bool):
    for illegal_char, replaced_char in GG_ILLEGAL_VS_REPLACED_CHAR.items():
        col_name = col_name.replace(illegal_char, replaced_char)
    return col_name if preserve_case else col_name.lower()


def cast_with_cdc_schema(tbl_df: DataFrame, cdc_schema_ddl: str):
    cdc_schema = struct_from_ddl(cdc_schema_ddl) # gets converted to struct
    cdc_schema_cols = set(cdc_schema.fieldNames())
    tbl_schema_cols = set(tbl_df.schema.fieldNames())
    #1st we find common_columns and select only those common columns from the existing table as df.
    common_cols = cdc_schema_cols.intersection(tbl_schema_cols)
    tbl_df = tbl_df.select(*common_cols)
    #2nd we find new columns by doing a diff b/w cdc_schema & table_schema
    new_cols = cdc_schema_cols.difference(tbl_schema_cols)

    #3rd for every new col, we add that col to df from 1st (add null as value for new col because the col didnt
    # exist till now)
    for col in new_cols:
        tbl_df = tbl_df.withColumn(col, F.lit(None))
    
    #4th we iterate over every col in the cdc_schema and cast the column in the df from 3rd to its respective type
    # defined in the cdc_schema
    for col in cdc_schema.fields:
        tbl_df = tbl_df.withColumn(col.name, F.col(col.name).cast(col.dataType))

    # 5th, we again run a select cdc_schema_cols on the df from 4th preserve order of columns.
    tbl_df = tbl_df.select(cdc_schema.fieldNames())

    return tbl_df

def convert_int_to_bool(value: int):
    if value == 0:
        return False
    elif value == 1:
        return True
    else:
        raise ValueError(f"Unable to conver {value} to boolean type")


def split_s3_path(s3_path):
    url = urlparse(s3_path, allow_fragments=False)
    bucket, key = url.netloc,url.path.lstrip('/')
    return bucket, key


def s3_path_exists(bucket: str, prefix: str) -> bool:
    s3 = boto3.client('s3')
    list_objs_resp = s3.list_objects(Bucket=bucket, Prefix=prefix, MaxKeys = 1)
    return 'Contents' in list_objs_resp


def broadcast_tbl_vs_inactive_schemas(app_spec: AppSpec):
    active_flows = [flow for flow in app_spec.flow_specs if flow.is_active]
    return spark.sparkContext.broadcast(dict(
        (flow.src_table, flow.inactive_schemas) for flow in active_flows
    ))

def broadcast_db_schema_vs_id(job_ctx: JobContext):
    db_schema_spec_tbl = job_ctx.get_db_schema_spec_tbl()
    df = (spark.table(db_schema_spec_tbl).where(f"app = '{job_ctx.app}'") \
            .select("id",
                    F.lower("src_db").alias("src_db"),
                    F.lower("src_schema").alias("src_schema")) \
                    .distinct().cache())
    df_cnt = df.count()

    if df_cnt != df.select("id").distinct().count():
        raise ValueError(f"Duplicate ids found in {db_schema_spec_tbl}")
    
    if df_cnt != df.select("src_db", "src_schema").distinct().count():
        raise ValueError(f"Multiple ids are assigned to the same src_db & src_schema")
    
    return spark.sparkContext.broadcast(
        dict([(f"{row['src_db']}.{row['src_schema']}", row["id"]) for row in df.collect()])
    )


@retry(retry_options=s3_retry_ops)
def get_s3_object_content_if_exists(bucket: str, key: str):
    s3 = boto3.resource('s3')
    try:
        s3.Object(bucket, key).load()
    except ClientError as e:
        if e.response["Error"]["code"] == "404":
            return None
        else:
            raise
    resp = s3.Object(bucket, key).get()
    return resp['Body'].read().decode('utf-8').strip()
        
    

