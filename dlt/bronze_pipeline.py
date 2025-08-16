# Databricks notebook source
"""writes data mainly into cdc_events and inactive_cdc_events from kafka topic
If bronze reads from kafka topic? what is the use of schema writer?
"""
import base64
import json
from datetime import date, datetime
from decimal import Decimal
from io import BytesIO

import boto3
import fastavro
import pytz
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StringType

from common.context import JobContext
from common.lrucache import LRUCache
from common.retry import retry
from common.udfs import get_table_name, struct_wrapper, normalize_message_key, is_active_event_udf, \
    get_schema_id_udf
from common.utils import Singleton, resolve_schema_path, s3_retry_ops, ACTIVE_BRONZE_TABLE_NAME, \
    INACTIVE_BRONZE_TABLE_NAME, raise_alert, split_s3_path, broadcast_tbl_vs_inactive_schemas, \
    broadcast_db_schema_vs_id
from dlt.common import payload_schema
from dlt.dataflow_pipeline import DataFlowPipeline
from common.logger import get_logger

logger = get_logger("ssot.dlt.bronze_pipeline")


class AvroSchemaUtil(metaclass=Singleton):
    cache = LRUCache(max_size=2048)

    def get_schema(self, db_schema_tbl: str, schema_fingerprint: str, is_active_event: bool):
        @retry(retry_options=s3_retry_ops)
        def get_schema_from_s3(schema_path):
            s3 = boto3.resource('s3')
            obj = s3.Object(bucket_name=job_ctx.get_landing_bucket(), key=schema_path)
            response = obj.get()
            return str(response['body'].read().decode('utf-8'))
        
        # try to get schema from cache 
        schema = self.cache.get(schema_fingerprint)
        # get_logger('get_schema').warn(f"Cache Metrics = {self.cache.get_cache_metrics()}")
        if schema is None:
            # cache_misses.add(1)
            schemas_dir = (
                job_ctx.get_schemas_dir(include_bucket=False) if is_active_event
                else job_ctx.get_inactive_schemas_dir(include_bucket=False)
            )
            schema_file_path = resolve_schema_path(schemas_dir, db_schema_tbl, schema_fingerprint)
            schema = get_schema_from_s3(schema_file_path)
            self.cache.put(schema_fingerprint, schema)
            return schema
        else:
            # schema found in cache, so return schema
            # cache_hits.add(1)
            return schema
        

wrapper_schema = '''
    {
        "type": "record",
        "name": "generic_wrapper",
        "namespace": "oracle.goldengate",
        "fields": [{
        "name": "table_name",
        "type": "string"
        },{
        "name": "schema_fingerprint",
        "type": "long"
        },{
        "name": "payload",
        "type": "bytes"
        }]
    }
    '''


@F.udf(returnType=struct_wrapper)
@retry(retry_options=s3_retry_ops)
def handle_large_event(value):
    if value.schema_fingerprint == 0 and value.table_name =='__S3_PATH_REF__':
        full_file_path = ''
        if isinstance(value.payload, (bytes, bytearray)):
            full_file_path = ''
        s3 = boto3.resource('s3')
        bucket, key = split_s3_path(full_file_path)
        obj = s3.Object(bucket_name= bucket, key=key)
        response = obj.get()
        msg_value = response['Body'].read()
        parsed_schema = fastavro.parse_schema(json.loads(wrapper_schema))
        message_bytes = BytesIO(msg_value)
        parsed_payload = fastavro.schemaless_reader(message_bytes, parsed_schema)
        return parsed_payload
    else:
        return value
    

def handle_toast_datum(payload: dict, src_db_type: str):
    if src_db_type == 'postgres' and payload['op_type'] == 'U':
        after_dict: dict = payload['after']
        before_dict: dict = payload['before']

        for key, value in after_dict.items():
            if isinstance(value, str) and value == 'unchanged-toast-datum':
                after_dict[key] = before_dict[key]
        payload['after'] = after_dict
    return payload


def convert_datatypes_to_str(data: dict):
    for key, value in data.items():
        if value is not None:
            if isinstance(value, bool):
                data[key] = str(value)
            if isinstance(value, (int, float, Decimal)):
                data[key] = str(value)
            elif isinstance(value, (bytes, bytearray)):
                data[key] = base64.b64encode(value).decode('utf-8')
            elif isinstance(value, (date, datetime)):
                # encode datetime fields as timestamp with micro-seconds precision
                data[key] = str(value.replace(tzinfo=pytz.UTC).timestamp())
            elif not isinstance(value, str):
                raise ValueError(f"datatype: {type(value)} not handled in bronze layer")
            
    return data


# check if primary key change in update, create 2 records 1 for delete(before) and another for insert(after)
# call get_surrogate_key to generate the surrogate key 

def convert_to_json_dump(parsed_payload):
    if parsed_payload['before'] is not None:
        parsed_payload['before'] = json.dumps(convert_datatypes_to_str(parsed_payload['before']))
    if parsed_payload['after'] is not None:
        parsed_payload['after'] = json.dumps(convert_datatypes_to_str(parsed_payload['after']))
    return parsed_payload


@F.udf(returnType=payload_schema)
# parse payload using fastavro
def from_avro_custom(db_schema_tbl: str, msg_value, schema_fingerprint: str,
                     is_active_event: bool):
    try:
        schema = AvroSchemaUtil().get_schema(db_schema_tbl, schema_fingerprint, is_active_event)
    except Exception as e:
        logger.error(f"key not found for table: {db_schema_tbl} and schema_fingerprint:{schema_fingerprint}")
        raise e
    parsed_schema = fastavro.parse_schema(json.loads(schema))
    message_bytes = BytesIO(msg_value)
    avro_dict = fastavro.schemaless_reader(message_bytes, parsed_schema)
    avro_dict = handle_toast_datum(avro_dict, app_spec.src_db_type)
    return convert_to_json_dump(avro_dict)


class BronzeDataFlowPipeline(DataFlowPipeline):

    def __init__(self):
        self.view_name = 'kafka_view'
    
    def read(self):
        dt.view(self.read_kafka,
                name = self.view_name,
                comment = f"Bronze Kafka View")
    
    def write(self):
        self.write_bronze()

    def read_kafka(self) -> DataFrame:
        kafka_conf = app_spec.kafka_conf
        kafka_conf['subscribe'] = app_spec.cdc_topics[0]
        default_reader_opts = {
            'startingOffsets': 'earliest',
            'maxOffsetsPerTrigger' : 10 * 1000 * 1000
        }
        bronze_reader_opts = app_spec.bronze_reader_opts
        if 'preserve_case' in bronze_reader_opts:
            bronze_reader_opts.pop('preserve_case')

        reader_opts = {**default_reader_opts, **bronze_reader_opts}
        return(
            spark.readStream
            .format("kafka")
            .options(**{**kafka_conf, **reader_opts})
            .option("includeHeaders", True)
            .load()
            .withColumn("original_key", F.col("key"))
            .withColumn("original_value", F.col("value"))
            .withColumn("key", F.col("key").cast(StringType()))
            .withColumn("key", normalize_message_key("key"))
            .withColumn("value", from_avro("value", wrapper_schema))
            .withColumn("value", handle_large_event("value"))
            .withColumn("src_table", get_table_name("key"))
            .withColumn("schema_id", get_schema_id_udf(db_schema_vs_id,"key"))
            .withColumn("is_active_event", is_active_event_udf(tbl_vs_inactive_schemas, "src_table", "schema_id"))
            .withColumn("parsed_payload",
                        from_avro_custom("key", "value.payload", "value.schema_fingerprint", "is_active_event"))
            .withColumn("year", F.year(F.to_date("parsed_payload.op_ts")))
            .withColumn("month", F.month(F.to_date("parsed_payload.op_ts")))
            .withColumn("bronze_created_at", F.current_timestamp())
            .withColumn("bronze_created_by", F.lit("bronze"))
        )

def write_bronze(self) -> None:
    partition_cols = ['src_table', 'schema_id', 'year', 'month']
    default_spark_conf = {'spark.sql.files.maxRecordsPerFile' : str(1000 * 1000)}
    active_spark_conf = {**default_spark_conf, **app_spec.bronze_spark_conf.active_flow_conf}
    inactive_spark_conf = {**default_spark_conf, **app_spec.bronze_spark_conf.inactive_flow_conf}

    def read_active():
        return dt.read_stream(self.view_name).where("is_active_event = true")
    
    dt.table(read_active,
             name = ACTIVE_BRONZE_TABLE_NAME,
             partition_cols = partition_cols,
             spark_conf = active_spark_conf,
             table_properties = app_spec.bronze_table_props,
             comment = "Bronze active events table")
    
    def read_in_active():
        return dt.read_stream(self.view_name).where("is_active_event = false")
    
    dt.table(read_in_active,
             name = INACTIVE_BRONZE_TABLE_NAME,
             partition_cols = partition_cols,
             spark_conf = inactive_spark_conf,
             table_properties = app_spec.bronze_table_props,
             comment = "Bronze inactive events table")


def invoke_dlt_peipeline():
    try:
        bronze_pipe = BronzeDataFlowPipeline()
        bronze_pipe.read()
        bronze_pipe.write()
    except Exception as ex:
        raise_alert(f"Bronze pipeline is failed for app:{job_ctx.app} and env: {job_ctx.env}")
        raise ex
    

tbl_vs_inactive_schemas = None
tbl_vs_cdc_keys = None

if __name__ == '__main__':
    import dlt as dt

    spark = SparkSession.getActiveSession()
    job_ctx = JobContext.from_pipeline_tag()
    app_spec = job_ctx.get_app_spec()
    tbl_vs_inactive_schemas = broadcast_tbl_vs_inactive_schemas(app_spec)
    db_schema_vs_id = broadcast_db_schema_vs_id(job_ctx)

    invoke_dlt_peipeline()