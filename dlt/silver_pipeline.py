# Databricks notebook source
import base64
import copy
import json
import typing
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import boto3
import pytz
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, Row, DecimalType, DoubleType, IntegralType, \
    FractionalType, ArrayType, BooleanType, _parse_datatype_string, FloatType, BinaryType, TimestampType, \
    TimestampNTZType

from common.context import JobContext
from common.control_spec import FlowSpec
from common.databricks1 import DLTPipelineUtils
from common.keygen import generate_surrogate_key
from common.udfs import generate_cdc_sequencer
from common.utils import is_active_flow, raise_alert, get_additional_fields_schema, convert_int_to_bool, \
    get_s3_object_content_if_exists, save_s3_object, delete_s3_objects
from dlt.common import payload_schema
from dlt.dataflow_pipeline import DataFlowPipeline

GG_FIELD_NAMES = ['op_ts', 'current_ts', 'pos', 'csn', 'op_type', 'is_pk_change']


def get_gg_fields(payload_field_name = 'parsed_payload'):
    list_col = []
    for name in GG_FIELD_NAMES:
        list_col.append(F.lit(name))
        if name == 'is_pk_change':
            list_col.append((F.col(f"{payload_field_name}.{name}").cast('string')))
        else:
            list_col.append((F.col(f"{payload_field_name}.{name}")))
    return list_col


def get_ts_fields():
    return [
        F.lit("bronze_created_at"),
        F.col("bronze_created_at"),
        F.lit("bronze_created_by"),
        F.col("bronze_created_by"),
        F.lit("updated_at"),
        F.current_timestamp(),
        F.lit("updated_by"),
        F.lit('silver')
    ]


def get_kafka_conf():
    list_kafka_conf = []
    kakfka_conf: dict = app_spec.kafka_conf
    for key, value in kakfka_conf.items():
        key = key.replace('kafka.','')
        list_kafka_conf.append(F.lit(key))
        list_kafka_conf.append(F.lit(value))
    return list_kafka_conf


@F.udf(returnType=payload_schema)
def write_truncate(parsed_payload, schema_id, target_table):
    """ writing the truncate event in s3 json format"""

    if parsed_payload['op_type'] == 'T':
        payload = {'schema_id' : schema_id, 'target_table' : target_table, 'csn': parsed_payload['csn'],
                   'op_ts': parsed_payload['op_ts'],
                   'current_ts': parsed_payload['current_ts'], 'op_type': parsed_payload['op_type'],
                   'pos': parsed_payload['pos'], 'txind': parsed_payload['txind'], 'xid': parsed_payload['xid']}
        
        op_ts = datetime.strptime(parsed_payload['op_ts'],"%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=pytz.UTC)
        uuid_key = str(uuid.uuid4())
        db_schema_split = schema_id.split('.')
        # db_chema_split = "ssot.test".split('.')
        key = f"{app_spec.app}/truncate_record/dlt/{op_ts.year}/{op_ts.month}/{db_schema_split[0]}_{db_schema_split[1]}_{payload['table']}_{uuid_key}.json"
        s3 = boto3.client('s3')
        s3.put_object(
            Body = json.dumps(payload),
            Bucket = job_ctx.get_landing_bucket(),
            key = key 
        )
    return parsed_payload


def get_json_and_actual_schema(cdc_schema: StructType) -> typing.Tuple[StructType, StructType]:
    """" checking if data contains binary or timestamp which provide null value if we parsed with from_json
        It will be applied on metadata --> at the time of creating the flow"""
    
    json_encoded_types = []

    for field in cdc_schema.fields:
        if isinstance(field.dataType, BinaryType):
            json_encoded_types.append(StructField(field.name, StringType()))
        elif isinstance(field.dataType, TimestampType):
            json_encoded_types.append(StructField(field.name, StringType()))
        elif isinstance(field.dataType, FractionalType):
            json_encoded_types.append(StructField(field.name, StringType()))
        elif isinstance(field.dataType, IntegralType):
            json_encoded_types.append(StructField(field.name, StringType()))
        elif isinstance(field.dataType, BooleanType):
            json_encoded_types.append(StructField(field.name, StringType()))
        elif isinstance(field.dataType, StringType):
            json_encoded_types.append(StructField(field.name, StringType()))
        else:
            raise ValueError(f"for {field.name}, expected data type: {field.dataType.typeName()} not handled in json"
                             f"encoding")
        
    return StructType(json_encoded_types), cdc_schema


def cast_str_to_decimal(value: str, decimal_type: DecimalType):
    if '.' in value:
        precision = len(value) - 1
        scale = len(value.split('.')[1])
    else:
        precision = len(value)
        scale = 0

    if precision <= decimal_type.precision and scale <= decimal_type.scale:
        return Decimal(value)
    else:
        raise ValueError(f"unable to cast {value} in {decimal_type}")
    

def handle_json_encoded_types_func(acutal_cdc_schema, json_schema_dict, row_data: Row, src_db_type: str):
    if row_data is not None:
        if isinstance(row_data, dict):
            raise ValueError(f"problem in dit: {row_data}")
        data = row_data.asDict()
        for field in acutal_cdc_schema.value.fields:
            json_schema_type = json_schema_dict.value.get(field.name)
            if field.dataType != json_schema_type:
                value = data[field.name]
                if value is not None:
                    if isinstance(field.dataType, TimestampType):
                        if src_db_type in {'postgres','mysql','sqlserver'} \
                            and not (value.replace('.','',1).isnumeric() or value.startswith('-')):
                            dt = datetime.strptime(value, '%Y-%m-%d')

                        else:
                            dt = datetime.fromtimestamp(float(value), tz=timezone.utc)

                        data[field.name] = dt
                    elif isinstance(field.dataType, BinaryType):
                        if src_db_type == 'mysql':
                            data[field.name] = bytes(value, 'utf-8')
                        else:
                            binary_data = base64.b64decode(value)
                            data[field.name] = binary_data
                    elif isinstance(field.dataType, DecimalType):
                        data[field.name] = cast_str_to_decimal(value, field.dataType)
                    elif isinstance(field.dataType, (DoubleType, FloatType)):
                        float_data = float(value)
                        data[field.name] = float_data
                    elif isinstance(field.dataType, IntegralType):
                        long_data = int(value)
                        data[field.name] = long_data
                    elif isinstance(field.dataType, BooleanType) and src_db_type in {'postgres', 'sqlserver'}:
                        binary_data = base64.b64decode(value)
                        # It is hexadecimal int convertible value ex: b'\x00'=0 and b'\x01'=1
                        int_data = int.from_bytes(binary_data, byteorder='big')
                        data[field.name] = convert_int_to_bool(int_data) 
                    else:
                        raise ValueError(f"Unexpected type {field.dataType.typeName()}. "
                                         f"Only timestamp and binary types are encoded differently in JSON")
        return data
    return row_data


def validate_schema_fingerprint_func(parsed_payload, schema_fingerprint: int, src_table: str,
                                     schema_fingerprint_set):
    if not schema_fingerprint in schema_fingerprint_set.value:
        raise ValueError(
            f"Schema fingerprint {schema_fingerprint} does not exist in "
            f"flow_spec for src_table= '{src_table}'"
        )
    return parsed_payload


def handle_pk_change_func(parsed_payload: Row, before_key_data: dict, after_key_data: dict):
    parsed_payload_list = []
    parsed_payload = parsed_payload.asDict()
    if parsed_payload['op_type'] == 'U':
        if before_key_data['__sk'] != after_key_data['__sk']:
            row_before = copy.deepcopy(parsed_payload)
            row_before['op_type'] = 'D'
            row_before['before'] = before_key_data
            row_before['after'] = None
            row_before['is_pk_change'] = True
            parsed_payload_list.append(row_before)

            row_after = copy.deepcopy(parsed_payload)
            row_after['op_type'] = 'I'
            row_after['before'] = None
            row_after['after'] = after_key_data
            row_after['is_pk_change'] = True
            parsed_payload_list.append(row_after)
        else:
            parsed_payload['before'] = before_key_data
            parsed_payload['after'] = after_key_data
            parsed_payload['is_pk_change'] = False
            parsed_payload_list.append(parsed_payload)
    
    elif  parsed_payload['op_type'] == 'I' or parsed_payload['op_type'] == 'D':
        parsed_payload['before'] = before_key_data
        parsed_payload['after'] = after_key_data
        parsed_payload['is_pk_change'] = False
        parsed_payload_list.append(parsed_payload)    
    else:
        raise ValueError(f"Unexpected op_type: {parsed_payload['op_type']}")
    
    return parsed_payload_list


class SilverDataFlowPipeline(DataFlowPipeline):
    def __init__(self, flow_spec: FlowSpec):
        self.flow_spec = flow_spec
        self.view_name = f"view_bronze_{self.flow_spec.src_table}"
        self.cdc_schema_ddl = f"{self.flow_spec.cdc_schema}, __sk string"
        # noinspection PyTypeChecker
        self.cdc_schema: StructType = _parse_datatype_string(self.cdc_schema_ddl)

    def read(self):
        dt.view(self.read_bronze,
                name = self.view_name,
                comment = f"Bronze table view on partition src_table={self.flow_spec.src_table}")
            
    def write(self):
        self.write_silver()

    def resolve_starting_bronze_timestamp(self):
        def validate_timestamp(ts: str):
            ts_format = "%Y-%m-%dT%H:%M:%S.%f%z"
            try:
                datetime.strptime(ts, ts_format)
            except ValueError:
                raise ValueError(
                    f" Invalid startingBronzeTimestamp='{starting_bronze_ts}', "
                    f" expecting the format to be '{ts_format}'"
                )
        
        storage_dir = job_ctx.get_dlt_storage(layer= 'silver',
                                                  flow_grp_id = self.flow_spec.flow_grp_id,
                                                  include_bucket = False)
        chkpt_dir = f"""{storage_dir}/checkpoints/{self.flow_spec.target_details.delta_table}"""
        saved_starting_bronze_ts_file_path = f"""{chkpt_dir}/starting_bronze_timestamp.txt"""

        saved_starting_bronze_ts = get_s3_object_content_if_exists(
            bucket = job_ctx.get_landing_bucket(),
            key = saved_starting_bronze_ts_file_path
        )
        if saved_starting_bronze_ts:
            validate_timestamp(saved_starting_bronze_ts)
        
        starting_bronze_ts = self.flow_spec.flow_reader_opts.get('startingBronzeTimestamp',
                                                                '1970-01-01T00:00:00.000+00:00').strip()
        validate_timestamp(starting_bronze_ts)

        if saved_starting_bronze_ts is None:
            save_s3_object(
                bucket = job_ctx.get_landing_bucket(),
                key = saved_starting_bronze_ts_file_path,
                content = starting_bronze_ts
            )
        elif saved_starting_bronze_ts != starting_bronze_ts:
            delete_s3_objects(
                bucket = job_ctx.get_landing_bucket(),
                prefix = chkpt_dir
            )
            save_s3_object(
                bucket = job_ctx.get_landing_bucket(),
                key = saved_starting_bronze_ts_file_path,
                content = starting_bronze_ts
            )
        return starting_bronze_ts
    
    def handle_lob_columns(self, stream: DataFrame) -> DataFrame:
        """
        Problem:
            LOB columns might be null in the Kafka payload (even though the real value exists in the source).
            This happens because GoldenGate does not always publish LOBs due to size/performance.
            If you merge this row as-is, you'll overwrite the existing LOB with null — which is incorrect.
        Solution in handle_lob_columns():
            Join the incoming stream (with potential null LOBs)
            ⮕ to
            the existing Delta table (which has the full/last known value of LOBs).

            For each LOB column:
                If new value is not null → use it.
                Else → use value from Delta.

            Select and reconstruct the record so that no LOB is lost during the merge.
        """
        lob_cols = self.flow_spec.cdc_lob_columns
        tbl_path = self.flow_spec.target_details.delta_table_path
        stream_schema = stream.schema
        all_cols = [field.name for field in stream_schema.fields]
        non_lob_cols = list(set(all_cols) - set(lob_cols))
        existing_delta_df = (
            spark.read.format("delta").load(tbl_path)
            .filter("__DeleteVersion IS NULL")
            .select("__sk", "__schema_id", *lob_cols)
        )

        list_lob_condition =  []
        for lob_col in lob_cols:
            list_lob_condition.append(
                F.when (stream[lob_col].isNotNull(), stream[lob_col]).otherwise(
                    existing_delta_df[lob_col]).alias(lob_col))
            
        select_non_lob_cols = [stream[fields] for fields in non_lob_cols]
        exclude_final_cols = ["original_key", "original_value", "__topic", "headers", "should_write_to_kafka",
                              "kafka_conf", "lob_cols", "header_value", "partition"]
        static_lob_col = [F.lit(col) for col in lob_cols]

        stream = (stream.join(existing_delta_df, ['__sk', '__schema_id'], 'left')
                  .select(*select_non_lob_cols, *list_lob_condition)
                  .withColumn("header_value", F.filter("headers", lambda kv: kv.key == 'X-LOB-RETRIES')[0].value)
                  .withColumn("kafka_conf", F.create_map(*get_kafka_conf()))
                  .withColumn("lob_cols", F.array(static_lob_col))
                  .withColumn("should_write_to_kafka", F.expr('write_to_kafka(struct(*))'))
                  .filter("should_write_to_kafka == false")
                  .select(*list(set(all_cols) - set(exclude_final_cols)))
                )
        return stream
    
    # reading bronze data here
    def read_bronze(self) -> DataFrame:
        json_schema, acutal_schema = get_json_and_actual_schema(self.cdc_schema)
        actual_cdc_schema = spark.sparkContext.broadcast(acutal_schema)
        json_schema_dict = spark.sparkContext.broadcast(
                dict((fld.name, fld.dataType) for fld in json_schema.fields))
        schema_fingerprint_set = spark.sparkContext.broadcast(
            set(fp for fp in self.flow_spec.cdc_schema_fingerprints)
        )
        key_list = spark.sparkContext.broadcast(self.flow_spec.cdc_keys)

        payload_schema_struct = StructType([
            StructField("table", StringType()),
            StructField("op_type", StringType()),
            StructField("op_ts", StringType()),
            StructField("current_ts", StringType()),
            StructField("pos", StringType()),
            StructField("xid", StringType()),
            StructField("csn", StringType()),
            StructField("txind", StringType()),
            StructField("primary_keys", ArrayType(StringType())),
            StructField("before", acutal_schema),
            StructField("after", acutal_schema),
            StructField("is_pk_change", BooleanType()),
        ])

        @F.udf(returnType=payload_schema)
        def validate_schema_fingerprint(parsed_payload, schema_fingerprint, src_table):
            return validate_schema_fingerprint_func(parsed_payload, schema_fingerprint, src_table,
                                                        schema_fingerprint_set)
        
        @F.udf(returnType=actual_cdc_schema.value)
        def handle_json_encoded_types(row_data: Row):
            return handle_json_encoded_types_func(actual_cdc_schema, json_schema_dict, row_data, app_spec.src_db_type)
        
        @F.udf(returnType=actual_cdc_schema.value)
        def create_surrogate_key(before_or_after):
            if before_or_after is not None:
                surrogate_key = generate_surrogate_key(before_or_after, key_list.value)
                if surrogate_key is None:
                    raise ValueError(f"problem in keygen: {surrogate_key}")
                before_or_after['__sk'] = surrogate_key
                return before_or_after
            
        @F.udf(returnType=StructType([
            StructField(fld.name, TimestampNTZType(), fld.nullable, fld.metadata)
            if fld.dataType == TimestampType() else fld 
            for fld in actual_cdc_schema.value.fields
        ]))
        # this is done to avoid the year 0001 serialization error with timestamp data type.type
        # this error occurs when a timestamp object with uear 00001 is passed to a py-UDF
        def cast_ts_to_tsntz(before_or_after):
            return before_or_after
        
        @F.udf(returnType=ArrayType(payload_schema_struct))
        def handle_pk_change(parsed_payload, before_key_data, after_key_data):
            return handle_pk_change_func(parsed_payload, before_key_data, after_key_data)
        
        default_reader_opts = {"ignoreChanges": "true",
                               "ignoreDeletes": "true",
                               "maxFilesPerTrigger": 50000,
                               "maxBytesPerTrigger": 2048 * 1000 * 1000}
        starting_bronze_ts = self.resolve_starting_bronze_timestamp()
        # noinspection PytypeChecker
        stream = (
            spark.readStream
            .format("delta")
            .options(**{**default_reader_opts, **self.flow_spec.flow_reader_opts})
            .table(f'{app_spec.delta_db}.cdc_events')
            .where(f"lower(src_table) = '{self.flow_spec.src_table}'")
            .where(f"lower(bronze_created_at) >= '{starting_bronze_ts}'")
            .where(f"parsed_payload.csn is not null")
            .filter(~F.lower("schema_id").isin(self.flow_spec.inactive_schemas))
            .withColumn("parsed_payload",
                        validate_schema_fingerprint("parsed_payload", "value.schema_fingerprint", "src_table"))
            .where("parsed_payload.op_type in ('I','U','D')")
            .withColumn("before", handle_json_encoded_types(F.from_json("parsed_payload.before", json_schema)))
            .withColumn("after", handle_json_encoded_types(F.from_json("parsed_payload.after", json_schema)))
            .withColumn("before", cast_ts_to_tsntz(create_surrogate_key('before')))
            .withColumn("after", cast_ts_to_tsntz(create_surrogate_key('after')))
            .withColumn("parsed_payload", handle_pk_change("parsed_payload","before","after"))
            .withColumn("parsed_payload",F.explode("parsed_payload"))
            .withColumn("final_payload",
                        F.when(
                            (F.col("parsed_payload.op_type") == "I") | (
                                F.col("parsed_payload.op_type") == "U"),
                                F.col("parsed_payload.after"))
                         .when( F.col("parsed_payload.op_type") == "D", F.col("parsed_payload.before")))
            .withColumn("decoded_csn",F.expr(f"decode_csn(parsed_payload.csn, '{app_spec.src_db_type}')"))
            .withColumn("__cdc_sequencer", generate_cdc_sequencer(
                "decoded_csn",
                "parsed_payload.pos",
                "parsed_payload.op_type",
                "parsed_payload.is_pk_change"
            ))
        )

        common_cols = ["final_payload.*", "parsed_payload.op_type",
                       F.col("schema_id").alias("__schema_id"), "__cdc_sequencer",
                       F.create_map(*get_gg_fields(), *get_ts_fields()).alias("__meta")]
        
        if len(self.flow_spec.cdc_lob_columns) > 0:
            lob_include_cols = ["original_key","original_value","headers", "__topic", "partition"]
            stream = stream.withColumnRenamed("topic", "__topic").select(*lob_include_cols, *common_cols)
            return self.handle_lob_columns(stream)
        else:
            return stream.select(*common_cols)
        # .withColumn("parsed_payload", write_truncate('parsed_payload', 'schema_id','target_table'))
        # overwrite parsed payload to execute write_truncate 

    def write_silver(self):
        delta_tbl_name = self.flow_spec.target_details.delta_table
        delta_tbl_schema = f"{self.cdc_schema_ddl}, {get_additional_fields_schema()}"

        dt.create_streaming_live_table(
            name = delta_tbl_name,
            schema = delta_tbl_schema,
            spark_conf = self.flow_spec.flow_spark_conf,
            partition_cols = self.flow_spec.target_details.partition_cols,
            path = self.flow_spec.target_details.delta_table_path,
            table_properties = {
                "delta.enableChangeDataFeed": "true",
                **app_spec.silver_table_props
            }
        )
        dt.apply_changes(
            target = delta_tbl_name,
            source = self.view_name,
            keys = [F.col("__sk"), F.col("__schema_id")],
            sequence_by = F.col("__cdc_sequencer"), #Revisit field for sequencing data
            ignore_null_updates = False,
            apply_as_deletes = F.expr("op_type = 'D'"),
            except_column_list=[F.col("op_type")]
        )


def invoke_dlt_pipeline():
    pipe_flow_grp_id: int = 0
    try:
        pipe_flow_grp_id = int(pipeline_api.get_arg_from_tags('flow_grp_id'))
        pipeline_flows = [flow for flow in app_spec.flow_specs if
                          flow.flow_grp_id== pipe_flow_grp_id and is_active_flow(flow)]
        
        for flow in pipeline_flows:
            silver_pipe = SilverDataFlowPipeline(flow)
            silver_pipe.read()
            silver_pipe.write()
    except Exception as ex:
        raise_alert(
            f"Silver pipeline is failed for app: {job_ctx.app}, env:{job_ctx.env} and group_id:{pipe_flow_grp_id}")
        raise ex
    

if __name__ == '__main__':
    import dlt as dt

    pipeline_api = DLTPipelineUtils()
    spark = SparkSession.getActiveSession()
    job_ctx = JobContext.from_pipeline_tag()
    spark.sparkContext._jvm.com.adp.ssot.SparkuserDefinedFunctions.register()
    app_spec = job_ctx.get_app_spec()
    invoke_dlt_pipeline()

