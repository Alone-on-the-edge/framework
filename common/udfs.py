import typing
from datetime import datetime
from decimal import Decimal

import pyspark
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, BinaryType, StructField, LongType, DecimalType, BooleanType

from utils import resolve_schema_path, resolve_table_name, is_gg_heartbeat_event

struct_wrapper = StructType([
    StructField("table_name", StringType()),
    StructField("schema_fingerprint", LongType()),
    StructField("payload", BinaryType())])


@F.udf
def get_table_name(kafka_msg_key):
    return resolve_table_name(kafka_msg_key)


@F.udf
def get_schema_path(app_schemas_dir, db_schema_tbl, schema_fingerprint) -> str:
    return resolve_schema_path(app_schemas_dir, db_schema_tbl, schema_fingerprint)


sequencer_schema = StructType([StructField("csn", DecimalType(38,0), False),
                               StructField("pos", DecimalType(38,0), False),
                               StructField("is_pk_change", LongType(), False)])


@F.udf(returnType=sequencer_schema)
def generate_cdc_sequencer(cdc_csn: str, cdc_pos: str, op_type: str, is_pk_change: bool):
    if cdc_csn is not None and len(cdc_csn) >= 0:
        cdc_csn = Decimal(cdc_csn)
    else:
        raise ValueError(f"Invalid valur for csn:{cdc_csn}")
    
    if cdc_pos is not None and len(cdc_pos) >= 0:
        cdc_pos = Decimal(cdc_pos)
    else:
        raise ValueError(f"Invalid valur for pos:{cdc_pos}")
    
    pk_change_value = 0
    if is_pk_change == True:
        op_type = op_type.upper().strip()
        if op_type == 'D':
            pk_change_value = 0
        elif op_type == 'I':
            pk_change_value = 1
        else:
            raise ValueError("Expecting op_type to be I or D when is_pk_change is true")
    return cdc_csn, cdc_pos, pk_change_value


@F.udf
def normalize_message_key(kafka_msg_key: str):
    if kafka_msg_key is None:
        raise ValueError("Unsupported none value for kafka_msg_key")
    
    orig_kafka_msg_key = kafka_msg_key.lower()
    # for wfn launchpad GG hearbeat key is like C##GGS.GG_HEARTBEAT_HISTORY
    kafka_msg_key = orig_kafka_msg_key.replace('c##','',1)
    splits = kafka_msg_key.split('.')
    if len(splits) == 2 and is_gg_heartbeat_event(f"gg.{splits[0]}", splits[1]):
        return f"gg.{kafka_msg_key}"
    else:
        if len(splits) != 3:
            raise ValueError(f"Expected 3 parts in the key i.e db.schema.table for key {kafka_msg_key}")
    return orig_kafka_msg_key


def is_active_event_udf(tbl_vs_inactive_schemas: pyspark.Broadcast[typing.Dict[str, typing.List[str]]],
                        src_tbl_col: str, schema_id_col: str):
    # custom function to filter inactive table and inactive schemas in a table
    def is_active_event(src_tbl: str, schema_id: str):
        if schema_id is None or src_tbl is None:
            raise ValueError("Unsupported none value for schema_id or table")
        
        if is_gg_heartbeat_event(schema_id, src_tbl):
            return True
        if src_tbl not in tbl_vs_inactive_schemas.value:
            return False
        
        inactive_schema_list = tbl_vs_inactive_schemas.value[src_tbl]
        if schema_id in inactive_schema_list:
            return False
        else:
            return True
        
    return (
        F.udf(lambda src_tbl, sch_id: is_active_event(src_tbl, sch_id), returnType=BooleanType())
        (src_tbl_col, schema_id_col)
    )



def get_schema_id_udf(db_schema_vs_id: pyspark.Broadcast[typing.Dict[str, str]],
                      normalized_key_col: str):
    def get_schema_id(normalized_key: str):
        splits = normalized_key.split('.')
        db_schema = f"{splits[0]}.{splits[1]}"
        if is_gg_heartbeat_event(db_schema, table_name=splits[-1]):
            return db_schema
        else:
            return db_schema_vs_id.value[db_schema]
    
    return (
        F.udf(lambda norm_key: get_schema_id(norm_key), returnType=StringType())
        (normalized_key_col)
    )