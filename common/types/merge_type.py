import sys
sys.path.append("d:\Project\ssot")

import json
from typing import List, Union, Optional

from pyspark.sql.types import DataType, StructType, StructField, \
    NullType, BooleanType, BinaryType, TimestampType, StringType, IntegerType, LongType, DecimalType
from common.control_spec import AppSpec

from common.types.cast import can_up_cast
from common.types.spark_struct import struct_from_ddl, struct_to_ddl


class SchemaMergerError(Exception):
    def __init__(self,data_schema: DataType, delta_schema: DataType):
        super().__init__(
            f"Cant merge incompatible types '{data_schema.simpleString()}'"
            f"& '{delta_schema.simpleString()}'"
        )


class MergedDataType:
    def __init__(self, field_name: str,
                 merged_type : DataType,
                 table_before_type : DataType,
                 table_after_type : DataType):
        self.field_name = field_name
        self.merged_type = merged_type
        self.table_before_type = table_before_type
        self.table_after_type = table_after_type

    def __repr__(self):
        return self.__dict__.__repr__()


class MergedSchema:
    def __init__(self, merged_schema: StructType,
                 new_fields: List[str],
                 merged_data_types: List[MergedDataType],
                 missing_fields : List[str]):
        self.merged_schema = merged_schema
        self.new_fields = new_fields
        self.merged_data_types = merged_data_types
        self.missing_fields = missing_fields
    
    def __repr__(self):
        return self.__dict__.repr__()
    
    def requires_schema_overwrite(self) -> bool:
        if len(self.new_fields) > 0 and len(self.merged_data_types) == 0:
            return False
        else:
            return True
    
    def schema_to_ddl(self):
        return struct_to_ddl(self.merged_schema)
    

def merge_schema(data_schema: Union[StructType, DataType],
                 delta_schema: Union[StructType, DataType]) -> MergedSchema:
    missing_fields = []
    new_fields = []
    merged_data_types = []

    def merge_schema_internal(data_schema: Union[StructType, DataType],
                              delta_schema: Union[StructType, DataType],
                               name: Optional[str] = None ) -> Union[StructType, DataType]:
        
        def get_fld_name(n: str) -> str:
            if name is not None:
                return f'{name}.{n}'
            else:
                return n
            
        if isinstance(data_schema, NullType):
            return delta_schema
        elif isinstance(delta_schema, NullType):
            return data_schema
        elif data_schema == delta_schema:
            return data_schema
        elif isinstance(data_schema, StructType) and isinstance(delta_schema, StructType):
            data_fields_dict = dict((f.name, f.dataType ) for f in data_schema.fields)
            merged_fields = []

            for fld in delta_schema.fields:
                data_type = data_fields_dict.get(fld.name, NullType())
                if fld.name not in data_fields_dict:
                    missing_fields.append(get_fld_name(fld.name))

                merged_fields.append(
                    StructField(
                        fld.name, merge_schema_internal(data_schema=data_type,
                                                        delta_schema=fld.dataType,
                                                        name = get_fld_name(fld.name))
                    )
                )

                merged_fields_names = set([f.name for f in merged_fields])
                for fld in data_fields_dict:
                    if fld not in merged_fields_names:
                        merged_fields.append(StructField(fld, data_fields_dict[fld]))
                        new_fields.append(get_fld_name(fld))
                
                return StructType(merged_fields)
        
        elif can_up_cast(from_type=delta_schema, to_type=data_schema):
            merged_data_types.append(
                MergedDataType(
                    field_name=name,
                    merged_type = data_schema,
                    table_before_type= delta_schema,
                    table_after_type=data_schema
                )
            )
            return data_schema
        
        elif can_up_cast(from_type=data_schema, to_type=delta_schema):
            merged_data_types.append(
                MergedDataType(
                    field_name=name,
                    merged_type = delta_schema,
                    table_before_type= delta_schema,
                    table_after_type=delta_schema
                )
            )
            return delta_schema
        
        else:
            raise SchemaMergerError(data_schema, delta_schema)
        
        merged_schema = merge_schema_internal(data_schema, delta_schema)
        return MergedSchema(
            merged_schema=merged_schema,
            new_fields=new_fields,
            merged_data_types=merged_data_types,
            missing_fields=missing_fields
        )


def postgres_merge_datatype_rule(data_schema, delta_schema):
    """Solved: cant merge incompatible types 1.binary & boolean 2. string & timestamp"""
    final_schema = []
    for data_field in data_schema.fields:
        if data_field.name not in delta_schema.fieldNames():
            final_schema.append(data_field)
        else:
            for delta_field in delta_schema.fields:
                if delta_field.name == data_field.name:
                    if isinstance(delta_field.dataType, BooleanType) and isinstance(data_field.dataType, BinaryType):
                        final_schema.append(StructField(delta_field.name, BooleanType()))
                    elif isinstance(delta_field.dataType, TimestampType) and isinstance(data_field.dataType,
                                                                                        StringType):
                        final_schema.append(StructField(delta_field.name, TimestampType()))
                    else:
                        final_schema.append(data_field)
    
    return StructType(final_schema)


def mysql_merge_datatype_rule(data_schema, delta_schema):
    """Solved: cant merge incompatible types 1.string & date 2. string & binary 3. merge decimal ex"""

    final_schema = []
    for data_field in data_schema.fields:
        if data_field.name not in delta_schema.fieldNames():
            final_schema.append(data_field)
        else:
            for delta_field in delta_schema.fields:
                if delta_field.name == data_field.name:

                    if isinstance(data_field.dataType, StringType) and isinstance(delta_field.dataType, TimestampType):
                        final_schema.append(StructField(delta_field.name, TimestampType()))
                    elif isinstance(data_field.dataType, StringType) and isinstance(delta_field.dataType, BinaryType):
                        final_schema.append(StructField(delta_field.name, BinaryType()))
                    else:
                        final_schema.append(data_field)
    
    return StructType(final_schema)


def sqlserver_merge_datatype_rule(data_schema, delta_schema):
    """Solved: cant merge incompatible types 1.string & date 2. binary & boolean(sqlserver)"""

    final_schema = []
    for data_field in data_schema.fields:
        if data_field.name not in delta_schema.fieldNames():
            final_schema.append(data_field)
        else:
            for delta_field in delta_schema.fields:
                if delta_field.name == data_field.name:

                    if isinstance(data_field.dataType, StringType) and isinstance(delta_field.dataType, TimestampType):
                        final_schema.append(StructField(delta_field.name, TimestampType()))
                    elif isinstance(data_field.dataType, BinaryType) and isinstance(delta_field.dataType, BooleanType):
                        final_schema.append(StructField(delta_field.name, BooleanType()))
                    else:
                        final_schema.append(data_field)
    
    return StructType(final_schema)


def handle_numeric_as_string(schema: str, app_spec: AppSpec):
    json_schema = json.loads(schema)

    def rec_process_schema(dict_schema):
        for key, value in dict_schema.items():
            if isinstance(value, dict):
                rec_process_schema(value)
            elif isinstance(value, list):
                for val in value:
                    if isinstance(val, str):
                        pass
                    elif isinstance(val, list):
                        pass
                    else:
                        if 'origtype' in val and val['origtype'] == 'number':
                            scale = int(app_spec.silver_spark_conf["dataflow.maxDecimalScale"])
                            logical_type = {"logicalType" : "decimal" , "precision": 38, "scale": scale, "type": "bytes"}
                            list_type = ["null", logical_type]
                            val["type"] = list_type
                        rec_process_schema(val)
    
    rec_process_schema(json_schema)
    return json.dumps(json_schema)


def merge_schema_from_ddl(data_schema: str, delta_schema: str, src_db_type : str) -> MergedSchema:
    data_schema = struct_from_ddl(data_schema)
    delta_schema = struct_from_ddl(delta_schema)

    if src_db_type == 'postgres':
        data_schema = postgres_merge_datatype_rule(data_schema, delta_schema)
    
    if src_db_type == 'mysql':
        data_schema = mysql_merge_datatype_rule(data_schema, delta_schema)

    if src_db_type == 'sqlserver':
        data_schema = sqlserver_merge_datatype_rule(data_schema, delta_schema)

    return merge_schema(data_schema, delta_schema)