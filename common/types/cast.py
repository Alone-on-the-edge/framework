import sys
sys.path.append("d:\Project\ssot")

from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DataType, NumericType, DecimalType, DateType, TimestampType, StructType, NullType

from common.types.spark_decimal import SparkDecimalType

#see https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
# Conversion for integral and floating point types have a linear widening hierarchy

_numeric_precedence = {
    ByteType(): 0,
    ShortType(): 1,
    IntegerType() : 2,
    LongType() : 3,
    FloatType() : 4,
    DoubleType() : 5
}

#noinspection PyTypeChecker

def _is_legal_numeric_precedence(from_type: DataType, to_type: DataType):
    from_precedence = _numeric_precedence.get(from_type, -1)
    to_precedence = _numeric_precedence.get(to_type, -1)
    return 0 <= from_precedence < to_precedence

def _resolve_nullability(from_is_nullable: bool, to_is_nullable: bool):
    return not from_is_nullable or to_is_nullable

def can_up_cast(from_type: DataType, to_type: DataType):
    if from_type == to_type:
        return True
    elif (isinstance(from_type, NumericType) and isinstance(to_type, DecimalType) and SparkDecimalType(from_type).is_wider_than(from_type)):
        return True
    elif (isinstance(from_type, DecimalType) and isinstance(to_type,NumericType ) and SparkDecimalType(from_type).is_tighter_than(to_type)):
        return True
    elif _is_legal_numeric_precedence(from_type, to_type):
        return True
    elif isinstance(from_type, DateType) and isinstance(to_type, TimestampType):
        return True
    #commenting this out because of known issue while converting numeric to string type columns
    # elif isinstance(from_type, AtomicType) and isinstance(to_type, TimestampType):
    #     return True
    elif isinstance(from_type, NullType):
        return True
    elif isinstance(from_type, TimestampType) and isinstance(to_type, LongType):
        return True
    elif isinstance(from_type, LongType) and isinstance(to_type, TimestampType):
        return True
    elif isinstance(from_type, StructType) and isinstance(to_type, StructType):
        from_fields = from_type.fields
        to_fields = to_type.fields
        zipped_fields = zip(from_fields, to_fields)
        return all([_resolve_nullability(f1.nullable, f2.nullable) and can_up_cast(f1.dataType, f2.dataType) for f1, f2 in zipped_fields])
    else:
        return False