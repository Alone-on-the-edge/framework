from pyspark.sql.types import StringType, ArrayType, StructField, StructType

payload_schema = StructType([
    StructField("table", StringType()),
    StructField("op_type", StringType()),
    StructField("op_ts", StringType()),
    StructField("current_ts", StringType()),
    StructField("pos", StringType()),
    StructField("xid", StringType()),
    StructField("csn", StringType()),
    StructField("txind", StringType()),
    StructField("primary_keys", ArrayType(StringType())),
    StructField("before", StringType()),
    StructField("after", StringType())
])