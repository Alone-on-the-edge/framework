import json

from pyspark import SparkContext
from pyspark.sql.types import StructType


def struct_from_ddl(ddl:str) -> StructType:
    """
    # Example DDL string
    ddl_string = "name STRING, age INT, salary DOUBLE"

    # Convert DDL to StructType
    schema = struct_from_ddl(ddl_string)

    # Create DataFrame with the schema
    data = [("John", 30, 50000.0), ("Jane", 25, 60000.0)]
    df = spark.createDataFrame(data, schema)

    Use Case:

    The struct_from_ddl method is particularly useful in scenarios where schema definitions are provided or stored in DDL format. This is common in many database management systems 
    and data engineering tasks. Here are some practical uses:

    Schema Evolution and Data Integration:
        When integrating data from different sources, schemas are often described in DDL format. This method allows you to programmatically convert these schemas into Spark's StructType for further processing.

    Dynamic Schema Handling:
        In environments where schema definitions might change frequently, such as when dealing with data streams or various external data sources, DDL strings can be dynamically converted into StructType objects to adapt to schema changes on-the-fly.

    Interoperability with SQL-Based Tools:
        Tools that define or export schema information in SQL DDL format can be integrated with Spark by converting those DDL strings into Spark-compatible schemas.
    """
    _sc = SparkContext.getOrCreate()
    return StructType.fromJson(
        json.loads(
            _sc._jvm.org.apache.spark.sql.types.StructType.fromDDL(ddl).json()
        )
    )


def struct_to_ddl(struct: StructType) -> str:
    """
    # Define a schema using StructType
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", IntegerType(), True)
    ])

    # Convert the schema to DDL
    ddl_string = struct_to_ddl(schema)

    # Print the DDL string
    print(ddl_string)
    `name` STRING, `age` INT, `salary` INT

    Use Cases of below methods.

    Schema Serialization:
        When you need to save the schema of a DataFrame as a string (e.g., for storing it in a metadata repository or a configuration file), converting the StructType to a DDL 
        string is a convenient way to serialize the schema.

    Schema Comparison:
        If you need to compare schemas, converting them to DDL strings can make it easier to compare them as plain text.

    Interoperability:
        When working with systems that accept DDL strings for schema definitions (e.g., certain database systems or SQL engines), this function can help convert Spark schemas to 
        a compatible format.

    Schema Validation and Debugging:
        Converting schemas to DDL strings can help in validating and debugging schema definitions by providing a clear, human-readable format.

    """
    _sc = SparkContext.getOrCreate()
    return _sc._jvm.org.apache.spark.sql.types.StructType.fromJson(struct.json()).toDDL()