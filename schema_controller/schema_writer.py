# Databricks notebook source

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.streaming import StreamingQuery #StreamingQuery lets you monitor and control a running stream.
from pyspark.sql.types import StringType

from common.context import JobContext
from common.udfs import normalize_message_key, get_schema_path
from common.utils import save_s3_object
from schema_controller.common import StreamingBatchManager, StreamType

# Schema writer is responsible for saving events to s3 from kafka
streaming_query: StreamingQuery


def start_schema_writer():
    app_spec = job_ctx.get_app_spec()
    stream =  (
        spark.readStream
        .format("kafka")
        .options(**app_spec.kafka_conf)
        .option("subscribe", app_spec.schema_topic)
        .option("startingOffsets", "earliest")
        .load()
        .withColumn("key", F.col("key").cast(StringType()))
        .withColumn("key", normalize_message_key("key"))
        .withColumn("value", F.col("value").cast(StringType()))
        .where("value not like '%generic_wrapper%'")  #Filters out generic wrapper messages.
        .selectExpr(    
            "key AS db_schema_tbl",
            "generate_schema_fingerprint(value) AS schema_fingerprint",
            f"preprocess_schema(value, {app_spec.bronze_reader_opts.get('preserve_case', 'false')}, {app_spec.silver_spark_conf['dataflow.maxDecimalScale']}) AS avro_schema"
        )
        .withColumn("schema_s3_path",get_schema_path(F.lit(job_ctx.get_schemas_dir(include_bucket=False)),
                                                     F.col("db_schema_tbl"),
                                                     F.col("schema_fingerprint")))
    )

    def save_schemas(schemas_batch_df, batch_id):
        """
        Defines a function to save the schema batch to S3. It checks if the batch is empty or already processed, and if not, 
        saves each schema to S3 and logs the completed batch.
        """
        batch_mgr = StreamingBatchManager(StreamType.SCHEMA_WRITER, job_ctx)
        if not (schemas_batch_df.isEmpty() or batch_mgr.is_completed_batch(streaming_query.id, batch_id)):
            schemas_batch_df.foreach(lambda row: save_s3_object(bucket=job_ctx.get_landing_bucket(),
                                                                key= row['schema_s3_path'],
                                                                content = row['avro_schema']))
            batch_mgr.log_completed_batch(streaming_query.id, batch_id)

    global streaming_query #Starts the streaming query, saving the data to S3 in batches and using a checkpoint to ensure exactly-once processing.
    streaming_query = (
        stream.writeStream
        .option("checkpointLocation", job_ctx.get_checkpoints_dir(StreamType.SCHEMA_WRITER.name))
        .foreachBatch(lambda schema_batch_df, batch_id: save_schemas(schema_batch_df, batch_id))
        .start(queryName=StreamType.SCHEMA_WRITER.name.lower())
    )


if __name__ == '__main__':
    spark = SparkSession.getActiveSession()
    spark.sparkContext._jvm.com.adp.ssot.SparkUserDefinedFunctions.register()
    job_ctx = JobContext.from_notebook_config()
    start_schema_writer()
    

#Four extra lines are comments in save_schemas function.     


