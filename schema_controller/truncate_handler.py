import traceback
import typing

import pyspark.sql
from delta import DeltaTable
from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType, StructField, ArrayType, Row

from ssot.common import udfs as SF
from ssot.common.context import JobContext
from ssot.common.utils import FlowErrorLogger
from ssot.schema_controller.common import StreamingBatchManager, StreamType, FlowLifeCycleManager


def truncate_schema_for_table(truncate_event: Row) -> typing.Tuple[Row, typing.Union[str, None]]:
    # TODO : add cdc_sequencer in silver layer tables so that it's avaialble for delete
    try:
        silver_layer_tbl = job_ctx.get_silver_table_name(silver_view_name = truncate_event['target_table'])
        DeltaTable.forName(spark, silver_layer_tbl) \
            .delete(f"__cdc_sequencer < '{truncate_event['cdc_sequencer']}' AND "
                    f"__schema_id = '{truncate_event['schema_id']}'")
        return truncate_event, None
    except:
        return truncate_event, traceback.format_exc()
    

def handle_truncates(truncates_batch_df: DataFrame, batch_id: str):
    batch_mgr = StreamingBatchManager(StreamType.TRUNCATE_HANDLER, job_ctx)
    if not (truncates_batch_df.isEmpty() or batch_mgr.is_completed_batch(batch_id)):
        flows_df = spark.createDataFrame(
            [(fs.flow_id, fs.target_details.delta_table) for fs in app_spec.flow_specs],
            ['flow_id', 'target_table']
        )

        grouped_truncates_df: pyspark.sql.DataFrame = (
            truncates_batch_df
                .withColumn("truncate_event_file", F.input_file_name())
                .alias("t")
                .join(F.broadcast(flows_df).alias("f"), 't.target_table = f.target_table')
                .select('f.flow_id', 't.schema_id', 't.target_table', 't.truncate_event_file',
                        SF.generate_cdc_sequencer('t.op_ts', 't.pos').alias('cdc_sequencer'))
                .groupBy(['flow_id', 'schema_id', 'target_table'])
                .agg(
                    F.max('cdc_sequencer').alias('cdc_sequencer'),
                    F.collect_set('truncate_event_file').alias('event_files')
                )
        )

        truncate_events : typing.List[Row] = grouped_truncates_df.collect()
        flows_for_truncate = set(map(lambda te: te.flow_id, truncate_events))
        flow_mgr = FlowLifeCycleManager(flows_for_truncate, job_ctx)
        flow_mgr.stop_pipelines()

        (
            DeltaTable.forName(truncate_status_tbl).alias('m')
                .merge(grouped_truncates_df.alias('u'), 'm.flow_id = u.flow_id AND m.schema_id = u.schema_id')
                .whenNotMatchedInsert(
                    {
                        'm.app' : F.lit(job_ctx.app),
                        'm.flow_id' : 'u.flow_id',
                        'm.schema_id' : 'u.schema_id',
                        'm.event_files' : 'u.event_files',
                        'm.schema_truncate_done' : F.lit(False),
                        'm.cdc_sequencer' : 'u.cdc_sequencer',
                        'm.created_at' : F.current_timestamp(),
                        'm.updated_at' : F.current_timestamp(),
                        'm.created_by' : F.lit(StreamType.TRUNCATE_HANDLER.name),
                        'm.updated_at' : F.lit(StreamType.TRUNCATE_HANDLER.name)
                    }
                ).whenMatchedUpdate(
                    set = {'m.schema_truncate_done' : F.lit(False),
                           'm.cdc_sequencer' : 'u.cdc_sequencer',
                           'm.updated_at' : F.current_timestamp(),
                           'm.updated_by' : F.lit(StreamType.TRUNCATE_HANDLER.name)}
                ).execute()
        )

        truncate_results = list(map(lambda te: truncate_schema_for_table(te,), truncate_events))
        success_events_df = spark.createDataFrame([te for te in truncate_results if te[1] is None])
        failed_events = [te for te in truncate_results if te[1] is not None]

        (
            DeltaTable.forName(truncate_status_tbl).alias('m')
                .merge(success_events_df.alias('u'), 'm.flow_id = u.flow_id AND m.schema_id = u.schema_id')
                .whenMatchedUpdate(
                    set = {'m.event_files' : F.array_distinct(F.concat('m.event_files','u.event_files')),
                           'm.schema_truncate_done' : F.lit(True),
                           'm.cdc_sequencer' : 'u.cdc_sequencer',
                           'm.updated_at' : F.current_timestamp(),
                           'm.updated_by' : F.lit(StreamType.TRUNCATE_HANDLER.name)}
                ).execute()
        )

        if len(failed_events) > 0:
            err_logger = FlowErrorLogger(StreamType.TRUNCATE_HANDLER.name, job_ctx)
            for te, err in  failed_events:
                err_logger.log(flow_id=te['flow_id'],
                               error_desc='Failure occured while truncating schema',
                               error_trace=err,
                               error_metadata=te.asDict(recursive=True))
        
        flow_mgr.start_pipelines()
        batch_mgr.log_completed_batch(batch_id)


def start_truncate_handler():
    truncate_event_schema = StructType([
        StructField("target_table", StringType()),
        StructField("op_type", StringType()),
        StructField("op_ts", StringType()),
        StructField("current_ts", StringType()),
        StructField("pos", StringType()),
        StructField("xid", StringType()),
        StructField("csn", StringType()),
        StructField("txind", StringType()),
        StructField("primary_keys", ArrayType(StringType())),
        StructField("schema_id", StringType()),
    ])

    stream =  (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("pathGlobFilter", "*.json")
            .schema(truncate_event_schema)
            .load(job_ctx.get_truncate_dir()))
    (
        stream.writeStream
            .option("checkpointLocation", job_ctx.get_checkpoints_dir(StreamType.TRUNCATE_HANDLER.name))
            .foreachBatch(lambda batch_df, batch_id: handle_truncates(batch_df, batch_id))
            .start(queryName=StreamType.TRUNCATE_HANDLER.name)
    )


if __name__ == "__main__":
    spark = SparkSession.getActiveSession()
    job_ctx = JobContext.from_notebook_config()
    app_spec = job_ctx.get_app_spec()
    truncate_status_tbl = job_ctx.get_truncate_status_tbl()
    start_truncate_handler()