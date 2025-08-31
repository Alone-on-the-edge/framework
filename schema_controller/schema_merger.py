# Databricks notebook source
import traceback
import typing
from dataclasses import dataclass
from multiprocessing.pool import ThreadPool

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StringType, LongType

from common.context import JobContext
from common.control_spec import FlowSpec
from common.types.merge_type import merge_schema_from_ddl, SchemaMergerError, handle_numeric_as_string
from common.utils import FlowErrorLogger, is_active_flow, cast_with_cdc_schema, s3_path_exists, split_s3_path, \
    get_additional_fields_schema, get_dlt_fields_schema
from common.worker import wait_until
from schema_controller.common import StreamType, StreamingBatchManager, FlowLifeCycleManager
from common.schema_change_notifier import notify_ddl_event

# Schema Merger: responsible for merging schemas from S3 using autoloader, updating control tables,
# and overwriting existing data with new schema.

streaming_query: StreamingQuery

def overwrite_schemas(sch_overwrite_flow_ids):
    """
    Overwrites the table schemas for specified flow IDs.

    Parameters:
    sch_overwrite_flow_ids (list): List of flow IDs for which schemas need to be overwritten.
    """

    def overwrite_table_schema(flow_spec: FlowSpec) -> typing.Tuple[str, typing.Union[str, None]]:
        """
        Overwrites the schema for a single table based on the provided flow specification.

        Parameters:
        flow_spec (FlowSpec): The flow specification containing details of the table.

        Returns:
        tuple: A tuple containing the flow ID and an optional error message.
        """
        spark.sql("set spark.sql.shuffle.partitions=800")
        spark.sql("set spark.databricks.delta.optimizeWrite.enabled=true")
        spark.sql("set spark.databricks.delta.autoCompact.enabled=true")
        
        # Split the S3 path to get the bucket and key
        tbl_path = flow_spec.target_details.delta_table_path
        tbl_bucket, tbl_key = split_s3_path(tbl_path)

        # Check if the S3 path exists
        if s3_path_exists(tbl_bucket, tbl_key):
            tbl_df = spark.read.format("delta").load(tbl_path)
            tbl_part_cols = flow_spec.target_details.partition_cols
            cdc_schema = f"{flow_spec.cdc_schema}, __sk STRING, {get_additional_fields_schema()}, {get_dlt_fields_schema()}"

            tbl_df = cast_with_cdc_schema(tbl_df, cdc_schema_ddl=cdc_schema)
            try:
                sc.setLocalProperty('spark.scheduler.pool', f'pool-{flow_spec.flow_id}')
                (
                    tbl_df.write
                    .partitionBy(*tbl_part_cols)
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .save(tbl_path)
                )
                return flow_spec.flow_id, None
            except:
                return flow_spec.flow_id, traceback.format_exc()
        else:
            return flow_spec.flow_id, None

    # Get flow specifications for the flows that need schema overwrite
    flows_for_overwrite = [spec for spec in job_ctx.get_app_spec().flow_specs
                           if spec.flow_id in sch_overwrite_flow_ids]

    # Use a thread pool to process schema overwrites concurrently
    thread_pool = ThreadPool(20)
    schema_overwrite_results = thread_pool.map(overwrite_table_schema, flows_for_overwrite)
    thread_pool.close()

    successful_flow_ids = []
    failed_flow_id_vs_err = []

    # Process the results of schema overwrites
    for flow_id, err in schema_overwrite_results:
        if err is None:
            successful_flow_ids.append(flow_id)
        else:
            failed_flow_id_vs_err.append((flow_id, err))

    # Update the control table with successful schema overwrites
    if len(successful_flow_ids) > 0:
        successful_flow_ids = [(flow_id,) for flow_id in successful_flow_ids]
        successful_flow_df = spark.createDataFrame(successful_flow_ids, ['flow_id'])
        (
            DeltaTable.forName(spark, flow_spec_tbl).alias("m")
            .merge(successful_flow_df.alias('u'), f"m.flow_id = u.flow_id AND m.app = '{job_ctx.app}'")
            .whenMatchedUpdate(set={
                'm.target_details.schema_refresh_done': F.lit(True),
                'm.updated_by': F.lit(StreamType.SCHEMA_MERGER.name),
                'm.updated_at': F.current_timestamp()
            }).execute()
        )

    # Log errors for failed schema overwrites
    if len(failed_flow_id_vs_err) > 0:
        err_logger = FlowErrorLogger(StreamType.SCHEMA_MERGER.name, job_ctx)
        for flow_id, err in failed_flow_id_vs_err:
            err_logger.log(flow_id=flow_id,
                           error_desc="Failure occurred while overwriting schemas",
                           error_trace=err)

@F.udf(returnType=LongType())
def get_schema_fingerprint(schema_path):
    """
    Extracts the schema fingerprint from the schema path.

    Parameters:
    schema_path (str): The S3 path to the schema file.

    Returns:
    int: The schema fingerprint extracted from the file name.
    """
    schema_file_name = schema_path.split('/')[-1]
    return int(schema_file_name.replace('.avsc', '', 1).split('.')[-1])

@F.udf(returnType=StringType())
def get_flow_id(schema_path):
    """
    Extracts the flow ID from the schema path.

    Parameters:
    schema_path (str): The S3 path to the schema file.

    Returns:
    str: The flow ID derived from the schema path.
    """
    tbl_name = schema_path.split("/")[-2]
    return f"{job_ctx.app}_{tbl_name}"

@dataclass
class CdcSchema:
    """
    Data class representing a CDC schema.
    """
    schema_ddl: str
    schema_fingerprints: set[int]
    needs_schema_overwrite: bool

    def is_defined(self):
        """
        Checks if the CDC schema is defined.

        Returns:
        bool: True if the schema is defined, False otherwise.
        """
        return self != CdcSchema.get_default()

    @staticmethod
    def get_default():
        """
        Returns the default CDC schema.

        Returns:
        CdcSchema: The default CDC schema.
        """
        return CdcSchema(schema_ddl=None,
                         schema_fingerprints=None,
                         needs_schema_overwrite=None)

def get_active_flows_and_schemas(schemas_batch: DataFrame, flow_specs: typing.List[FlowSpec]) -> \
        typing.Tuple[typing.List[FlowSpec], typing.List[typing.Tuple[str, str, int, str]]]:
    """
    Identifies active flows and their corresponding schemas from a batch of schemas.

    Parameters:
    schemas_batch (DataFrame): DataFrame containing a batch of schemas.
    flow_specs (list): List of flow specifications.

    Returns:
    tuple: A tuple containing a list of active flow specifications and a list of active flow schemas.
    """
    active_flows = [flow for flow in flow_specs if is_active_flow(flow)]
    active_flow_ids = [(flow.flow_id, True) for flow in flow_specs if is_active_flow(flow)]
    inactive_flow_ids = [(spec.flow_id, False) for spec in flow_specs if not is_active_flow(spec)]
    flows_df = spark.createDataFrame([*active_flow_ids, *inactive_flow_ids], ['flow_id', 'is_active'])
    schema_flow_joined = (
        schemas_batch
        .withColumn("flow_id", get_flow_id("schema_path"))
        .withColumn("schema_fingerprint", get_schema_fingerprint("schema_path"))
        .alias('s')
        .join(F.broadcast(flows_df).alias('f'), 'flow_id')
        .select("s.*", "f.is_active")
        .collect()
    )
    inactive_flow_schemas = [(row['flow_id'], row["schema_path"])
                             for row in schema_flow_joined if row["is_active"] is False]

    for flow_id, schema_path in inactive_flow_schemas:
        flow_err_logger.log(
            flow_id=flow_id,
            error_desc=f"[WARN] Skipping schema evolution for schema at {schema_path}",
            error_trace="Either schema refresh wasn't completed previously or the flow is inactive"
        )

    return (active_flows, [(row['flow_id'], row['avro_schema'], row['schema_fingerprint'], row['schema_path'])
                           for row in schema_flow_joined if row['is_active'] is True])

def merge_schemas(schemas_batch_df: DataFrame, batch_id: str):
    """
    Merges schemas from a batch of schemas.

    Parameters:
    schemas_batch_df (DataFrame): DataFrame containing a batch of schemas.
    batch_id (str): The batch ID.
    """
    batch_mgr = StreamingBatchManager(StreamType.SCHEMA_MERGER, job_ctx)

    if not (schemas_batch_df.isEmpty() or batch_mgr.is_completed_batch(streaming_query.id, batch_id)):
        app_spec = job_ctx.get_app_spec()

        # First identify what schemas for which flows from the batch of schemas need to be processed
        active_flows, schemas_batch = get_active_flows_and_schemas(schemas_batch_df, app_spec.flow_specs)

        # Second, build a mapping of flow and its existing schema derived from the DLT flow spec control table
        flow_vs_existing_schema = dict(
            (spec.flow_id, CdcSchema(schema_ddl=spec.cdc_schema,
                                     schema_fingerprints=set(spec.cdc_schema_fingerprints),
                                     needs_schema_overwrite=False))
            for spec in active_flows
        )

        # Iterate over the schemas batch and build a map of flow vs. merged schema
        flow_vs_merged_schema: typing.Dict[str, CdcSchema] = {}
        for schema in schemas_batch:
            (flow_id, avro_schema, schema_fingerprint, schema_path) = (schema[0], schema[1], schema[2], schema[3])
            try:
                if app_spec.src_db_type in ['mysql', 'sqlserver']:
                    avro_schema = handle_numeric_as_string(avro_schema, app_spec)
                schema_ddl = jvm.com.adp.ssot.schema.AvroSchemaUtils.parseAsDDL(avro_schema)
                existing_cdc_schema = flow_vs_merged_schema.get(flow_id, flow_vs_existing_schema[flow_id])
                if existing_cdc_schema is not None and existing_cdc_schema.is_defined():
                    merged_schema = merge_schema_from_ddl(schema_ddl, existing_cdc_schema.schema_ddl,
                                                          app_spec.src_db_type)
                    merged_schema_fingerprints = {schema_fingerprint, *existing_cdc_schema.schema_fingerprints}
                    needs_schema_overwrite = merged_schema.requires_schema_overwrite() or \
                                             existing_cdc_schema.needs_schema_overwrite
                    merged_cdc_schema = CdcSchema(
                        schema_ddl=merged_schema.schema_to_ddl(),
                        schema_fingerprints=merged_schema_fingerprints,
                        needs_schema_overwrite=needs_schema_overwrite
                    )
                    flow_vs_merged_schema[flow_id] = merged_cdc_schema

                    if merged_schema != existing_cdc_schema:
                        notify_ddl_event(merged_schema, flow_id, job_ctx)

            except SchemaMergerError:
                flow_err_logger.log(flow_id=flow_id,
                                    error_desc='Failure occurred while merging schemas',
                                    error_trace=traceback.format_exc(),
                                    error_metadata={'erroneous_schema_path': schema_path})
                flow_vs_merged_schema[flow_id] = CdcSchema.get_default()

        if len(flow_vs_merged_schema) > 0:
            flow_vs_merged_schema_list = list(
                map(lambda i: (
                    i[0],
                    i[1].schema_ddl,
                    list(i[1].schema_fingerprints) if i[1].schema_fingerprints else None,
                    i[1].needs_schema_overwrite
                ), flow_vs_merged_schema.items())
            )

            schema_chg_flows_df = spark.createDataFrame(flow_vs_merged_schema_list,
                                                        schema="""
                                                        flow_id STRING,
                                                        merged_schema STRING,
                                                        merged_schema_fingerprints ARRAY<LONG>,
                                                        needs_schema_overwrite BOOLEAN""")

            schema_chg_flows_df = spark.table(flow_spec_tbl).alias('m') \
                .join(schema_chg_flows_df.alias('u'), 'flow_id') \
                .selectExpr('u.*',
                            """CASE
                                WHEN u.merged_schema IS NULL AND u.merged_schema_fingerprints IS NULL AND u.needs_schema_overwrite IS NULL
                                    THEN 'FAILURE'
                                WHEN m.cdc_schema = u.merged_schema AND ARRAY_SORT(m.cdc_schema_fingerprints) = ARRAY_SORT(u.merged_schema_fingerprints)
                                    THEN 'SAME'
                                WHEN m.cdc_schema = u.merged_schema
                                    THEN 'FLOW_RESTART_ONLY'
                                WHEN m.cdc_schema != u.merged_schema AND u.needs_schema_overwrite = false
                                    THEN 'FLOW_RESTART_ONLY'
                                ELSE
                                    'FLOW_RESTART_AND_OVERWRITE'
                                END AS schema_chg_type
                            """) \
                .persist()

            def get_flows_based_on_type(schema_chg_type: str):
                """
                Retrieves flow IDs based on the schema change type.

                Parameters:
                schema_chg_type (str): The type of schema change.

                Returns:
                set: A set of flow IDs matching the schema change type.
                """
                flows = schema_chg_flows_df \
                    .where(f"schema_chg_type = '{schema_chg_type}'") \
                    .select('flow_id') \
                    .collect()
                return set(map(lambda flo: flo['flow_id'], flows))

            sch_overwrite_flows = get_flows_based_on_type('FLOW_RESTART_AND_OVERWRITE')
            sch_restart_only_flows = get_flows_based_on_type('FLOW_RESTART_ONLY')
            sch_merge_failed_flows = get_flows_based_on_type('FAILURE')
            flows_to_restart = {*sch_overwrite_flows, *sch_restart_only_flows, *sch_merge_failed_flows}
            flow_mgr = FlowLifeCycleManager(flows_to_restart, job_ctx)
            flow_mgr.stop_pipelines()

            # Update status of flows that were successful during a merge
            (
                DeltaTable.forName(spark, flow_spec_tbl).alias('m')
                .merge(schema_chg_flows_df.where("schema_chg_type != 'FAILURE' ").alias('u'),
                       f"m.flow_id = u.flow_id AND m.app = '{job_ctx.app}'")
                .whenMatchedUpdate(set={
                    'm.cdc_schema': 'u.merged_schema',
                    'm.cdc_schema_fingerprints': F.array_distinct(
                        F.concat('m.cdc_schema_fingerprints', 'u.merged_schema_fingerprints')
                    ),
                    'm.target_details.schema_refresh_done': F.col('u.schema_chg_type').isin(
                        ['SAME', 'FLOW_RESTART_ONLY']
                    ),
                    'm.updated_by': F.lit(StreamType.SCHEMA_MERGER.name),
                    'm.updated_at': F.current_timestamp()
                }).execute()
            )
            schema_chg_flows_df.unpersist()
            overwrite_schemas(sch_overwrite_flows)

            # Update status of flows that failed during a schema merge, so that when the pipeline starts, this flow is not started.
            if len(sch_merge_failed_flows) > 0:
                sch_merge_failed_flows_df = spark.createDataFrame(
                    list(map(lambda f: (f,), sch_merge_failed_flows)), ['flow_id']
                )
                (
                    DeltaTable.forName(spark, flow_spec_tbl).alias('m')
                    .merge(sch_merge_failed_flows_df.alias('u'),
                           f"m.flow_id = u.flow_id AND m.app = '{job_ctx.app}'")
                    .whenMatchedUpdate(set={
                        'm.target_details.schema_refresh_done': F.lit(False),
                        'm.updated_by': F.lit(StreamType.SCHEMA_MERGER.name),
                        'm.updated_at': F.current_timestamp()
                    }).execute()
                )
                flow_mgr.start_pipelines()
            batch_mgr.log_completed_batch(streaming_query.id, batch_id)

def start_schema_merger():
    """
    Starts the schema merger streaming job.
    """
    wait_until(predicate=s3_path_exists,
               args=[job_ctx.get_landing_bucket(), job_ctx.get_schemas_dir(include_bucket=False)],
               timeout_seconds=60 * 60,
               poll_interval_seconds=60)

    stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.backfillInterval", "1 hour")
        .load(job_ctx.get_schemas_dir())
        .withColumn("schema_path", F.input_file_name())
        .withColumnRenamed("value", "avro_schema")
    )

    global streaming_query
    streaming_query = (
        stream.writeStream
        .option("checkpointLocation", job_ctx.get_checkpoints_dir(StreamType.SCHEMA_MERGER.name))
        .foreachBatch(lambda batch_df, batch_id: merge_schemas(batch_df, batch_id))
        .start(queryName=StreamType.SCHEMA_MERGER.name.lower())
    )

if __name__ == '__main__':
    spark = SparkSession.getActiveSession()
    sc = spark.sparkContext
    jvm = sc._jvm
    jvm.com.adp.ssot.SparkUserDefinedFunctions.register()
    job_ctx = JobContext.from_notebook_config()
    app_spec = job_ctx.get_app_spec()
    flow_spec_tbl = job_ctx.get_flow_spec_table()
    flow_err_logger = FlowErrorLogger(StreamType.SCHEMA_MERGER.name, job_ctx)
    start_schema_merger()
