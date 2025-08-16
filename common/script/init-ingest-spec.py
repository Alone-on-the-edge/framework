import itertools
import traceback
from multiprocessing.pool import ThreadPool

from delta import DeltaTable
from pyspark import StorageLevel, Row
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, lit, isnull, current_timestamp, struct
from pyspark.sql.types import StructType

from common.context import JobContext
from common.control_spec import FlowSpec, InitIngestSpec
from common.keygen import generate_surrogate_key
from common.types.spark_struct import struct_from_ddl, struct_to_ddl
from common.utils import cast_with_cdc_schema, get_dlt_fields_schema, sanitize_column_name

@udf
def get_schema_id(schema_name):
    return schema_name_vs_id.value[schema_name.lower()].lower()

def read_init_load(format, init_path) -> DataFrame:
    return spark.read.format(format).load(init_path)


#Running the load job in parallel
def run_init_load_job(init_load_config: InitIngestSpec):
    table_name = init_load_config.table_name
    schema_id = init_load_config.schema_id
    flow_spec: FlowSpec = None
    for flow_spec_cls in flow_spec_list:
        if flow_spec_cls.src_table == init_load_config.table_name:
            flow_spec = flow_spec_cls
            break
    if flow_spec is None:
        raise ValueError(f"table {init_load_config.table_name} not found in {job_ctx.get_flow_spec_table()}")
    
    try:
        target_tbl_path = flow_spec.target_details.delta_table_path
        partition_cols = flow_spec.target_details.partition_cols
        primary_keys = flow_spec.cdc_keys
        init_load_path = init_load_config.init_load_path

        @udf
        def get_surrogate_key(data: Row):
            data_dict = data.asDict()
            return generate_surrogate_key(data_dict, primary_keys)
        
        if init_load_config.load_type == 'dms':
            dms_df = read_init_load('parquet', init_load_path)
            preserve_case_conf = app_spec.bronze_reader_opts.get('preserve_case',
                                                                 'false') == 'true'
            for col_name in dms_df.columns:
                dms_df = dms_df.withColumnRenamed(col_name, sanitize_column_name(col_name, preserve_case_conf))

            dms_cols = set(col.lower() for col in dms_df.columns)
            cdc_schema = StructType(
                [fld for fld in struct_from_ddl(flow_spec.cdc_schema).fields
                 if fld.name.lower() in dms_cols]
            )
            dms_df = dms_df.select(cdc_schema.fieldNames())
            init_load_df = (
                cast_with_cdc_schema(dms_df, f"{struct_to_ddl(cdc_schema)},{get_dlt_fields_schema()}")
                                     .withColumn('__schema_id', lit(schema_id))
            )

        elif init_load_config.load_type == 'delta':
            init_load_df = read_init_load('delta', init_load_path)
            init_load_df = init_load_df.select(struct_from_ddl(flow_spec.cdc_schema).fieldNames())
            init_load_df = init_load_df.withColumn('__schema_id', get_schema_id('schema_id'))
            init_load_schema = f"{flow_spec.cdc_schema}, __schema_id STRING , {get_dlt_fields_schema()}"
            init_load_df = cast_with_cdc_schema(init_load_df, init_load_schema)
            init_load_df = init_load_df.filter(f"lower(__schema_id) == '{schema_id}' ")
        
        else:
            raise ValueError(f"Invalid load type {init_load_config.load_type}")
        
        init_load_df = init_load_df\
                        .withColumn('__sk', get_surrogate_key(struct('*')))
        
        init_load_df.persist(StorageLevel.MEMORY_AND_DISK)
        unique_sk_df = init_load_df.select("__sk").distinct()
        init_load_count = init_load_df.count()
        unique_sk_count = unique_sk_df.count()
        duplicate_count = init_load_count - unique_sk_count

        if duplicate_count > 0:
            init_load_df = init_load_df.dropDuplicates(["__sk"])

        init_load_df.write.mode("append").format("delta") \
            .partitionBy(partition_cols) \
            .save(target_tbl_path)
        
        init_load_df.unpersist()

        return table_name, schema_id, None, duplicate_count
    
    except:
        error = traceback.format_exc()
        if 'Path does not exist' in error and len(flow_spec.cdc_lob_columns) > 0:
            schema = f"{flow_spec.cdc_schema}, __schema_id STRING , __sk STRING, {get_dlt_fields_schema()}"

            (spark.createDataFrame([], schema)
             .write.mode("append").format("delta")
             .partitionBy(flow_spec.target_details.partition_cols)
             .save(flow_spec.target_details.delta_table_path))
        
        return table_name, schema_id, error, 0
    
def run_init_load_jobs(init_load_configs):
        results = []
        for init_load_config in init_load_configs:
            result = run_init_load_job(init_load_config)
            results.append(result)
        
        return results
    
if __name__ == "__main__":
    spark = SparkSession.getActiveSession()
    job_ctx = JobContext.from_notebook_config()
    app = job_ctx.app
    env = job_ctx.env
    bucket_name = job_ctx.get_landing_bucket()
    app_spec = job_ctx.get_app_spec()
    init_load_config_table = job_ctx.get_init_load_table()
    flow_spec_list = app_spec.flow_specs
    init_ingest_specs = app_spec.init_ingest_specs

    schema_meta_query = f"""select id as schema_id,lower(src_db) as src_db,lower(src_schema) as src_schema
        from {job_ctx.get_db_schema_spec_tbl()}
        where app = '{app}' """

    schema_name_vs_id = spark.sql(schema_meta_query).collect()
    schema_name_vs_id = spark.sparkContext.broadcast(dict((fld.src_schema, fld.schema_id) for fld in schema_name_vs_id))

    if len(init_ingest_specs) > 0:
        spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
        init_load_confs = sorted(init_ingest_specs, key=lambda conf: conf.table_name)
        delta_tbl_vs_configs = itertools.groupby(init_load_confs, lambda config: config.table_name)
        tbl_configs = []

        for delta_tbl , configs in delta_tbl_vs_configs:
            tbl_configs.append(list(configs))

        pool = ThreadPool(20)
        result = pool.map(run_init_load_jobs, tbl_configs)
        pool.close()

        result_df = (
            spark.createDataFrame(list(itertools.chain(*result)),'table_name string, schema_id string, error_trace string, duplicate_count int')
            .withColumn("is_complete", isnull("error_trace"))
        )
        (
            DeltaTable.forName(spark, job_ctx.get_init_load_table()).alias('m')
            .merge(result_df.alias('u'),
                f"m.app = '{app}' AND m.table_name = u.table_name AND m.schema_id = u.schema_id")
            .whenMatchedUpdate(
                set = {
                    'm.is_complete' : 'u.is_complete',
                    'm.error_trace' : 'u.error_trace',
                    'm.duplicate_count' : 'u.duplicate_count',
                    'm.updated_by' : lit("init_load_job"),
                    'm.updated_at' : current_timestamp()
                }  
            )
            .execute()
        )

