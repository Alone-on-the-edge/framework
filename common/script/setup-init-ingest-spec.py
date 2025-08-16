import sys
sys.path.append("d:\Project\ssot")

from datetime import datetime

from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from common.context import JobContext


def get_current_user():
    current_user = (spark.sql("select current_user")).collect()[0][0]

    return current_user

def set_up_tab_init_ingest_spec(db_schema_specs):
    list_data = []
    for row in db_schema_specs:
        db_name = row.src_db.lower()
        schema = row.src_schema.lower()
        for flow_spec in flow_specs:
            table_name = flow_spec.src_table
            init_ingest_row = (app,table_name,row.id, "dms",
                               f"{job_ctx.get_dms_load_dir(include_bucket=True)})/{db_name}/{schema}/{table_name}")
            
            list_data.append(init_ingest_row)

    init_ingest_spec_df = (
        spark.createDataFrame(list_data, ['app','table_name','schema_id', 'load_type','init_load_path'])
        .withColumn('is_complete',lit(False))
        .withColumn('duplicate_count',lit(0))
        .withColumn('error_trace',lit(None))
        .withColumn('created_at',lit(datetime.now()))
        .withColumn('updated_at',lit(datetime.now()))
        .withColumn('created_by',lit(get_current_user()))
        .withColumn('updated_by',lit(get_current_user()))
    )
    (
        DeltaTable.forName(spark, job_ctx.get_init_load_table()).alias('m')
        .merge(init_ingest_spec_df.alias('u')),
        f"m.app = u.app AND m.table_name = u.table_name AND m.schema_id = u.schema_id AND m.app = '{job_ctx.app}'"
        .whenNotMatchedInsertAll()
        .execute()
    )

    if __name__ == "__main__":
        spark = SparkSession.getActiveSession()
        job_ctx = JobContext.from_notebook_config()
        flow_specs = job_ctx.get_app_spec().flow_specs
        app = job_ctx.app
        schema_meta_query = f"""select id,src_db,src_schema
        from {job_ctx.get_db_schema_spec_tbl()}
        where app = '{app}' """
        db_schema_spec = spark.sql(schema_meta_query).collect()
        set_up_tab_init_ingest_spec(db_schema_spec)