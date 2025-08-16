import json
import time
import traceback
from enum import Enum
from os import path
import sys
import importlib

import boto3
from boto3.exceptions import ClientError
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

from common.dms import DMSRunner, DMSLoadConfig
from common.context import JobContext
from common.databricks1 import NotebookConfig

spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
spark.conf.set("spark.databricks.delta.properties.default.enableChangeDataFeed", "true")


class SchemaLoadStatus(Enum):
    CDC_INGEST_DISABLED = 1
    METADATA_UPDATED = 2
    GG_ENABLED = 3
    DMS_LOAD_COMPLETED = 4
    INIT_INGEST_COMPLETED = 5
    CDC_INGEST_ENABLED = 6
    FAILURE = 7

def create_load_status_tbl():
    tbl_props = ','.join(['delta.autoOptimize.optimizeWrite = true',
                          'delta.autoOptimize.autoCompact = true',
                          'delta.enableChangeDataFeed = true'])
    spark.sql(f""" 
            CREATE TABLE IF NOT EXISTS {app_del_db}.ssot_schema_load_status (
                src_db STRING NOT NULL,
                src_schema STRING NOT NULL,
                load_status STRING NOT NULL,
                failure_reason STRING,
                job_details MAP<STRING, STRING> NOT NULL,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )USING DELTA TBLPROPERTIES({tbl_props})
    """)

def upsert_status_table(schema_name: str, status: SchemaLoadStatus,
                        failure_reason: str = None):
    notebook_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    curr_job_tags = json.loads(notebook_ctx.toJson())['tags']
    current_job_id = curr_job_tags.get('jobId')
    current_run_id = curr_job_tags.get('runId')
    job_details = {"job_id": current_job_id, "run_id" : current_run_id}

    status = (app_src_db, schema_name, status.name, failure_reason, job_details)
    status_df_schema = StructType([
        StructField("src_db", StringType(), False),
        StructField("src_schema", StringType(), False),
        StructField("load_status", StringType(), False),
        StructField("failure_reason", StringType(), False),
        StructField("job_details", MapType(StringType(), StringType()), False)
    ])
    status_df = spark.createDataFrame([status], schema=status_df_schema) \
            .withColumn("created_at", current_timestamp()) \
            .withColumn("updated_at", current_timestamp())
    
    (DeltaTable
        .forName(spark, f'{app_del_db}.ssot_schema_load_status').alias('main')
        .merge(status_df.alias('updates'),
               'main.src_db = updates.src_db AND '
               'main.src_schema = updates.src_schema')
        .whenNotMatchedInsertAll()
        .whenMatchedUpdate(set = {'main.load_status' : 'updates.load_status',
                                  'main.failure_reason' : 'updates.failure_reason',
                                  'main.updated_at' : 'updates.updated_at'})

        .execute()
    )

def do_init_load(schema_name: str):
    #dms_db_config = DMSDBConfig(app_src_db, src_db_ip, src_db_service_name)
    #dms_app_config = DMSAppConfig(app_name=src_app, db_config = dms_db_config,
                                    # schemas=[schema_name], tables= src_tables)
    # dms_job_config = DMSJobConfid(env, endpoint_id='x99')
    # dms_initial_load =(dms_app_config, dms_job_config)

    # init_load_schema_dir = f'HISTORY/{src_app}/{app_src_db}/{schema_name}'
    # init_load_tbl_dirs = list(map(lambda src_tbl: f'{init_load_schema_dir}/{src_tbl}', src_tables))
    # wait_until(predicate=s3_directories_exist,
        # args = [landing_bucket, init_load_tbl_dirs],
        # timeout_seconds = 10 * 60)

    dms_runner = DMSRunner(
        meta_conf_path = meta_conf_path,
        job_ctx=JobContext(app={src_app}, env={env}, team = 'sgdp')
    )

    load_conf = DMSLoadConfig(
        replication_instance_class='dms.t3.micro',
        max_lob_size_in_kb=50
    )
    print("dms run load is starting ...!")
    dms_runner.run_load(load_conf)


def do_init_ingestion(schema_name: str):
    def databricks_job_has_stopped(run_id: str) -> bool:
        return get_databricks_run_status(run_id) != 'RUNNING'
    
    job_run_id = start_databricks_job(job_name = init_ingest_job)
    wait_until(predicate=databricks_job_has_stopped,
        args = [job_run_id],
        timeout_seconds = 40 * 60)
    
    if get_databricks_run_status(job_run_id) != 'SUCCESS':
        raise ValueError(f"Init ingestion job {init_ingest_job} with run_id {job_run_id} has failed")
    
    for table in delta_tables:
        if spark.table(f"{app_del_db}.{table}").where(f"schema_id = '{schema_name}'").count() == 0:
                       raise ValueError(f"Table {table} for schema {schema_name} has no records after init ingestion")
        
# Invokes jenikins job to enable gg
# This will block until the job complete.
# Also checks if the job results in a success

def enable_gg_replication(schema_name: str):
     gg_jenkins_job_params['Schemalist'] = schema_name
     invoke_jenkins_job(
          gg_jenkins_server_url,
          gg_jenikins_job_name,
          gg_jenkins_job_params
     )


# Metadata is inserted by copying all the fields from previous meta schema version
def insert_schema_metadata(schema_name: str):
    #  insert new meta schema into __db_schema_spec

    db_schema_spec_df = spark.sql(f"select * from {control_db}.__db_schema_spec")

    new_schema_df = db_schema_spec_df.where(db_schema_spec_df.app == 'test') \
                                     .select('app', 'src_db', 'created_at','updated_at', 'created_by', 'updated_by') \
                                     .distinct().withColumn("src_schema", lit(schema_name)) \
                                     .withColumn("id", F.expr("uuid()")) \
                                     .select("id","app","src_schema",'created_at','updated_at', 'created_by', 'updated_by')
    
    new_schema_df.write.format("delta").mode("append").insertInto(f"{control_db}.__db_schema_spec")


# check if the schema exists and cdc_ingest_enabled = 6 then skip
def handle_schema_insert(sch_chg_df, _):
    loaded_schemas_df = spark.table(f"{app_del_db}.ssot_schema_load_status") \
                              .where("load_status = 'CDC_INGEST_ENABLED'") \
                              .select('src_schema')
     
    new_schemas_df = sch_chg_df.where("_change_type = 'insert'") \
                               .select(col(schema_name_col).alias('schema_name')).alias('s') \
                               .join(loaded_schemas_df.alias('l'),col('s.schema_name') == col('l.src_schema'), 'left_outer') \
                               .select('s.*')
     
    new_schemas_df.persist()

    if new_schemas_df.count() > 0:
        databricks_stop_exception = None
        try:
            stop_databricks_job(job_name = cdc_ingest_job)
            log.warning(f"cdc job with name {cdc_ingest_job} has stopped")
            stop_databricks_job(job_name = init_ingest_job)
            log.warning(f"cdc job with name {init_ingest_job} has stopped")
        except Exception:
            databricks_stop_exception = traceback.format_exc()
               
        #   These are background threads responsible for continuously cancelling
        #   any cdc/init job runs. This is done to prevent the ssot-restart-job or
        #   someone else from starting these jobs.
        cdc_job_stop_worker = BackgroundWorker(function=stop_databricks_job, args=[cdc_ingest_job])
        init_job_stop_worker = BackgroundWorker(function=stop_databricks_job, args=[init_ingest_job])
        cdc_job_stop_worker.run()
        init_job_stop_worker.run()

        #   Handle every schema independency in a batch of Schemas 
        #   i.e if there is any error, contain it only for that schema
        #   and continure to process the next schema
        schema_vs_exception = {}
        new_schemas = list(map(lambda ns: ns['schema_name'],
                                new_schemas_df.select('schema_name').collect()))
          
        if not databricks_stop_exception:
            for ns in new_schemas:
                try:
                    #  After every stage, update the status is the status table
                    print("upsert breakpoint")
                    upsert_status_table(schema_name=ns, status=SchemaLoadStatus.CDC_INGEST_DISABLED)
                    log.warning(f"CDC ingestion disabled for schema {ns}, updating metadata tables")

                    print("insert metadata")
                    insert_schema_metadata(schena_name = ns)
                    upsert_status_table(ns, SchemaLoadStatus.METADATA_UPDATED)
                    log.warning(f"Metadata updated for schema {ns}, invoking jenkins job for enabling GG") #stopped at line 236
                    
                    # enable_gg_replication(schema_name=ns)
                    # upsert_status_table(schema_name=ns, status=SchemaLoadStatus.GG_ENABLED)
                    # log.warning(f"GG replication enabled for schema {ns} starting DMS load")

                    # do_init_load(schema_name=ns)
                    # upsert_status_table(schema_name=ns, status=SchemaLoadStatus.DMS_LOAD_COMPLETED)
                    # log.warning(f"DMS load completed for schema {ns} starting init ingestion")

                    # terminate the background thread responsible for cancelling init job runs
                    # before the init do_init_ingestion
                    # init_job_stop_worker.terminate()
                    # do_init_ingestion(schema_name=ns)
                    # upsert_status_table(schema_name=ns, status=SchemaLoadStatus.INIT_INGEST_COMPLETED)
                    # log.warning(f"Init ingestion completed for schema {ns} starting cdc ingestion")
                except Exception:
                        schema_vs_exception[ns] = traceback.format_exc()
        databricks_starts_exception  = None
        try:
            cdc_job_stop_worker.terminate()
            start_databricks_job(job_name=cdc_ingest_job)
            log.warning(f"CDC job with name {cdc_ingest_job} has started")
        except Exception:
            databricks_starts_exception = traceback.format_exc()  

        databricks_job_exception = databricks_starts_exception or databricks_stop_exception

    #   If there are any exceptions, update status to failure in the status table.
    #   and throw an error so that the job fails
        for ns in new_schemas:
            if not databricks_job_exception:
                schema_exception = schema_vs_exception.get(ns)
                status = SchemaLoadStatus.FAILURE if schema_exception else SchemaLoadStatus.CDC_INGEST_ENABLED
                upsert_status_table(schema_name=ns, status=status, failure_reason=schema_exception)
            else:
                schema_vs_exception[ns] = databricks_job_exception
                upsert_status_table(schema_name=ns, status=SchemaLoadStatus.FAILURE,
                                        failure_reason=databricks_job_exception)
            
            if len(schema_vs_exception) > 0:
                failed_schemas = set(schema_vs_exception.keys())
                for fs in failed_schemas:
                    log.error(f"FAILURE DETECTED FOR SCHEMA {fs}")
                    log.error(schema_vs_exception[fs])
                raise ValueError(f"Job has failed for {len(failed_schemas)} schemas."
                                f"Failed schemas are = {failed_schemas}. Check the job"
                                f"logs or {app_del_db}.schema_load_status table for debugging the failure")
    new_schemas_df.unpersist()

def main():
    s3 = boto3.resource('s3')

    def get_object_content_if_exists(bucket: str, key: str):
        try:
            s3.Object(bucket, key).load()
        except ClientError as e:
             if e.response['Error']['Code'] == '404':
                  return None
             else:
                  raise
        resp = s3.Object(bucket, key).get()
        return resp['Body'].read().decode('utf-8').strip()
    
    # if starting version is specified, read change data feed from taht version
    # or else from the last version
    schema_change_tbl_name = f"{app_del_db}.{schema_change_tbl}"
    chkpt_dir = f"_checkpoint_dir/schema_chg_listener/{src_app}"

    if starting_version:
         version = starting_version
    else:
        #  save the version stream started with the checkpoint dir
        # starting the stream everytime with diff version causes spark to discard checkpoints.
        version_file_key = f"{chkpt_dir}/version.txt"
        # if we have checkpoint_dir/version.txt then always start from that version
        # this results in spark resuming its execution from the checkpoint.
        chkpt_version = get_object_content_if_exists(landing_bucket, version_file_key)
        if chkpt_version:
             version = int(chkpt_version)
        else:
             latest_version = DeltaTable.forName(spark, schema_change_tbl_name) \
                .history(1)\
                .select("version") \
                .collect()[0].version
             
             s3.Object(landing_bucket, version_file_key).put(Body=str(latest_version))
             version = latest_version
        
        version += 1
        schema_change_stream = spark.readStream.format("delta") \
            .option("readChangeFeed", 'true') \
            .option("startingVersion", version) \
            .option(schema_change_tbl_name)
        
        # if starting_version is not specified, start streaming chages after teh latest version
        if not starting_version:
             schema_change_stream = schema_change_stream.where(f"_commit_version > {version}")

        schema_change_stream_writer = schema_change_stream.writeStream
        if streaming_mode == 'triggered':
            schema_change_stream_writer = schema_change_stream_writer.trigger(once=True)

        schema_change_stream_writer \
            .option("checkpointLocation", f"s3://{landing_bucket}/_checkpoint_dir/schema_chg_listener/{src_app}/") \
            .foreachBatch(lambda sch_chg_df, batch_id: handle_schema_insert(sch_chg_df, batch_id) ) \
            .start()

if __name__ == '__main__':
    env = dbutils.widgets.get("env").lower()
    if env == "uat":
        raise ValueError(
            "This job cannot run in UAT environment due to lack of GG automation (jenkins job)"
        )
    src_app = dbutils.widgets.get("src_app")
    src_db_service_name = dbutils.widgets.get("src_db_service_name")
    src_db_ip = dbutils.widgets.get("src_db_ip")
    src_tables = dbutils.widgets.get("src_tables").split(",")

    init_ingest_job = dbutils.widgets.get("init_ingest_job")
    cdc_ingest_job = dbutils.widgets.get("cdc_ingest_job")
    schema_change_tbl = dbutils.widgets.get("schema_change_tbl")
    schema_name_col = dbutils.widgets.get("schema_name_col")

    try:
        starting_version = dbutils.widgets.get("starting_version")
    except Exception as e:
        if "InputWidgetNotDefined" in str(e):
            starting_version = None
    else:
        raise e
    
    try:
        streaming_mode = dbutils.widgets.get("streaming_mode")
        if streaming_mode.lower() not in {'continuous', 'triggered'}:
             raise ValueError("Only supports two modes i.e continuous & triggered")
    except Exception as e:
        if "InputWidgetNotDefined" in str(e):
            streaming_mode = "triggered" 
        else:
            raise e
    
    gg_jenkins_server_url = dbutils.widgets.get("gg_jenkins_server_url")
    gg_jenkins_job_name = dbutils.widgets.get("gg_jenkins_job_name")
    gg_jenkins_job_params = dbutils.widgets.get("gg_jenkins_job_params")

    delta_tables = list(map(lambda tbl: tbl.replace("$", "__DOL").strip(), src_tables))
    control_db = f"ssot_platform_meta_{env}"
    landing_bucket = f"adpdc-ssot-landing-zone-{env.replace('prod', 'prd')}"
    meta_conf_path = NotebookConfig.get_arg('metadata_config_path')

    src_db_vs_delta_db = spark.sql(f"select * from {control_db}.__db_schema_spec").filter(col("app") == src_app.lower()).select("app","src_db").\
        join(spark.sql(f"select * from {control_db}.app_spec").filter(col("app") == src_app.lower())).select ("src_db","delta_db").collect()
    
    if len(src_db_vs_delta_db) == 0:
        raise ValueError("No rows found in table map for app {src_app}")
    
    def validate_kv_param(kv_param):
        kv_splits = kv_param.split("=")
        if len(kv_splits) == 2:
            return (kv_splits[0], kv_splits[1])
        else:
            raise ValueError("param validation errot. key value pair must be separated by '='")
    
    gg_jenkins__job_params = dict(
        map(lambda key_value: validate_kv_param(key_value)),
        gg_jenkins_job_params.split(",")
    )

    app_src_db , app_del_db = src_db_vs_delta_db[0].src_db, src_db_vs_delta_db[0].delta_db
    create_load_status_tbl()
    main()