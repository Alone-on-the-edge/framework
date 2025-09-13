# Databricks notebook source
import traceback
from multiprocessing.pool import ThreadPool

from pyspark.sql import SparkSession, functions as F

from common.context import JobContext
from common.databricks1 import NotebookConfig
from common.logger import get_logger
from common.utils import get_s3_object_content_if_exists, save_s3_object 
from script.mongo.common import MongoUtils, MongoCollectionConfig, delta_table_schema, MongoConnectionConfig

spark = SparkSession.getActiveSession()
logger = get_logger(__name__)


def run_batch_incremental_load(connection: MongoConnectionConfig):
    def run_batch_incremental_load_(coll_config: MongoCollectionConfig):
        try:
            chkpt_file = f"{base_chkpt_dir}/{coll_config.delta_table}/{connection.connection_id}/last_processed_timestamp.txt"
            chkpt_ts = get_s3_object_content_if_exists(bucket=job_ctx.get_landing_buket(), key=chkpt_file)
            chkpt_ts = int(chkpt_ts) if chkpt_ts else 0

            df = mongo_utils.extract_from_mongo(connection, coll_config.collection_name, lower_bound_ts=chkpt_ts)
            if df:
                df = df.withColumn("__meta", F.create_map(*mongo_utils.get_metadata_fields()))
                (
                    df.select(*delta_table_schema.fieldNames()).write.mode("append")
                    .saveAsTable(mongo_utils.get_full_table_name(coll_config))  
                )

                last_processed_timestamp = df.select(F.max("event_ts_millis")).collect()[0][0]
            logger.info(
                f"Last processed timestamp for collection {coll_config.collection_name} & connection_i {connection.connection_id} = {last_processed_timestamp}"
                )
            save_s3_object(
                bucket=job_ctx.get_landing_buket(),
                key=chkpt_file,
                content=str(last_processed_timestamp)
            )
            df.persist()
            error_trace = None
        except:
            logger.error(f"Error while extracting data for collection {coll_config.collection_name}", exc_info=True)
            error_trace = traceback.format_exc()
        
        return connection.connection_id, coll_config.collection_name, error_trace
    
    with ThreadPool(coll_thread_pool_size) as coll_pool:
        status_list = coll_pool.map(run_batch_incremental_load_,mongo_utils.config.collections)
    return status_list


if __name__ == "__main__":
    job_ctx = JobContext.from_notebook_config()
    config_path = NotebookConfig.get_arg("mongo_config_path")
    db_thread_pool_size =NotebookConfig.get_int_arg("db_thread_pool_size",default=None,allow_null_default=True)
    coll_thread_pool_size = NotebookConfig.get_int_arg("coll_thread_pool_size", default=2)

    base_chkpt_dir = job_ctx.get_checkpoints_dir('mongo-extracts', include_bucket=False)
    mongo_utils = MongoUtils(config_path, job_ctx)

    with ThreadPool(db_thread_pool_size) as db_pool:
        collection_status_list = db_pool.map(run_batch_incremental_load, mongo_utils.config.connections)
    
    maintenance_status_list = mongo_utils.run_maintenance()
    mongo_utils.log_status([*collection_status_list, maintenance_status_list])

