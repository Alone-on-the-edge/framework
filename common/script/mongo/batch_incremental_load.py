# Databricks notebook source
import math
import traceback
from multiprocessing.pool import ThreadPool

from pyspark.sql import SparkSession, functions as F

from common.context import JobContext
from common.databricks1 import NotebookConfig
from common.logger import get_logger
from common.utils import delete_s3_dir 
from script.mongo.common import MongoUtils, MongoCollectionConfig, MongoConnectionConfig, get_safe_upper_bound_ts

spark = SparkSession.getActiveSession()
logger = get_logger(__name__)


def run_load(conn_conf: MongoConnectionConfig):
    def run_load_(coll_conf: MongoCollectionConfig):
        try:
            if coll_conf.lower_bound_ts_millis is None:
                logger.info(f"Sourcing lower_bound_ts from checkpoints "
                            f"for {conn_conf.connection_id} & {coll_conf.collection_name}  ")
            else:
                logger.info(f"Sourcing lower_bound_ts from config "
                            f"for {conn_conf.connection_id} & {coll_conf.collection_name}  ")
                
            lower_bound_ts = coll_conf.lower_bound_ts_millis or mongo_utils.get_checkpoint_ts(coll_conf.delta_table,
                                                                                           conn_conf.connection_id)
            upper_bound_ts = coll_conf.upper_bound_ts_millis or get_safe_upper_bound_ts()
            staging_dir = mongo_utils.get_staging_dir(coll_conf.delta_table, conn_conf.connection_id)
            delete_s3_dir(staging_dir)

            if coll_conf.chunk_duration_millis is not None:
                num_chunks = math.ceil((upper_bound_ts - lower_bound_ts) / coll_conf.chunk_duration_millis)    
                for i in range(num_chunks):
                    chunk_lower_bound_ts = lower_bound_ts + (i * coll_conf.chunk_duration_millis)
                    # Extract from Mongo uses $gt and $lt for upper and lower bounds, so adding 1ms to upper-bound
                    chunk_upper_bound_ts = min(chunk_lower_bound_ts + coll_conf.chunk_duration_millis + 1,
                                               upper_bound_ts)
                    mongo_utils.extract_and_stage_mongo_data(
                        conn_conf, coll_conf,
                        chunk_lower_bound_ts, chunk_upper_bound_ts
                    )
            else:
                mongo_utils.extract_and_stage_mongo_data(
                    conn_conf, coll_conf,
                    lower_bound_ts, upper_bound_ts
                )
            error_trace = None
        except:
            logger.error(f"Error while extracting data for collection {coll_conf.collection_name} , exc_info=True")
            error_trace = traceback.format_exc()
        
        return conn_conf.connection_id, coll_conf.collection_name, error_trace
    
    with ThreadPool(coll_thread_pool_size) as coll_pool:
        status_list = coll_pool.map(run_load_,mongo_utils.config.collections)
    return status_list


if __name__ == "__main__":
    job_ctx = JobContext.from_notebook_config()
    config_path = NotebookConfig.get_arg("mongo_config_path")
    db_thread_pool_size =NotebookConfig.get_int_arg("db_thread_pool_size", default=None, allow_null_default=True)
    coll_thread_pool_size = NotebookConfig.get_int_arg("coll_thread_pool_size", default=2)

    mongo_utils = MongoUtils(config_path, job_ctx)
    with ThreadPool(db_thread_pool_size) as db_pool:
        collection_status_list = db_pool.map(run_load, mongo_utils.config.connections)
    mongo_utils.log_status(collection_status_list)
    


