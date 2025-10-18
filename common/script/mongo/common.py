import json
import time
import traceback
from dataclasses import dataclass
from itertools import chain
from multiprocessing.pool import ThreadPool
from urllib.parse import quote_plus

from delta import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import functions as F, DataFrame, SparkSession
from pyspark.sql.types import *

from common.context import JobContext
from common.databricks1 import get_entry_point_notebook_name, JobUtils
from common.logger import get_logger
from common.utils import split_s3_path, s3_path_exists, get_s3_object_content_if_exists, save_s3_object, \
    save_s3_object 

spark = SparkSession.getActiveSession()
logger = get_logger(__name__)

IGNORABLE_ERRORS = {
    "'buckets' field must be greater tahn 0, but foun: 0" # This error occurs when the collection is empty
}
MONGO_STAGING_DIR = "mongo_staging"
MONGO_CHKPT_DIR = "mongo_extracts"
DATA_FRESHNESS_BUFFER_MILLIS = 15  * 60 * 1000 # PULL records at most 15 minutes old to avoid race conditions.

delta_table_schema = StructType([
    StructField("document_id", StringType(), nullable=False),
    StructField("document", VariantType(), nullable=False),
    StructField("sgdp_org_id", StringType(), nullable=False),
    StructField("event_date", DateType(), nullable=False),
    StructField("schema_id", StringType(), nullable=False),
    StructField("__meta" , MapType(StringType(), StringType()), nullable=True)
])


@dataclass
class MongoCollectionConfig:
    collection_name: str
    delta_table: str
    lower_bound_ts_millis: int | None = None
    upper_bound_ts_millis: int | None = None
    chunk_duration_millis: int | None = None


@dataclass
class MongoConnectionConfig:
    connection_id: str
    connection_string: str
    partitioner: str | None 
    

@dataclass
class MongoConfig:
    connections: list[MongoConnectionConfig]
    collections: list[MongoCollectionConfig]    
    client_id_field: str
    event_timestamp_field: str
    delta_db: str

    @staticmethod
    def load(json_config_path: str):
        with open(json_config_path, "r") as f:
            config_dict = json.load(f)
        config_dict['connections'] = [
            MongoConnectionConfig(
                connection_id=conn["connection_id"],
                connection_string=conn["connection_string"]
                partitioner=conn.get("partitioner", None)
            ) for conn in config_dict["connections"]
        ]
        config_dict['collections'] = [
            MongoCollectionConfig(
                collection_name=coll["collection_name"],
                delta_table=coll["delta_table"]
                lower_bound_ts_millis=coll.get("lower_bound_ts_millis", None),
                upper_bound_ts_millis=coll.get("upper_bound_ts_millis", None),
                chunk_duration_millis=coll.get("chunk_duration_millis", None)
            ) for coll in config_dict["collections"]
        ]
        return MongoConfig(**config_dict)


def get_safe_upper_bound_ts() :
    return int(time.time() * 1000) - DATA_FRESHNESS_BUFFER_MILLIS


class MongoUtils:
        def __init__(self, config_path: str, job_ctx: JobContext):
             self.config = MongoConfig.load(config_path)
             self.job_ctx = job_ctx
             self.entry_point_notebook_name = get_entry_point_notebook_name()

        def get_options(self, connection: MongoConnectionConfig, collection: str,
                        lower_bound_ts: int = None, upper_bound_ts: int = None) -> dict[str, str]:
            def get_auth_connection_string():
                username, password = self.job_ctx.resolve_secrets(db_name=connection.connection_id, secret_mode="multi")
                protocol, rest = connection.connection_string.split("://", 1)
                #URL encode credentials
                encoded_username, encoded_password = quote_plus(username), quote_plus(password)
                return f"{protocol}://{encoded_username}:{encoded_password}@{rest}"
            
            opts = {
                "connection.uri": get_auth_connection_string(),
                "collection": collection
            }
            if connection.partitioner is not None:
                opts["partitioner"] = connection.partitioner
            # whenever bounds are set, mongo spark partitioner fires a count query on the resut of the bound
            # this can be very expensive when doing a full-load i.e lower_bound_ts = 0
            # so, avoid setting any bounds when lower_bound_ts = 0.
            bounds = []
            if lower_bound_ts != 0:
                if lower_bound_ts is not None:
                    bounds.append('"$gt": %s' % lower_bound_ts)
                if upper_bound_ts is not None:
                    bounds.append('"$lt": %s' % upper_bound_ts)

            if len(bounds) > 0:
                bounds = ", ".join(bounds)
                match_condition = '{$match: { "%s": { %s } } }' % (self.config.event_timestamp_field, bounds)
                agg_pipeline = '[ %s, {$project: {document: "$$ROOT"}} ]' % match_condition
            else:
                agg_pipeline = '[ {$project: {document: "$$ROOT"}} ]'
            opts["aggregation.pipeline"] = agg_pipeline
            return opts
             
        def extract_from_mongo(self, 
                               conn_conf: MongoConnectionConfig,
                               coll_conf: MongoCollectionConfig,
                               lower_bound_ts: int = None,
                               upper_bound_ts: int = None) -> DataFrame | None:
            mongo_opts = self.get_options(conn_conf, coll_conf.collection_name, lower_bound_ts, upper_bound_ts)
            logger.info(
                f"Running extract for collection={coll_conf.collection_name} & connection={conn_conf.connection_id}"
                f"for lower_bound={lower_bound_ts} & upper_bound={upper_bound_ts}"
            )
            mongo_extract_schema = StructType([
                StructField("_id", StringType(), nullable=False),
                StructField("document", StringType(), nullable=False)
            ])
            df = (
                spark.read
                .format("mongodb")
                .options(**mongo_opts)
                .schema(mongo_extract_schema)
                .load()
                .persist(StorageLevel.MEMORY_AND_DISK)
            )
            if not df.isEmpty():
                df = self.transform_from_delta(conn_conf.connection_id, df)
                staging_dir = self.get_staging_dir(coll_conf.delta_table, conn_conf.connection_id)
                df.write.mode("overwrite").parquet(staging_dir)
                logger.info(
                    f"Successfully staged records for collection {coll_conf.collection_name  } from connection {conn_conf.connection_id} "
                    f"for lower_bound={lower_bound_ts} & upper_bound={upper_bound_ts}"
                ) 
            else:
                logger.info(
                    f"Nothing to extract for {coll_conf.collection_name} from connection {conn_conf.connection_id} "
                    f"for lower_bound={lower_bound_ts} & upper_bound={upper_bound_ts}"
                )
            df.unpersist()

        def transform_for_delta(self, connection_id: str, df: DataFrame) -> DataFrame:
            return (
                df
                .withColumn("document_id", F.col("document._id"))
                .withColumn("document", F.parse_json("document"))
                .withColumn("sgdp_org_id", F.variant_get("document", f"$.{self.config.client_id_field}", "string"))
                .withColumn("event_ts_millis",F.variant_get("document", f"$.{self.config.event_timestamp_field}", "long"))
                .withColumn("event_date", F.to_date(F.to_timestamp(F.col("event_ts_millis")/1000)))
                .withColumn("schema_id", F.lit(connection_id))
                .withColumn("__meta", F.create_map(*self.get_metadata_fields()))
            )
        
        def get_full_table_name(self, coll_config: MongoCollectionConfig) -> str:
            return f"{self.config.delta_db}.{coll_config.delta_table}"
        
        def log_status(self, collection_status_list: list[list[tuple[str, str, str | None]]]):
            # mongo_status_log_tbl = f"{self.job_ctx.get_control_db()}.mongo_status_log"
            mongo_status_logs_path = f"{self.job_ctx.get_metadata_staging_dir()}/mongo_status_logs"
            collection_status_list = list(chain.from_iterable(collection_status_list))
            (
                spark.createDataFrame(collection_status_list, "connection_id string, collection string", "error_trace string")
                .withColumn("app", F.lit(self.job_ctx.app))
                .withColumn("created_at", F.current_timestamp())
                .withColumn("created_by", F.lit(self.entry_point_notebook_name))
                .withColumn("metadata", F.create_map(*self.get_metadata_fields()))
                .select("app", "connection_id", "collection", "error_trace", "created_at", "created_by", "metadata")
                .write
                .partitionBy("app")
                .mode("append")
                .option("mergeSchema", "true")
                .save(mongo_status_logs_path)
            )
            errors = [error for _, _, error in collection_status_list 
                      if error and not any(ignorable_error in error for ignorable_error in IGNORABLE_ERRORS)]
            if len(errors) > 0:
                raise ValueError(
                    f"Atleast one collection failed with an error, query {mongo_status_logs_path} for details"
                    )
           
        def get_metadata_fields(self):
            return [
                F.lit("created_at"),
                F.current_timestamp(),
                F.lit("created_by"),
                F.lit(self.entry_point_notebook_name),
                F.lit("job_info"),
                F.lit(json.dumps(JobUtils(is_uc_cluster=self.job_ctx.is_uc_cluster).get_current_job_details()))
            ]
        
        def run_maintenance(self) -> list[tuple[str, str, str | None]]:
            def run_maintenance_(coll_config: MongoCollectionConfig):
                try:
                    delta_table = DeltaTable.forName(spark, self.get_full_table_name(coll_config))
                    delta_table.optimize().executeCompaction()
                    delta_table.vacuum()
                    error_trace = None
                except:
                    error_trace = traceback.format_exc()
                return "maintenance", coll_config.collection_name, error_trace 
            
            with ThreadPool() as pool:
                maintenance_status_list = pool.map(run_maintenance_, self.config.collections)
            return maintenance_status_list  
        
        def get_staging_dir(self, delta_table: str, connection_id: str) -> str:
            return f"{self.job_ctx.get_checkpoints_dir(MONGO_STAGING_DIR)}/{delta_table}/{connection_id}"
        
        def read_from_staging(self, delta_table: str, connection_id: str) -> DataFrame | None:
            staging_dir = self.get_staging_dir(delta_table, connection_id)
            bucket, key = split_s3_path(staging_dir)
            if s3_path_exists(bucket, key, self.job_ctx.get_uc_boto_session()):
                return spark.read.format("parquet").load(staging_dir)
            else:
                return None
            
        def get_checkpoint_location(self, delta_table: str, connection_id: str) -> (str, str):
            base_chkpt_dir = self.job_ctx.get_checkpoints_dir(MONGO_CHKPT_DIR)
            chkpt_file_path = f"{base_chkpt_dir}/{delta_table}/{connection_id}/last_processed_timestamp.txt"
            return split_s3_path(chkpt_file_path)
        
        def get_checkpoint_ts(self, delta_table: str, connection_id: str) -> int:
            chkpt_bucket, chkpt_key = self.get_checkpoint_location(delta_table, connection_id)
            chkpt_ts = get_s3_object_content_if_exists(
                bucket=chkpt_bucket,
                key=chkpt_key, 
                boto3_session=self.job_ctx.get_uc_boto_session()
            )
            return int(chkpt_ts) if chkpt_ts else 0
        
        def save_checkpoint(self, delta_table: str, connection_id: str, chkpt_ts: int):
            chkpt_bucket, chkpt_key = self.get_checkpoint_location(delta_table, connection_id)
            save_s3_object(
                bucket=chkpt_bucket,
                key=chkpt_key,
                content=str(chkpt_ts),
                boto3_session=self.job_ctx.get_uc_boto_session()
            )