import json
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

spark = SparkSession.getActiveSession()
logger = get_logger(__name__)

IGNORABLE_ERRORS = {
    "'buckets' field must be greater tahn 0, but foun: 0" # This error occurs when the collection is empty
}

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


@dataclass
class MongoConnectionConfig:
    connection_id: str
    connection_string: str


@dataclass
class MongoConfig:
    collections: list[MongoCollectionConfig]
    connections: list[MongoConnectionConfig]
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
            ) for conn in config_dict["connections"]
        ]
        config_dict['collections'] = [
            MongoCollectionConfig(
                collection_name=col["collection_name"],
                delta_table=col["delta_table"]
            ) for col in config_dict["collections"]
        ]
        return MongoConfig(**config_dict)


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
                "schemaHints": "_id string, document string",
                "collection": collection
            }
            bounds = []
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
             
        def extract_from_mongo(self, connection: MongoConnectionConfig, collection: str,
                               lower_bound_ts: int = None, upper_bound_ts: int = None) -> DataFrame | None:
            mongo_opts = self.get_options(connection, collection, lower_bound_ts, upper_bound_ts)
            logger.info(
                f"Running extract for collection={collection} & connection={connection.connection_id}"
                f"for lower_bound={lower_bound_ts} & upper_bound={upper_bound_ts}"
            )
            df = (
                spark.read
                .format("mongodb")
                .options(**mongo_opts)
                .load()
                .persist(StorageLevel.MEMORY_AND_DISK)
            )
            if not df.isEmpty():
                return self.transform_from_delta(connection, df)
            else:
                logger.warn(f"Nothing to extract for {collection}")
                return None
            
        def transform_from_delta(self, connection: MongoConnectionConfig, df: DataFrame) -> DataFrame:
            return (
                df
                .withColumn("document_id", F.col("document._id"))
                .withColumn("document", F.parse_json("document"))
                .withColumn("sgdp_org_id", F.variant_get("document", f"$.{self.config.client_id_field}", "string"))
                .withColumn("event_ts_millis",F.variant_get("document", f"$.{self.config.event_timestamp_field}", "long"))
                .withColumn("event_date", F.to_date(F.to_timestamp(F.col("event_ts_millis")/1000)))
                .withColumn("schema_id", F.lit(connection.connection_id))
            )
        
        def get_full_table_name(self, coll_config: MongoCollectionConfig) -> str:
            return f"{self.config.delta_db}.{coll_config.delta_table}"
        
        def log_status(self, collection_status_list: list[list[tuple[str, str, str | None]]]):
            mongo_status_log_tbl = f"{self.job_ctx.get_control_db()}.mongo_status_log"
            collection_status_list = list(chain.from_iterable(collection_status_list))
            (
                spark.createDataFrame(collection_status_list, "connection_id string, collection string", "error_trace string")
                .withColumn("app", F.lit(self.job_ctx.app))
                .withColumn("created_at", F.current_timestamp())
                .withColumn("created_by", F.lit(self.entry_point_notebook_name))
                .select("app", "connection_id", "collection", "error_trace", "created_at", "created_by")
                .write
                .partitionBy("app")
                .mode("append")
                .saveAsTable(mongo_status_log_tbl)
            )
            errors = [error for _, _, error in collection_status_list 
                      if error and not any(ignorable_error in error for ignorable_error in IGNORABLE_ERRORS)]
            if len(errors) > 0:
                raise ValueError(
                    f"Atleast one collection failed with an error, query {mongo_status_log_tbl} for details"
                    )
           
        def get_metadata_fields(self):
            return [
                F.lit("created_at"),
                F.current_timestamp(),
                F.lit("created_by"),
                F.lit(self.entry_point_notebook_name),
                F.lit("job_info"),
                F.lit(json.dumps(JobUtils().get_current_job_details()))
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
                return "maintenace", coll_config.delta_table, error_trace 
            
            with ThreadPool() as pool:
                maintenance_status_list = pool.map(run_maintenance_, self.config.collections)
            return maintenance_status_list  
        