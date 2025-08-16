import sys
sys.path.append("d:\Project\ssot")

import functools
import typing
from abc import ABC , abstractmethod
from dataclasses import dataclass
from datetime import datetime
from multiprocessing.pool import ThreadPool

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F, Window
from pyspark.sql.types import BooleanType, StringType, ArrayType

from common.context import JobContext
from common.control_spec import FlowSpec
from common.logger import get_logger
from common.metadata.config import MetadataConfig
from common.types.merge_type import merge_schema, merge_schema_from_ddl
from common.types.spark_struct import struct_from_ddl, struct_to_ddl
from common.utils import cast_with_cdc_schema, sanitize_column_name

BRONZE_SPARK_CONF = {
    "active_flow_conf" : {},
    "inactive_flow_conf" : {}
}

SILVER_SPARK_CONF = {
    "spark.databricks.io.cache.enabled" : "true",
    "spark.sql.adaptive.coalescePartitions.minPartitionSize" : "8MB",
    "spark.databricks.delta.optimizeWrite.enabled" : "true",
    "spark.databricks.delta.autoCompact.enabled" : "true",
    "spark.databricks.eventLog.rolloverIntervalSeconds" : "300",
    "spark.databricks.io.cache.compression.enabled" : "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes" : "64MB",
    "spark.databricks.io.cache.maxDiskUsage" : "75g",
    "spark.databricks.io.cache.maxMetaDataCache" : "1g",
    "spark.sql.shuffle.partitions" : "600",
    "dataflow.maxDecimalScale" : "6"
}

BRONZE_TABLE_PROPS = {
    "delta.deletedFileRetentionDuration" : "interval 7 days",
    "delta.logRetentionDuration" : "interval 14 days",
    "pipelines.autoOptimize.managed" : "true",
    "delta.autoOptimize.optimizeWrite" : "true"
}

SILVER_TABLE_PROPS = {
    "delta.deletedFileRetentionDuration" : "interval 2 days",
    "delta.logRetentionDuration" : "interval 14 days",
    "pipelines.autoOptimize.managed" : "true",
    "delta.autoOptimize.optimizeWrite" : "true"
}

spark = SparkSession.getActiveSession()

logger = get_logger('metadata_provider')


@dataclass
class JDBCOptions:
    jdbc_driver: str
    jdbc_url_prefix: str
    extra_opts: typing.Dict[str, str]

@dataclass
class SystemTableSpec:
    table_name: str
    table_filter_col: str
    schema_filter_col: str
    schema_name: str = None
    select_cols: str = None
    table_prefix: str = None

    def __post__init__(self):
        self.table_name = self.table_name.lower()
        if self.schema_name is not None:
            self.schema_name = self.schema_name.lower()
        if self.select_cols is None:
            self.select_cols = '*'



@F.udf
def get_delta_table_path(catalog_id: str, delta_db: str, delta_table: str,
                         version: int):
    import boto3
    glue = boto3.client("glue","us-east-1")
    db_path = glue.get_database(CatalogId=catalog_id, name=delta_db)["Database"]["LocationUri"]
    return f"{db_path.rstrip('/')}/v{version}/{delta_table}"


class MetadataProvider(ABC):
    NAME = 'METADATA_PROVIDER'

    def __init__(self, meta_conf: MetadataConfig, job_ctx: JobContext):
        self.meta_conf = meta_conf
        self.job_ctx = job_ctx
        self.current_ts = datetime.now()
        self.db_schema_spec_tbl = self.job_ctx.get_db_schema_spec_tbl()  # f"{self.get_control_db()}.__db_schema_spec"
        self.col_spec_tbl = self.job_ctx.get_column_spec_tbl() # f"{self.get_control_db()}.__column_spec"
        self.key_spec_tbl = self.job_ctx.get_key_spec_tbl() #  return f"{self.get_control_db()}.__key_spec"
        self.db_schema_spec_df: DataFrame

    @property
    @abstractmethod
    def jdbc_options(self) -> JDBCOptions:
        pass

    @property
    @abstractmethod
    def system_tables(self) -> typing.List[SystemTableSpec]:
        pass

    def _resolve_meta_table_on_delta(self, meta_tbl_name: str, table_prefix: str):
        """
        For the _resolve_meta_table_on_delta method, the constructed metadata table name depends on the values returned by get_control_db(), 
        the src_db_type from meta_conf, the meta_tbl_name, and the table_prefix.

        meta_tbl_name = dba_tab_columns

        o/p: "ssot_platform_meta_fit.__oracle_dba_tab_columns"
        """
        if table_prefix is None:
            metadata_table = f"{self.job_ctx.get_control_db()}.__{self.meta_conf.src_db_type}_{meta_tbl_name}"
        else:
            metadata_table = f"{self.job_ctx.get_control_db()}.__{self.meta_conf.src_db_type}_{table_prefix}_{meta_tbl_name}"
        return metadata_table
    
    @staticmethod
    def _fmt_for_in_clause(l: list):
        quoted_strings = ','.join([f"'{i}'" for i in l])
        return f"{quoted_strings}"
    
    def query_via_jdbc(self, query:str, db_conf: MetadataConfig.DBConfig) -> DataFrame:
        username, password = self.job_ctx.resolve_secrets(db_conf.database_alias, self.meta_conf.secret_mode)
        jdbc_opts = {
            "url" : f"{self.jdbc_options.jdbc_url_prefix}{db_conf.host}:{db_conf.port}",
            "user" : username,
            "password" : password,
            "driver" : self.jdbc_options.jdbc_driver,
            **self.jdbc_options.extra_opts
        }
        if self.meta_conf.src_db_type == 'sqlserver':
            jdbc_opts["url"] += f";databaseName={db_conf.database};encrypt=true;trustServerCertificate=true;"
        else:
             jdbc_opts["url"] += f"/{db_conf.database}"
            
        logger.info(f"Executing query = \n{query}")
        return (
            spark.read.format("jdbc")
            .options(**jdbc_opts)
            .option("query", query)
            .load()
        )
    
    @staticmethod
    def delta_table_exists(delta_table_name: str) -> bool:
        delta_table_splits = delta_table_name.split('.')
        if spark._jsparkSession.catalog().tableExists(delta_table_splits[0], delta_table_splits[1]):
            return True
        return False
    
    @staticmethod
    def __update_meta_table_schema(data_schema, delta_table_name: str):
        if MetadataProvider.delta_table_exists(delta_table_name):
            delta_table_df = spark.table(delta_table_name)
            delta_schema = delta_table_df.schema
            merged_schema = merge_schema_from_ddl(struct_to_ddl(delta_schema), struct_to_ddl(  ), None)
            if merged_schema.merged_schema != delta_schema:
                delta_table_df = cast_with_cdc_schema(delta_table_df, cdc_schema_ddl=merged_schema.schema_to_ddl())
                (
                    delta_table_df
                    .write
                    .partitionBy("app")
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .saveAsTable(delta_table_name)
                )

    def __query_sys_table(self, sys_tbl_spec: SystemTableSpec,
                          db_conf: MetadataConfig.DBConfig,
                          src_tables: typing.List[str]):
        """
        This method constructs and runs SQL queries on the Oracle database to retrieve metadata about tables and filters them based on given configurations.
        sys_table_name = sys_tbl_spec.table_name if sys_tbl_spec.schema_name is None else f"{sys_tbl_spec.schema_name}.{sys_tbl_spec.table_name}"
        
        sys_tbl_spec.table_name = dba_tab_columns
       
        query = f"
        SELECT {sys_tbl_spec.select_cols}
        FROM {sys_table_name}"

        if sys_tbl_spec.table_filter_col is not None:
            query = f"{query} WHERE {sys_tbl_spec.table_filter_col} IN {MetadataProvider._fmt_for_in_clause(src_tables)}"

        Query output:for below conf file
        db_conf = {
            "host": "abc.com",
            "port": 1521,
            "database": "database1",
            "database_alias": "db_alias1",
            "schemas": ["schema1", "common"]
        } 

        src_tables = ["table1", "table2", "table3"]

        sys_tbl_spec = SystemTableSpec(
            table_name='dba_tab_columns',
            table_filter_col='table_name',
            schema_filter_col='owner'
        )

        SELECT *
        FROM dba_tab_columns
        WHERE table_name IN ('table1', 'table2', 'table3') AND owner = 'schema1'   

        SELECT *
        FROM dba_tab_columns
        WHERE table_name IN ('table1', 'table2', 'table3') AND owner = 'common'         
        """

        sys_table_name = sys_tbl_spec.table_name if sys_tbl_spec.schema_name is None \
            else f"{sys_tbl_spec.schema_name}.{sys_tbl_spec.table_name}"
        
        query = f"""
                SELECT {sys_tbl_spec.select_cols}
                FROM {sys_table_name}"""
        
        if sys_tbl_spec.table_filter_col is not None:
            query = f"{query} WHERE {sys_tbl_spec.table_filter_col} IN {MetadataProvider._fmt_for_in_clause(src_tables)}"
        
        def run_query(schema_to_filter: str):
            """
            This nested function constructs and runs the final query for each schema in db_conf.schemas. It adds an additional WHERE clause to filter by schema if necessary.
            """
            if sys_tbl_spec.table_filter_col is not None and sys_tbl_spec.schema_filter_col is not None:
                schema_filter_query = f"{query} AND {sys_tbl_spec.schema_filter_col} = '{schema_to_filter}'"
            elif sys_tbl_spec.schema_filter_col is not None:
                schema_filter_query = f"{query} WHERE {sys_tbl_spec.schema_filter_col} = {schema_to_filter}' " 
            else:
                schema_filter_query = query
            return self.query_via_jdbc(schema_filter_query, db_conf)
        
        pool = ThreadPool(self.meta_conf.thread_pool_size)
        dfs = pool.map(run_query, db_conf.schemas)
        df = functools.reduce(lambda df1, df2: df1.union(df2), dfs)
        pool.close()
        return df.select([F.col(col).alias(col.lower()) for col in df.columns])
    
    def extract_metadata_tables(self):
        """
        This method efficiently processes and stores metadata from source databases into Delta tables, ensuring schema consistency and data integrity. 
        The actual "output" is the transformed and appended data in the Delta tables.
        """
        for sys_tbl in self.system_tables: #O/P: SystemTableSpec(table_name='dba_tab_columns', table_filter_col='table_name', schema_filter_col='owner', schema_name=None,
            #  select_cols='*', table_prefix=None)
            for db_conf in self.meta_conf.src_dbs:
                meta_df = self.__query_sys_table(sys_tbl, db_conf, self.meta_conf.table_names) #runs queries in the db containing rows from dba_tab_columns where table_name matches 
                # any of the source tables and owner matches any of the specified schemas, with all column names in lowercase. 
                meta_df = (meta_df.select(
                    F.lit(self.job_ctx.app).alias("app"),
                    F.lit(db_conf.database_alias).alias("src_db"),
                    F.col('*'),
                    F.lit(self.current_ts).alias("created_at"))
                )
        
                meta_table = self._resolve_meta_table_on_delta(sys_tbl.table_name, sys_tbl.table_prefix) # o/p: "ssot_platform_meta_fit.__oracle_dba_tab_columns"
                self.__update_meta_table_schema(meta_df.schema, meta_table) #If the schemas differ, the method will overwrite meta_table with this merged schema.It converts
                # the table metadata to string and does the comparision
                if self.delta_table_exists(meta_table):
                    meta_df = cast_with_cdc_schema(meta_df,
                                                   cdc_schema_ddl=struct_to_ddl(spark.table(meta_table).schema)) #cast_with_cdc_schema ensures the DataFrame's schema matches 
                    # the desired schema by adding missing columns, dropping extra columns, and casting columns to the correct types.
                (
                    meta_df
                    .write
                    .saveAsTable(
                        meta_table,
                        partitionBy="app",
                        mode="append",
                        format="delta",
                        mergeSchema = True
                    )
                )

    @staticmethod
    def get_latest_df(df: DataFrame):
        df.cache()
        max_created_at = df.agg(F.max("created_at")).collect()[0][0]
        return df.where(f"created_at = '{max_created_at}'")
    
    def _get_latest_meta_df(self, sys_tbl_name: str, table_prefix: str = None):
        meta_df = (spark.table(self._resolve_meta_table_on_delta(sys_tbl_name, table_prefix))
                   .where(f"app = '{self.job_ctx.app}' "))
        return self.get_latest_df(meta_df)
    
    @abstractmethod
    def get_column_spec_df(self) -> DataFrame:
        pass

    @abstractmethod
    def get_primary_keys_df(self) -> DataFrame:
        pass

    @abstractmethod
    def get_unique_keys_df(self) -> DataFrame:
        pass

    @abstractmethod
    def get_unique_indexes_df(self) -> DataFrame:
        pass

    @staticmethod
    @abstractmethod
    def is_lob_type(column_type, column_length) -> bool:
        pass

    @staticmethod
    @abstractmethod
    def get_max_lob_size_column_function(lob_column_name: str, lob_column_type: str) -> str:
        pass

    def get_key_spec_df(self) -> DataFrame:
        pass

    def __replace_db_schema_with_id(self, df: DataFrame) -> DataFrame:
        df.cache()
        cnt_before_join = df.count()
        db_schema_join_cols = ['app','src_db','src_schema']
        cols_to_select = set(df.schema.fieldNames()).difference(db_schema_join_cols)
        cols_to_select = [f"m.{col}" for col in cols_to_select]

        joined_df = (
            df.alias("m")
            .join(self.db_schema_spec_df.alias("d"), on = db_schema_join_cols, how="inner")
            .select(
                "m.app",
                F.col("d.id").alias("src_schema_id"),
                *cols_to_select
            )
        ).cache()
        cnt_after_join = joined_df.count()
        if cnt_before_join == cnt_after_join:
            return joined_df
        else:
            raise ValueError("something went wrong while looking up schema_id")
        
    def __populate_db_schema_spec(self, col_spec_df: DataFrame) -> DataFrame:
        db_schema_spec_cols = ['app','src_db','src_schema']
        db_schema_spec_df = col_spec_df.select(*db_schema_spec_cols).distinct().withColumn("id", F.expr("uuid()"))
        (
            DeltaTable.forName(spark, self.db_schema_spec_tbl).alias("m")
            .merge(db_schema_spec_df.alias("u"),
                   f"""m.app = u.app AND
                       m.src_db = u.src_db AND
                       m.src_schema = u.src_schema AND
                       m.app = '{self.job_ctx.app}'""")
            .whenNotMatchedInsert(values={
                "id" : "u.id",
                "app": "u.app",
                "src_db": "u.src_db",
                "src_schema" : "u.src_schema",
                "created_by" : F.current_timestamp(),
                "updated_at" : F.current_timestamp(),
                "created_by" : F.lit(MetadataProvider.NAME),
                "updated_by" : F.lit(MetadataProvider.NAME)
            }).execute()
        )
        return (
            spark.table(self.db_schema_spec_tbl)
            .where(f"app = '{self.job_ctx.app}'")
        )
    
    def __populate_column_spec(self, preserve_case: bool):
        col_spec_df = self.get_column_spec_df()  #gets columns from dba_tab_columns. it is implemented in oracle.py
        self.db_schema_spec_df = self.__populate_db_schema_spec(col_spec_df)
        col_spec_df = (
            self.__replace_db_schema_with_id(col_spec_df)
            .withColumn("is_lob_type",
                        F.udf(self.is_lob_type, returnType=BooleanType())("column_type", "column_length"))
        )
        col_spec_df = (
            col_spec_df
            .withColumn("created_at", F.lit(self.current_ts))
            .withColumn("created_by", F.lit(MetadataProvider.NAME))
        ).cache()
        col_spec_df = col_spec_df.withColumn("column_name",
                                             F.udf(lambda col: sanitize_column_name(col, preserve_case))("column_name"))
        (
            col_spec_df
             .write
             .saveAsTable(
                    self.col_spec_tbl,
                    format = "delta",
                    mode = "append",
                    partitionBy = "app"
             )
        )
        return col_spec_df
    
    def __populate_key_spec(self, col_spec_df: DataFrame, preserve_case: bool):
        pk_df = self.__replace_db_schema_with_id(self.get_primary_keys_df())
        uk_df = self.__replace_db_schema_with_id(self.get_unique_keys_df())
        ui_df = self.__replace_db_schema_with_id(self.get_unique_indexes_df())
        join_cols =  ["app", "src_schema_id", "table_name"]
        all_cols_df = (
            col_spec_df
            .where("is_lob_type = false")
            .groupby("app", "src_schema_id", "table_name")
            .agg(
                F.transform(F.array_sort(F.collect_list(F.struct("column_position", "column_name"))),
                            lambda struct: struct['column_name']).alias("column_names")
            )
        )

        @F.udf(returnType=ArrayType(StringType()))
        def sanitize_column_name(col_names):
            if col_names is not None:
                return [sanitize_column_name(col, preserve_case) for col in col_names]
            else:
                return None
            
        key_spec_df = (
            all_cols_df.alias("all")
            .join(pk_df.alias("pk"), join_cols, "left_outer")
            .join(uk_df.alias("uk"), join_cols, "left_outer")
            .join(ui_df.alias("ui"), join_cols, "left_outer")
            .select (
                "all.app",
                "all.src_schema_id",
                "all.table_name",
                sanitize_column_name(F.col("pk.column_names")).alias("primary_key_cols"),
                sanitize_column_name(F.col("uk.column_names")).alias("unique_key_cols"),
                sanitize_column_name(F.col("ui.column_names")).alias("unique_index_cols"),
                sanitize_column_name(F.col("all.column_names")).alias("all_cols")
            )
            .withColumn("cdc_keys", F.coalesce("primary_key_cols", "unique_key_cols", "unique_index_cols", "all_cols"))
            .withColumn("cdc_keys_type",F.expr("""
                        CASE 
                            WHEN primary_key_cols IS NOT NULL THEN 'PRIMARY_KEY'
                            WHEN unique_key_cols IS NOT NULL THEN 'UNIQUE_KEY'
                            WHEN unique_index_cols IS NOT NULL THEN 'UNIQUE_INDEX'
                        ELSE 'ALL_COLS"
                        END AS cdc_keys_type
            """))
            .withColumn("created_at", F.current_timestamp())
            .withColumn("created_by", F.lit(MetadataProvider.NAME))
        ).cache()
        (
            key_spec_df
            .write
            .saveAsTable(
                self.key_spec_tbl, format="delta",
                mode = "append", partitionBy="app"
            )
        )
        return key_spec_df
    
    def __resolve_table_schemas(self, col_spec_df: DataFrame):
        df = (col_spec_df.withColumn("column_def", F.concat_ws(" ","column_name", "column_delta_type"))
              .groupBy("app", "src_schema_id", "table_name")
              .agg(
                  F.concat_ws(",",
                              F.transform(F.array_sort(F.collect_list(F.struct("column_position", "column_def"))),
                                          lambda struct: struct["column_def"])).alias("table_ddl"),
                              F.collect_list(
                                  F.when(F.col("is_lob_type") == True, F.col("column_name"))
                              ).alias("table_lobs")
              ).groupBy("table_name")
              .agg(
                  F.collect_list("table_ddl").alias("table_ddls"),
                  F.collect_list("table_lobs").alias("table_lobs_list")
              ).collect())
        
        tbl_schema_lobs = []
        for tbl, ddls, lobs_list in df:
            merged_ddl = functools.reduce(
                lambda ddl1, ddl2 : merge_schema(
                    struct_from_ddl(ddl1),
                    struct_from_ddl(ddl2)
                ).schema_to_ddl(), ddls
            )
            merged_lobs = functools.reduce(lambda lobs1, lobs2: set(lobs1).union(lobs2), lobs_list)
            tbl_schema_lobs.append((tbl, merged_ddl, list(merged_lobs)))
        
        return spark.createDataFrame(tbl_schema_lobs,
                                     schema="table_name STRING, cdc_schema STRING cdc_lob_columns ARRAY<STRING>")
    
    def __resolve_table_keys(self, key_spec_df: DataFrame):
        key_spec_grouped = (
            key_spec_df
            .groupBy("table_name")
            .agg(F.collect_set("cdc_keys").alias("cdc_keys_list"))
            .collect()
        )
        error_tables = []
        table_vs_keys = []
        for key_spec_df in key_spec_grouped:
            table = key_spec_df["table_name"]
            keys_list = key_spec_df["cdc_keys_list"]
            key_list_sorted: typing.Set[str] = set()
            for keys in keys_list:
                key_list_sorted.add(",".join(sorted(keys)))
            
            if len(key_list_sorted) == 1:
                table_vs_keys.append(table, keys_list[0])
            else:
                error_tables.append(table)

        if len(error_tables) > 0:
            raise ValueError(f"Unable to resolve key columns for tables {','.join(error_tables)}."
                             f"More tahn 1 combination of cdc_keys found for these tables. "
                             f"Query {self.key_spec_tbl} to find the conflicting keys for these tables")
        
        return spark.createDataFrame(table_vs_keys, ['table_name', 'cdc_keys'])
    
    def populate_metadata(self) -> None:
        self.extract_metadata_tables()  #stores app, src_db, created_at along with columns fetched from dba_tab_columns, dba_constraints etc (method efficiently processes and stores 
        # metadata from source databases into Delta tables, ensuring schema consistency and data integrity. The actual "output" is the transformed and appended data in 
        # the Delta tables.)
        col_spec_df = self.__populate_column_spec(self.meta_conf.preserve_case) #gets cols from dba_tab_columns.
        tbl_schemas_df = self.__resolve_table_schemas(col_spec_df)
        tbl_schemas_df.cache()
        key_spec_df = self.__populate_key_spec(col_spec_df, self.meta_conf.preserve_case)
        tbl_keys_df = self.__resolve_table_keys(key_spec_df)
        tbl_keys_df.cache()

        if tbl_schemas_df.count() != tbl_keys_df.count():
            raise ValueError(f"Number of tables after resolving cdc_schema and cdc_keys dont match")
        
        tbl_schema_keys_df = (
            tbl_schemas_df.alias("s")
            .join(tbl_keys_df.alias("k"), "table_name")
            .select(
                F.lower("s.table_name").alias("src_table"),
                "s.cdc_schema",
                "s.cdc_lob_columns",
                "k.cdc_keys"
            )
        )
        flow_spec_tbl = self.job_ctx.get_flow_spec_table()
        flow_spec_df = (
            tbl_schema_keys_df
            .withColumn("flow_id", F.concat_ws("_", F.lit(self.job_ctx.app), "src_table"))
            .withColumn("app", F.lit(self.job_ctx.app))
            .withColumn("cdc_schema_fingerprints", F.array().cast("array<long>"))
            .withColumn("delta_table", F.udf(FlowSpec.get_delta_table)("src_table"))
            .withColumn("delta_table_path", get_delta_table_path(
                F.lit(self.job_ctx.get_glue_catalog_id()),
                F.lit(self.meta_conf.delta_db),
                "delta_table",
                F.lit(self.meta_conf.delta_table_version)
            ))
            .withColumn("partition_cols", F.array([F.lit(col) for col in self.meta_conf.delta_partition_cols]))
            .withColumn("schema_refresh_done", F.lit(True))
            .withColumn("flow_grp_id", F.ceil(
                F.row_number().over(Window.orderBy("src_table")) /
                F.lit(self.meta_conf.flows_per_pipeline)
            ).cast("int"))
            .withColumn("flow_reader_opts", F.create_map().cast("map<string, string>"))
            .withColumn("flow_spark_conf", F.create_map().cast("map<string, string>"))
            .withColumn("inactive_schemas", F.array().cast("array<string>"))
            .withColumn("is_active", F.lit(True))
            .withColumn("created_at", F.lit(self.current_ts))
            .withColumn("updated_at", F.lit(self.current_ts))
            .withColumn("created_by", F.lit(MetadataProvider.NAME))
            .withColumn("updated_by", F.lit(MetadataProvider.NAME))
            .withColumn("target_details", F.struct("delta_table", "delta_table_path",
                                                   "partition_cols", "schema_refresh_done"))
            .select(spark.table(flow_spec_tbl).columns)                                                   
        )
        flow_spec_df = spark.createDataFrame(flow_spec_df.rdd,
                                             schema=spark.table(flow_spec_tbl).schema)
        
        (
            DeltaTable.forName(spark, flow_spec_tbl).alias("m")
            .merge(flow_spec_df.alias("u"), f"m.flow_id = u.flow_id AND m.app = '{self.job_ctx.app}'")
            .whenNotMatchedInsertAll()
            .whenMatchedUpdate(set = {
                "m.cdc_lob_columns" : "u.cdc_lob_columns",
                "m.updated_at" : F.lit(self.current_ts),
                "m.updated_by" : F.lit(MetadataProvider.NAME)
            }).execute()
        )

        app_spec_tbl = self.job_ctx.get_app_spec_table()
        tenancy_conf = self.meta_conf.tenancy_conf
        app_spec_df = spark.createDataFrame([
            (
                self.job_ctx.app,
                self.meta_conf.src_db_type,
                self.meta_conf.delta_db,
                self.job_ctx.get_kafka_config(),
                self.job_ctx.get_schema_topic_name(),
                [self.job_ctx.get_cdc_topic_name()],
                {"preserve_case" : self.meta_conf.preserve_case},
                BRONZE_SPARK_CONF,
                SILVER_SPARK_CONF,
                BRONZE_TABLE_PROPS,
                SILVER_TABLE_PROPS,
                (tenancy_conf.tenancy_type, tenancy_conf.client_id_col, tenancy_conf.handle_l2l),
                self.current_ts,
                self.current_ts,
                MetadataProvider.NAME,
                MetadataProvider.NAME,
            )], schema = spark.table(app_spec_tbl).schema
        )
        (
            DeltaTable.forName(spark, app_spec_tbl).alias("m")
            .merge(app_spec_df.alias("u"), f"m.app = u.app AND m.app = '{self.job_ctx.app}'")
            .whenNotMatchedInsertAll()
            .execute()
        )