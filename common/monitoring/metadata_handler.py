import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import pyspark.sql.functions as F 
from pyspark.sql.window import window
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, StructType, StructField, BooleanType, IntegerType

from common.logger import get_logger
from common.context import JobContext
from common.metadata.provider import MetadataProvider

spark = SparkSession.getActiveSession()

client_id_table_schema = StructType(
    [
        StructField("table_name", StringType(), True),
        StructField("has_client_id", BooleanType(), True),
        StructField("client_id_col", StringType(), True)
    ]
)


@F.udf
def get_src_schema_table(schema: str, table: str):
    return (f"{schema}.{table}")


@F.udf
def get_sor_query(table: str, function: str, db_type: str, has_client_id: bool = None, client_id_col: bool = None, cdc_keys: list = None, num_records: int = None, src_condition: str = None):
    if function == "count":
        if client_id_col is None:
            # <degree_of_parallelism> will be replaced with an actual number before the query is fired
            if cdc_keys == None:
                # has primart/unique key
                query = f"""select /*+ parallel(<degree_of_parallelism>) */ count(*) src_count from {table.replace('__DOL','$')}"""
                if src_condition != None:
                    query = f"""select /*+ parallel(<degree_of_parallelism>) */ count(*) src_count from {table.replace('__DOL','$')} where {src_condition} tmp"""
            else:
                pk_cols = ", ".join(cdc_keys)
                if db_type == "sql_server":
                    if src_condition == None:
                        query = f"""select /*+ parallel(<degree_of_parallelism>) */ count(*) src_count from (select distinct {pk_cols} from {table.replace('__DOL', '$')}) tmp"""
                    else:
                        query = f"""select /*+ parallel(<degree_of_parallelism>) */ count(*) src_count from (select distinct {pk_cols} from {table.replace('__DOL', '$')} where {src_condition}) tmp"""
                else:
                    if src_condition == None:
                        query = f"""select /*+ parallel(<degree_of_parallelism>) */ count(*) src_count from (select distinct {pk_cols} from {table.replace('__DOL', '$')} group by {pk_cols}) tmp"""
                    else:
                        query = f"""select /*+ parallel(<degree_of_parallelism>) */ count(*) src_count from (select distinct {pk_cols} from {table.replace('__DOL', '$')} where {src_condition} group by {pk_cols}) tmp"""
            return query
        else:
            #l2l condition for tables that have the client id column 
            if has_client_id is True:
                client_id_col_alias = client_id_col
                table_name = table.split(".")[-1].lower()
                if table_name == "portal_partner_entitlements__dol":
                    client_id_col = "partner_oid"
                if cdc_keys == None:
                    #has primary/unique key
                    if src_condition == None:
                        query = f"""select /*+ parallel(<degree_of_parallelism>) */ count(*) src_count, {client_id_col} as {client_id_col_alias} from {table.replace('__DOL', '$')} group by {client_id_col}"""
                    else:
                        query = f"""select /*+ parallel(<degree_of_parallelism>) */ count(*) src_count, {client_id_col} as {client_id_col_alias} from {table.replace('__DOL', '$')} where {src_condition} group by {client_id_col}"""
                else:
                    pk_cols = ", ".join(cdc_keys)
                    if db_type == "sql_server":
                        if src_condition == None:
                            query = f"""select /*+ parallel(<degree_of_parallelism>) */ count(*) src_count {client_id_col} as {client_id_col_alias} from (select distinct {pk_cols}, {client_id_col} from {table.replace('__DOL', '$')}) tmp group by {client_id_col}"""
                        else:
                            query = f"""select /*+ parallel(<degree_of_parallelism>) */ count(*) src_count {client_id_col} as {client_id_col_alias} from (select distinct {pk_cols}, {client_id_col} from {table.replace('__DOL', '$')}) where {src_condition} tmp group by {client_id_col}"""
                    else:
                        if src_condition == None:
                            query = f"""select /*+ parallel(<degree_of_parallelism>) */ count(*) src_count from (select distinct {pk_cols} from (select 1, {client_id_col} from {table.replace('__DOL', '$')} group by {pk_cols},{client_id_col}) tmp group by {client_id_col}"""
                        else:
                            query = f"""select /*+ parallel(<degree_of_parallelism>) */ count(*) src_count from (select distinct {pk_cols} from (select 1, {client_id_col} from {table.replace('__DOL', '$')} where {src_condition} group by {pk_cols},{client_id_col}) tmp group by {client_id_col}"""
                return query

            else:
                #adding a dummy value for the client id column to match the schema of the results
                if cdc_keys == None:
                    # has primary/unique key
                    if src_condition == None:
                        query = f"""select /*+ parallel(<degree_of_parallelism>) */ (select count(*) from {table.replace('__DOL', '$')}) as src_count, 'datawatch_dummy_client_id' as {client_id_col}"""
                    else:
                        query = f"""select /*+ parallel(<degree_of_parallelism>) */ (select count(*) from {table.replace('__DOL', '$')}) where {src_condition} as src_count, 'datawatch_dummy_client_id' as {client_id_col}"""
                else:
                    pk_cols = ", ".join(cdc_keys)    
                    if db_type == "sql_server":
                        if src_condition == None:
                            query = f"""select /*+ parallel(<degree_of_parallelism>) */ count(*) src_count, 'datawatch_dummy_client_id' as {client_id_col} from (select distinct {pk_cols} from {table.replace('__DOL', '$')}) tmp) """
                        else:
                            query = f"""select /*+ parallel(<degree_of_parallelism>) */ count(*) src_count, 'datawatch_dummy_client_id' as {client_id_col} from (select distinct {pk_cols} from {table.replace('__DOL', '$')} where {src_condition}) tmp) """
                    else:
                        if src_condition == None:
                            query = f"""select /*+ parallel(<degree_of_parallelism>) */ (select count(*) from (select 1 from {table.replace('__DOL', '$')} group by {pk_cols}) tmp)as src_count, 'datawatch_dummy_client_id' as {client_id_col}"""
                        else:
                            query = f"""select /*+ parallel(<degree_of_parallelism>) */ (select count(*) from (select 1 from {table.replace('__DOL', '$')} where {src_condition} group by {pk_cols}) tmp)as src_count, 'datawatch_dummy_client_id' as {client_id_col}"""
                
                if db_type == "oracle":
                    query += " from dual"
                
                return query
                
    elif function == "data":
        if db_type == "oracle":
            query = f"select /*+ parallel(<degree_of_parallelism>) */ * from (select * from {table} order by dbms_random.value) where rownum < ({num_records}+1)"

        elif db_type == "postgres":
            query = f"select /*+ parallel(<degree_of_parallelism>) */ * from {table} order by random() limit {num_records}"

        elif db_type == "mysql":
            query = f"select /*+ parallel(<degree_of_parallelism>) */ * from {table} order by rand() limit {num_records}"

        elif db_type == "sqlserver":
            query = f"select /*+ parallel(<degree_of_parallelism>) */ top {num_records} * from {table} order by newid()"

        return query
    

def get_sor_db_table_map(sor_table_collection: list):
    #takes a spark collection as input

    # will have the following structure
    # {db1: [tables], db2: [tables]}
    sor_db_table_map = {}
    for row in sor_table_collection:
        table = row.asDict()

        # If entry for that db already exists, append to the table list
        # if not, create an emapty list and add the table
        if table['src_db'] in sor_db_table_map:
            sor_db_table_map[table['src_db']].append(table)
        else:
            sor_db_table_map[table['src_db']] = [table]
        
    return sor_db_table_map


def get_delta_table_list(table_map_df: DataFrame):
    #takes spark df as input
    delta_table_df = table_map_df.sselect(F.col("src_table")).dropDuplicates()
    delta_table_collection = delta_table_df.collect()

    delta_tables = []

    for row in delta_table_collection:
        delta_tables.append(row[0].lower())

    return delta_tables


def get_priority_tables(meta_db: str, app: str, function: str, ingestion_mode: str, table_map_df: DataFrame, db_schema_spec_df: DataFrame = None):
    table_map_df = table_map_df.withColumn("db_schema_table", F.concat(F.col("src_db"), F.lit("."), F.col("src_schema"),F.lit("."), F.col("src_table")))

    priority_table_df = spark.table(f"{meta_db}.datawatch_priority_spec")\
        .filter(F.col("app") == F.lit(app)\
        .filter(F.col("active_flag") == "y")\
        .filter(F.array_contains(F.col("operations"), "count"))\
        .filter(F.lower(F.col("priority")) != "exclude"))
    
    priority_table_df = priority_table_df.select(
        F.col("app").alias("priority_app"),
        F.col("schema_id"),
        F.col("src_table").alias("priority_table"),
        F.col("priority")
    )

    if ingestion_mode == "dataflow":
        enriched_priority_table_df = priority_table_df.join(F.broadcast(db_schema_spec_df), priority_table_df['schema_id'] == db_schema_spec_df["id"], "left")\
        .withColumn("priority_db_schema_table", F.concat(F.col("src_db"), F.lit("."), F.col("src_schema"), lit("."), F.col("priority_table")))\
        .drop("id", "src_schema_id","schema_id", "src_schema", "src_db")

    else:
        enriched_priority_table_df = priority_table_df.withColumn("priority_db_schema_table", F.concat(F.col("schema_id"), F.lit("."), F.col("priority_table")))

    priority_table_map_df = table_map_df.join(enriched_priority_table_df, F.lower(enriched_priority_table_df['priority_db_schema_table']) == F.lower(table_map_df["db_schema_table"]), "inner")

    priority_table_map_df.cache()

    if priority_table_map_df.count() == 0:
        raise Exception(f"No priority tables for app = {app} exist in {meta_db}.datawatch_priority_spec")
    
    return priority_table_map_df


def get_ingestion_control_config(app: str, meta_db: str) -> DataFrame:
    ingestion_control_config_df = spark.table(f"{meta_db}.ingestion_control_config")\
                                       .filter(F.lower(F.col("src_app")) == F.lit(app))\
                                       .select("client_id_column", "sgdp_org_id_enabled")
    
    return ingestion_control_config_df


def check_l2l_status(ingestion_mode: str, job_context: JobContext, meta_db: str):
    # returns client id if l2l is enabled for the sor
    # else retuns null
    if ingestion_mode == "s3":
        ingestion_control_config_df = get_ingestion_control_config(job_context.app, meta_db)
        ingestion_control_config = ingestion_control_config_df.collect()[0]
        client_id_column = ingestion_control_config["client_id_column"]
        is_sgdp_org_id_enabled = ingestion_control_config["sgdp_org_id_enabled"]

        if is_sgdp_org_id_enabled and client_id_column is not None:
            # l2l enabled
            return client_id_column
        else:
            # no l2l
            return None
        
    if ingestion_mode == "dataflow":
        if job_context.get_app_spec().tenancy_conf.handle_l2l:
            return job_context.get_app_spec().tenancy_conf.client_id_col
        else:
            return None
        
    
def get_client_id_table_map(client_id_table_map):
    delta_db = client_id_table_map['delta_db']
    table_name = client_id_table_map['table_name']
    client_id_col = client_id_table_map['client_id_col']

    try:
        client_id_table_map = {"table_name" : table_name, "has_client_id": False}
        table_columns = spark.table(f"{delta_db}.{table_name.replace('$','__DOL')}").schema.names

        if client_id_col.lower() in table_columns or client_id_col.upper() in table_columns:
            client_id_table_map["has_client_id"] = True
            client_id_table_map["client_id_col"] = client_id_col

        return client_id_table_map
    except:
        return None
    
def get_client_id_table_df(delta_db: str, table_collection: list, client_id_col: str):
    client_id_table_input = [{'delta_db': delta_db, 'table_name': table[0], 'client_id_col': client_id_col} for table in table_collection]
    with ThreadPoolExecutor(max_workers=len(table_collection)) as executor:
        results = executor.map(get_client_id_table_map, client_id_table_input)

    client_id_table_maps = list(results)

    filtered_client_id_table_maps = [result for result in client_id_table_maps if result is not None]

    client_id_df = spark.createDataFrame(filtered_client_id_table_maps, client_id_table_schema)

    return client_id_df


def get_sor_delta_tables(meta_db: str, function: str, ingestion_mode: str, run_mode: str, job_context: JobContext, db_type: str, client_id_col: str = None, num_records: int = None):
    # takes meta_db name, app name and the function that is to be performed -> count
    # retuns a tuple consisting Of 
    # a dictionary with sor db name as the key and the list of tables in it as the value 
    # the list of tables in the delta db of the app 
    app = job_context.app

    if ingestion_mode == "dataflow":

        db_schema_spec_df = (
            spark.read.table(f"{meta_db}.__db_schema_spec")
            .filter(F.lower(F.col("app")) == F.lit(app))
            .select(F.col("id"), F.col("src_db"), F.col("src_schema"), F.col("app"))
        )

        # needed as db_schema_spec_df does not have table_name
        column_spec = spark.read.table(f"{meta_db}.__column_spec").filter(
            F.lower(F.col("app") == F.lit(app))
        )

        column_spec_df = (
            MetadataProvider.get_latest_df(column_spec)
            .select(F.col("src_schema_id"), F.col("table_name").alias("src_table"))
            .dropDuplicates()
        )

        table_map_df = (
            column_spec_df.join(
                F.broadcast(db_schema_spec_df),
                column_spec_df["src_schema_id"] == db_schema_spec_df["id"],
                "left"
            )
            .drop("id")
            .withColumn(
                "id",
                F.concat(
                    F.col("app"),
                    F.lit("."),
                    F.col("src_db"),
                    F.lit("."),
                    F.col("src_schema"),
                    F.lit("."),
                    F.col("src_table"),
                ),
            )
        )

        # some tables in the sor does not have primary keys or unique keys
        # a combination of columns are used to enforce uniqueness and dedupe in delta
        # to get the count of unique records fetching those keys from key_spec and flow_spec
        key_spec = spark.read.table(f"{meta_db}.__key_spec").filter(
            (F.col("app") == app) & (F.upper((F.col("cdc_keys_type"))) == "ALL_COLS")
        )

        cdc_keys_df = (
            MetadataProvider.get_latest_df(key_spec)
            .select("table_name", F.col("src_schema_id").alias("schema_id"), "cdc_keys")
            .dropDuplicates()
        )

        flow_spec_df = (
            spark.read.table(f"{meta_db}.flow_spec")
            .filter((F.lower(F.col("is_active")) == "true") & (F.lower(F.col("app")) == app))
            .select(F.col("src_table"), F.col("inactive_schemas"))
        )

        flow_spec_df.cache()

        # filtering out inactive tables and inactive schemas
        table_map_df = (
            table_map_df.alias("tbl_map")
            .join(
                F.broadcast(flow_spec_df.alias("flw_spc")),
                F.lower(table_map_df["src_table"]) == F.lower(flow_spec_df["src_table"])
            )
            .filter(~F.array_contains(F.col("flw_spc.inactive_schemas"), F.col("tbl_map.src_schema_id")))
            .select("tbl_map.*")
        )

        custom_query_config_df = spark.sql(f"""select *, concat_ws('.', app, src_db, src_schema, src_table) as id from {meta_db}.datawatch_custom_query_config where app = '{app}'""")

        tbl_map_df = (table_map_df.alias("tbl_map").join(F.broadcast(custom_query_config_df.alias("custom_query_config")),
                                                         F.lower(table_map_df["id"]) == F.lower(custom_query_config_df["id"]), "left"))
        
        tbl_map_df = tbl_map_df.select("tbl_map.*", F.col("src_condition"))

        if run_mode == "priority_tables":
            get_logger(__name__).info(f"getting priority tables")

            tbl_map_df = get_priority_tables(meta_db, app, function, ingestion_mode, tbl_map_df, db_schema_spec_df)
        
        else:
            get_logger(__name__).info(f"getting all tables")

        if function == "count":

            if client_id_col is not None:
                # sor has l2l 
                # there may be tables without the client id column
                # identifying such tables
                table_collection = flow_spec_df.select("src_table").distinct().collect()
                delta_db = job_context.get_app_spec().delta_db
                client_id_df = get_client_id_table_df(delta_db, table_collection, client_id_col)

                tbl_map_df = (tbl_map_df.alias("table_map").join(F.broadcast(client_id_df).alias("clnt_id_df"),
                F.lower(client_id_df['table_name']) == F.lower(tbl_map_df["src_table"]), "left")
                .select("table_map.*", "has_client_id"))

            else:
                tbl_map_df = tbl_map_df.withColumn("has_client_id", F.lit(False))

            enriched_table_map_df = (
                tbl_map_df.join(
                    F.broadcast(cdc_keys_df),
                    (cdc_keys_df["table_name"] == tbl_map_df["src_table"]) &
                    (cdc_keys_df["schema_id"] == tbl_map_df["src_schema_id"]),
                    "left",
                )
                .drop("schema_id", "src_schema_id")
                .withColumn("src_schema_table", get_src_schema_table(F.col("src_schema"), F.col("src_table")),)
                .withColumn("query",
                get_sor_query(F.col("src_schema_table"), F.lit(function), F.lit(db_type), F.col("has_client_id"),
                F.lit(client_id_col), F.col("cdc_keys"), F.lit(None), F.col("src_condition")
                ),
                ).withColumn("client_id_col", F.lit(client_id_col))).dropDuplicates()
            


            app_table_df = enriched_table_map_df.select(
                        F.col("id"),
                        F.lower(F.col("src_db")).alias("src_db"),
                        F.col("src_schema"),
                        F.col("src_table"),
                        F.col("src_schema_table"),
                        F.col("query"),
                        F.col("client_id_col"),
                        F.col("has_client_id"),
            )
        
        elif function == "data":
            enriched_table_map_df = (
                tbl_map_df.join(
                    F.broadcast(cdc_keys_df),
                    (cdc_keys_df["table_name"] == tbl_map_df["src_table"]) &
                    (cdc_keys_df["schema_id"] == tbl_map_df["src_schema_id"]),
                    "left"
                )
                .drop("schema_id","src_schema_id")
                .withColumn("src_schema_table", get_src_schema_table(F.col("src_schema"), F.col("src_table")),)
                .withColumn("query",
                get_sor_query(F.col("src_schema_table"), F.lit(function), F.lit(db_type), F.lit(None), F.lit(None), F.lit(None), F.lit(num_records), F.lit(None))
                )
            ).dropDuplicates()


            app_table_df = enriched_table_map_df.select(
                        F.col("id"),
                        F.lower(F.col("src_db")).alias("src_db"),
                        F.col("src_schema"),
                        F.col("src_table"),
                        F.col("src_schema_table"),
                        F.col("query"),
            )

    else:
        tbl_map_df = spark.read.table(f"{meta_db}.table_map")\
            .filter(F.lower(F.col("src_app")) == F.lit(app))\
            .filter(F.lower(F.col("active_flag")) == "y")
        
        # in s3 ingested tables the schema_id column is schema name 
        # there are multiple db's which have schemas with the same name
        # ignoring these schemas as there is no way of knowing which db a row in the delta table came from

        client_id = check_custom_client_id(app, meta_db)
        if not (client_id or client_id_col):
            non_unique_schema_df = tbl_map_df.groupBy("src_schema").agg(F.countDistinct("src_db").alias("num_dbs"))\
                                            .filter(F.col("num_dbs") > 1)\
                                            .select(F.col("src_schema"))
            
            non_unique_schemas = [row.asDict()['src_schema'] for row in non_unique_schema_df.collect()]

            tbl_map_df = tbl_map_df.filter(~F.col("src_schema").isin(non_unique_schemas))

            tbl_map_df = tbl_map_df.dropDuplicates()

        if run_mode == "priority_tables":
            get_logger(__name__).info("getting priority tables")
            tbl_map_df = get_priority_tables(meta_db, app, function, ingestion_mode, tbl_map_df)

        else:
            get_logger(__name__).info("getting all tables")            

        if function == "count":
            custom_query_config_df = spark.sql(f"""select *, concat_ws('.', app, src_sb, src_schema, src_table) as id from {meta_db}.datawatch_custom_query_config where app = '{app}'""")

            tbl_map_df = (tbl_map_df.alias("tbl_map").join(F.broadcast(custom_query_config_df.alias("custom_query_config")),
                                                           F.lower(tbl_map_df['id']) == F.lower(custom_query_config_df['id']), "left"))
            
            tbl_map_df = tbl_map_df.select("tbl_map.*", F.col("src_condition"))
            if client_id_col is not None:
                tbl_map_df.cache()

                tables = tbl_map_df.select("src_condition").distinct.collect()
                delta_db = tbl_map_df.select("del_db").limit(1).collect()[0][0]            
                client_id_df = get_client_id_table_df(delta_db, tables, client_id_col)

                tbl_map_df = (tbl_map_df.alias("table_map").join(F.broadcast(client_id_df).alias("clnt_id_df"),
                                    client_id_df["table_name"] == tbl_map_df["src_table"],
                                    "left").select ("table_map.*", "has_client_id"))
                
            else:
                tbl_map_df = tbl_map_df.withColumn("has_client_id", F.lit(False))

            app_table_df = (
                tbl_map_df.select(
                    F.col("id"),
                    F.lower(F.col("src_db")).alias("src_db"),
                    F.col("src_schema"),
                    F.col("src_table"),
                    F.col("has_client_id"),
                    F.lit(client_id_col).alias("client_id_col"),
                    F.col("src_condition")
                )
                .withColumn("src_schema_table", get_src_schema_table(F.col("src_schema"), F.col("src_table")))
                .withColumn("query", get_sor_query(F.col("src_schema_table"), F.lit(function), F.lit(db_type),
                F.col("has_client_id"), F.lit(client_id_col), F.lit(None), F.lit(None), F.col("src_condition")))
            )

            app_table_df = app_table_df.select("id","src_db","src_schema","src_table", "has_client_id","client_id_col", "src_schema_table", "query")

        elif function == "data":
            app_table_df =  (
                tbl_map_df.select(
                    F.col("id"),
                    F.lower(F.col("src_db")).alias("src_db"),
                    F.col("src_schema"),
                    F.col("src_table"),
                )
                .withColumn("src_schema_table", get_src_schema_table(F.col("src_schema"), F.col("src_table")))
                .withColumn("query", get_sor_query(F.col("src_schema_table"), F.lit(function), F.lit(db_type), F.lit(None), F.lit(None), F.lit(None),F.lit(num_records), F.lit(None)))

            )

    app_table_df_rows = app_table_df.collect()    

    sor_db_table_map = get_sor_db_table_map(app_table_df_rows)

    delta_tables = get_delta_table_list(tbl_map_df)

    return (sor_db_table_map, delta_tables)

def check_custom_client_id(app: str, meta_db: str):
    custom_validation_spec_df = spark.table(f"{meta_db}.datawatch_custom_validation_spec").filter(F.col("app") == app)
    custom_validation_spec_df.cache()
    if not custom_validation_spec_df.isEmpty():
        delta_client_id_col = custom_validation_spec_df.select("delta_client_id_col").collect()[0][0]
        return delta_client_id_col
    else:
        return None
    
class DeltaDbMetadata:
    @dataclass
    class DeltaQueryConfig:
        table_name: str
        query: str
        group_by_column: str
        ingestion_mode: str
        app: str
        env: str
        meta_db: str
        delta_db: str
        purged_tables: str
        client_id_col: str

    def __init__(self, app: str, env: str, ingestion_mode: str, function: str, tables: list, meta_db: str,
                 job_context: JobContext, client_id_col: str = None):
        self.app = app
        self.env = env
        self.function = function.lower()
        self.tables = tables
        self.meta_db = meta_db
        self.job_context = job_context
        self.client_id_col = client_id_col

        if ingestion_mode.lower() not in ["s3", "dataflow"]:
            raise ValueError(f"Invalied ingestion_mode: {ingestion_mode} parameter. It must be eith 'dataflow' or 's3'")
        
        self.ingestion_mode = ingestion_mode.lower()
    

    def set_delta_db(self):
        if self.ingestion_mode == "s3":
            self.delta_db = spark.table(f"{self.meta_db}.table_map").filter(F.lower(F.col("src_app"))== self.app).select(F.col("del_db")).limit(1).collect()[0][0]
        else:
            self.delta_db = self.job_context.get_app_spec().delta_db

    def set_client_id_col_info(self):
        self.client_id_df_dict = {}
        #method to see if the table has client_id_col
        if self.client_id_col:
            tables_list = [(table, ) for table in self.tables]
            client_id_df = get_client_id_table_df(self.delta_db, tables_list, self.client_id_col)
            if client_id_df is not None:
                client_id_df_result = client_id_df.collect()
                self.client_id_df_dict = {row[0]: {'has_client_id': row[1], 'client_id_col': row[2]} for row in
                                          client_id_df_result}
    
    def set_purged_tables(self):
        self.purged_tables = set(spark.sql(f"""select lcase(purged_table)
                                           from {self.meta_db}.purged_tables
                                           where lower(app) = lower('{self.app}')""").rdd.flatMap(lambda x: x).collect())
        
    def set_group_by_column(self):
        if self.ingestion_mode == "dataflow":
            self.group_by_column = "__schema_id"
        else:
            custom_client_id_col = check_custom_client_id(self.app, self.meta_db)
            if custom_client_id_col:
                self.group_by_column = custom_client_id_col
            else:
                df = spark.read.table(f"{self.meta_db}.ingestion_control_config").filter(F.lower(F.col("src_app")) == self.app).select(F.col("sgdp_ord_id_enabled"))
                sgdp_ord_id_enabled = df.collect()[0]['sgdp_org_id_enabled']

                if sgdp_ord_id_enabled:
                    self.group_by_column = "sgdp_org_id"
                else:
                    self.group_by_column = "schema_id"
    
    def get_delta_query(self, table: str, group_by_column: str):
        if self.function == "count":
            custom_query_config_df = spark.sql(
                f"""select delta_condition from {self.meta_db}.datawatch_custom_query_config
                 where app = '{self.app}' and ingestion_mode = '{self.ingestion_mode}' 
                 and lcase(src_table) = '{table.lower()}'
                and src_db is null and src_schema is null and delta_condition is not null """)
            custom_query_config_df.cache()
            if custom_query_config_df.isEmpty() == False:
                delta_condition = custom_query_config_df.collect()[0][0]
            else:
                delta_condition = None

            if self.ingestion_mode == "dataflow":
                group_by_columns = group_by_column
                if self.app.lower() in ('portal-wfnpi','wfnpi'):
                    group_by_columns = group_by_column if group_by_column == self.group_by_column else f"{group_by_column}, {self.group_by_column}"
                if delta_condition == None:
                    return f"select sum(case when __DeleteVersion is null then 1 esle 0 end) as delta_count, "\
                           f"{group_by_columns}, "\
                           f"coalesce(max(__meta.op_ts), '1970-01-01T00:00:00.000+00:00) as max_op_ts, "\
                           f"current_timestamp() as query_exec_time " \
                           f"from {self.delta_db}.__apply_changes_storage_{table.replace('$', '__DOL')} group by {group_by_columns}"
                else:
                    return f"select sum(case when __DeleteVersion is null then 1 esle 0 end) as delta_count, "\
                           f"{group_by_columns}, "\
                           f"coalesce(max(__meta.op_ts), '1970-01-01T00:00:00.000+00:00) as max_op_ts, "\
                           f"current_timestamp() as query_exec_time " \
                           f"from {self.delta_db}.__apply_changes_storage_{table.replace('$', '__DOL')} where {delta_condition} group by {group_by_columns}"

            else:
                if delta_condition != None:
                    return f"""select count(*) as delta_count, {self.group_by_column}
                    from {self.delta_db}.{table.replace('$','__DOL')}
                    where {delta_condition} group by {self.group_by_column} """
                else:
                    return f"""select count(*) as delta_count, {self.group_by_column}
                    from {self.delta_db}.{table.replace('$','__DOL')}
                    group by {self.group_by_column} """
    

    def set_delta_query_config(self):
        query_config_list = []
        
        if self.ingestion_mode == "dataflow":
            for table in self.tables:
                if self.client_id_df_dict.get(table) and self.client_id_df_dict[table].get('has_client_id'):
                    group_by_column = self.client_id_col
                else:
                    group_by_column = self.group_by_column
                delta_query_config = self.DeltaQueryConfig(
                    table_name=table,
                    query=get_delta_query(table, group_by_column),
                    group_by_column=group_by_column,
                    ingestion_mode=self.ingestion_mode,
                    app=self.app,
                    env=self.env,
                    meta_db=self.meta_db,
                    delta_db=self.delta_db,
                    purged_tables=self.purged_tables,
                    client_id_col=self.client_id_col
                )
                query_config_list.append(delta_query_config)
        else:
            group_by_column = self.group_by_column
            for table in self.tables:
                delta_query_config = self.DeltaQueryConfig(
                    table_name=table,
                    query=get_delta_query(table, group_by_column),
                    group_by_column=group_by_column,
                    ingestion_mode=self.ingestion_mode,
                    app=self.app,
                    env=self.env,
                    meta_db=self.meta_db,
                    delta_db=self.delta_db,
                    purged_tables=self.purged_tables,
                    client_id_col=self.client_id_col
                )
                query_config_list.append(delta_query_config)

        self.query_config_list = query_config_list
    

    def get_query_config_list(self):
        return self.query_config_list
    

    def set_properties(self):
        self.set_group_by_column()
        self.set_delta_db()
        self.set_purged_tables()
        self.set_client_id_col_info()
        self.set_delta_query_config()

class JdbcUtil:
    jdbc_driver_dict = {
        "oracle" : "oralce.jdbc.driver.OracleDriver",
        "mysql" : "com.mysql.jdbc.driver",
        "postgres" : "org.postgressql.Driver",
        "sqlserver" : "com.microfost.sqlserver.jdbc.SQLServerDriver"
    }



    def get_jdbc_url(self, db_type, host, port, sid):
        if db_type == "oracle":
            return f"jdbc:oracle:thin:@{host}:{port}/{sid}"
        elif db_type == "mysql":
            return f"jdbc.mysql://{host}:{port}/{sid}"
        elif db_type == "postgres":
            return f"jdbc:postgresql://{host}:{port}/{sid}"
        elif db_type == "sqlserver":
            return f"jdbc:sqlserver://{host}:{port};databaseName={sid};encrypt=true;trustServerCertificate=true;"
        else:
            raise Exception("Unsupported db type")
        
    
    def get_jdbc_driver(self, db_type):
        try:
            return self.get_jdbc_driver[db_type]
        except:
            raise Exception("Unsupported db type")
    
        
def get_archive_data(meta_db):
    arc_df = (
        spark.sql(f"""select src_db, arc_schema, arc_table, vpd_key, cast(arc_count as long) as arc_count, created_at from {meta_db}.wfn_archive_counts
                  where arc_table like '%_ARCH_TAB' and error_msg is null """)
    )
    window_spec = Window.partitionBy('src_db','src_schema','arc_table','vpd_key') \
                    .orderBy(F.desc('created_at'))
    arc_df = arc_df.withColumn("r", F.row_number() over(window_spec))\
            .filter((F.col('r') == 1))
    arc_df.cache()
    arc_df.count()
    archived_tables = set(arc_df.withColumn("src_table"), F.regexp_replace(F.lower(F.col("arc_table")), '_arch_tab', '_tab').select("src_table").rdd.flatMap(lambda x: x).collect())
    return ((arc_df, archived_tables))


class SorMetadata():

    def __init__(self, app:str, db_type: str, jdbc_port: int, database_property_list: list, secret_mode: str,
                 db_table_map: dict, meta_db: str, num_parallel_connections: int, degree_of_parallelism: int,
                 max_timeout_seconds: int, max_timeout_seconds_incr: int, max_no_of_attempts: int,
                 job_context: JobContext, ingestion_mode: str):
        self.app = app
        self.db_type = db_type
        self.jdbc_port = jdbc_port
        self.database_property_list = database_property_list
        self.jdbc_util = JdbcUtil()
        self.db_table_map = db_table_map
        self.meta_db = meta_db
        self.max_timeout_seconds = max_timeout_seconds
        self.max_timeout_seconds_incr = max_timeout_seconds_incr
        self.max_no_of_attempts = max_no_of_attempts
        self.degree_of_parallelism = degree_of_parallelism
        self.job_context = job_context
        self.ingestion_mode = ingestion_mode

        if secret_mode is None:
            self.secret_mode = "multi"
        else:
            self.secret_mode = secret_mode

        if num_parallel_connections is None:
            self.num_parallel_connections = 50
        else:
            self.num_parallel_connections = num_parallel_connections

        # key -> db name value -> properties and table list of the db
        self.db_map = {}

    def get_db_connection_config(self, db):
        # returns a dictionary with properties needed to connect to the database
        db_connection_config = {
            'app' : self.app,
            'db_type' : self.db_type,
            'jdbc_port' : (self.jdbc_port),
            'secret_mode' : self.secret_mode,
            'attempt_num' : "1",
            'ingestion_mode' : self.ingestion_mode,
            'env' : self.job_context.env,
            'meta_db' : self.meta_db,
            'database' : db['database'],
            'host' : db['host'],
            'max_timeout_seconds' : self.max_timeout_seconds,
            'max_timeout_seconds_incr' : self.max_timeout_seconds_incr,
            'max_no_of_attempts' : self.max_no_of_attempts,
            'degree_of_parallelism' : self.degree_of_parallelism
        }

        try:
            db_connection_config['database_alias'] = db['database_alias']

        except KeyError:
            db_connection_config['database_alias'] = db['database']

        db_connection_config['driver'] = self.jdbc_util.get_jdbc_driver(self.db_type)
        db_connection_config['jdbc_url'] = self.jdbc_util.get_jdbc_url(db_type=self.db_type, host=db['host'],
                                                                       port=self.jdbc_port, sid=db['database'])
        
        username, password = self.job_context.resolve_secrets(db['database_alias'], self.secret_mode)
        db_connection_config['username'] = username
        db_connection_config['password'] = password
        if self.app == 'wfn':
            arc_data = get_archive_data(self.meta_db)
            db_connection_config['arc_df'] = arc_data[0]
            db_connection_config['archive_tables'] = arc_data[1]
        else:
            db_connection_config['arc_df'] = None
            db_connection_config['archive_tables'] = None

        return db_connection_config
    
    def get_db_list(self):
        _logger = get_logger(__name__)
        db_map = {}
        for db in self.database_property_list:
            db_name = db['database_alias']
            db_map['db_name'] = db
            db_map["db_name"]["db_connection_config"] = self.get_db_connection_config(db)

            try:
                db_map[db_name]['num_parallel_connections'] = db['num_parallel_connections']

            except KeyError:
                _logger.info(f"{db_name}: num_parallel_connections not found in config. Applying default value")
                db_map[db_name]['num_parallel_connections'] = self.num_parallel_connections

        for db in self.db_table_map:
            # Joining db metadata with table list of the db
            try:
                db_map[db]['tables'] = self.db_table_map[db]
            except:
                _logger.warning(f"Entry for database {db} not found in the sor config file")

        # The framework takes input as a list of dictionaries
        db_list = []
        for db in db_map:
            db_list.append(db_map[db])

        return db_list
    
class SorConfigReader:
    def __init__(self, app: str, ingestion_mode: str, env: str, sor_config_path: str):
        self.app = app
        self.ingestion_mode = ingestion_mode
        self.env = env

        if env in ["fit","dit", "dev"]:
            self.service_user_prefix = "dev"
        else:
            self.service_user_prefix = env
        
        if sor_config_path != "":
            self.sor_config_path = sor_config_path
        else:
            if ingestion_mode == "s3":
                self.sor_config_path = f"/Workspace/Repos/dc-dp-{self.service_user_prefix}/@XYZ.com/ssot_config/SORConfig/{self.env}/{self.app.upper()}_config.json"
            else:
                self.sor_config_path = f"/Workspace/Repos/dc-dp-{self.service_user_prefix}/@XYZ.com/ssot-dataflow-jobs/config/{self.env}/{self.app.lower()}_config.json"
            
        
    def read_sor_config(self):
        try:
            sor_config_file = open(self.sor_config_path, "r")
            sor_config_json = sor_config_file.read()
            sor_config = json.loads(sor_config_json)

            self.sor_config = sor_config

        except Exception as e:

            get_logger(__name__).error(f"exception trying to read sor config")
            raise e
        

    def standardise_database_property_list(self, database_property_list):
        standardised_database_property_list = []

        if self.ingestion_mode == "s3":

            for db in database_property_list:
                # print(db)
                # renaming fields from the s3 ssot_config to match the dataflow config files
                db['database'] = db.pop("dbservice").lower()
                db['database_alias'] = db.pop("secret").lower()
                db["schemas"] = db.pop("schemaList")

                standardised_database_property_list.append(db)
            
            return standardised_database_property_list
        
        else:

            for db in database_property_list:
                if db.get("database_alias") is not None:
                    db['database_alias'] = db.pop('database_alias').lower()
                else:
                    db['database_alias'] = db['database'].lower()

            return database_property_list
        
    
    def get_sor_config(self):
        self.read_sor_config()

        sor_config_formatted = dict()

        if self.ingestion_mode == "s3":

            db_type = self.sor_config.get("db_type")
            jdbc_port = self.sor_config.get("jdbcport")
            database_property_list = self.standardise_database_property_list(self.sor_config.get("databaseList"))
            secret_mode = self.sor_config.get("secret_mode")

        else:

            # dataflow config has different key names in the config file
            db_type = self.sor_config.get("src_db_type")
            jdbc_port = self.sor_config.get("src_dbs")[0]["port"]
            database_property_list = self.standardise_database_property_list(self.sor_config.get("src_dbs"))
            secret_mode = self.sor_config.get("secret_mode")
        
        sor_config_formatted['db_type'] = db_type
        sor_config_formatted['jdbc_port'] = jdbc_port
        sor_config_formatted['database_property_list'] = database_property_list
        sor_config_formatted['secret_mode'] = secret_mode

        return sor_config_formatted
