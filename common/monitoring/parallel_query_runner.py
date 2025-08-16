import numpy as np
import asyncio
import nest_asyncio
import time
import decimal
from datetime import datetime
from functools import partial
from concurrent.futures import ThreadPoolExecutor

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, LongType, BooleanType
from pyspark.sql import SparkSession, DataFrame

from common.logger import get_logger
from monitoring.metadata_handler import DeltaDbMetadata

spark = SparkSession.getActiveSession()

sor_table_details_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("src_db", StringType(), True),
        StructField("src_schema", StringType(), True),
        StructField("src_table", StringType(), True),
        StructField("is_error_src", IntegerType(), True),
        StructField("error_msg_src", StringType(), True),
        StructField("query_duration_seconds", StringType(), True),
        StructField("attempt_num", StringType(), True),
    ]
)

@F.udf(returnType=BooleanType())
def is_valid_client(sgdp_org_id: str, sgdp_client_id: str, temp_sgdp_org_id: str):
    if not sgdp_org_id:
        return False
    
    if sgdp_org_id.isdigit():
        # numeric org id's 
        if (sgdp_client_id and sgdp_client_id.upper() in ('NG_COMMON','TLRECRUITMENT','TOTALCOMPMASTER','TALENT')) or temp_sgdp_org_id is not None:
            # valid client - consider for count validation
            return True
        else:
            # invalid client. dont consider for count validation
            return False
    elif sgdp_org_id.isalnum():
        return True
    else:
        # raise ValueError("Unexpected sgdp_org_id: {sgdp_org_id}. Expecting only numeric or alphanumeric value")
        return True
    

class SorQueryUtil:

    def start_query_on_sor(self, function: str, num_parallel_db_connections: int, dbs: list):
        # takes in the list of dictionaries with metadata of all dbs in an sor
        # parallelizes running queries on each db 
        # number of parallel tasks will be equal to number of dbs
        with ThreadPoolExecutor(max_workers=num_parallel_db_connections) as executor:
            try:
                results = executor.map(partial(self.prepare_table_batches, function), dbs)
                db_results = list(results)
                return db_results
            except Exception as e:
                get_logger(__name__).error(f"Exception in start_query_on_sore: {e}")
                raise e
    

    def prepare_table_batches(self, function, db_info_map):
        # takes in the list of all tables in a db, splits it to batches based on num_parallel_connections 
        # passes the batch to the function that runs the query on the tables in the batch 
        # number of parallel connections allowed on a db
        num_parallel_connections = db_info_map['num_parallel_connections']
        tables = db_info_map['tables']
        #has jdbc connection info
        db_connnection_config = db_info_map['db_connection_config']

        table_config_tuples = []
        for table in tables:
            table_config_tuples.append((table, db_connnection_config, function))

        
        try:
            with ThreadPoolExecutor(max_workers=num_parallel_db_connections) as executor:
                results = executor.map(self.query_sor, table_config_tuples)
            
            return list(results)

        except Exception as e:
            get_logger(__name__).error(f"Exception in prepare_table_batches: {e}")
            raise e
    


    def query_sor(self, table_config_tuple):
        table = table_config_tuple[0]
        db_connection_config = table_config_tuple[1]
        function = table_config_tuple[2]
        driver = db_connection_config.get("driver")
        username = db_connection_config.get("username")
        password = db_connection_config.get("password")
        max_timeout_seconds = db_connection_config.get("max_timeout_seconds")
        max_timeout_seconds_incr = db_connection_config.get("max_timeout_seconds_incr")
        jdbc_url = db_connection_config.get("jdbc_url")
        attempt_num = int(db_connection_config.get("attempt_num"))
        max_no_of_attempts = db_connection_config.get("max_no_of_attempts")
        degree_of_parallelism = db_connection_config.get("degree_of_parallelism")
        query = table.get("query").replace(",degree_of_parallelism>", f"{degree_of_parallelism}")
        arc_df = db_connection_config.get("arc_df")
        archived_tables = db_connection_config.get("archived_tables")
        try:
            query_start_time = datetime.now()

            result_df = spark.read.formatt("jdbc")\
                                  .option("url",jdbc_url)\
                                  .option("driver",driver)\
                                  .option("oracle.jdbc.timezoneAsRegion","false")\
                                  .option("user",username)\
                                  .option("password",password)\
                                  .option("query",query)\
                                  .option("numPartitions",1)\
                                  .option("queryTimeout",max_timeout_seconds)\
                                  .load()
            
            if function == "data":
                result_df = result_df.withColumn("src_db",F.lit(table["src_db"]))\
                                     .withColumn("src_schema",F.lit(table["src_schema"]))\
                                     .withColumn("src_table",F.lit(table["src_table"]))
                query_end_time = datetime.now()
                query_duration_seconds = (query_end_time - query_start_time).total_seconds()

            elif function == "count":
                if "src_count" in result_df.schema.fieldNames() or "SRC_COUNT" in result_df.schema.fieldNames():
                    result_df = result_df.withColumn("src_count", F.col("src_count").cast(LongType()))
                query_end_time = datetime.now()
                query_duration_seconds = (query_end_time - query_start_time).total_seconds()

                if result_df.count() > 0:

                    if table.get('client_id_col') is not None:

                        app = db_connection_config['app']
                        env = db_connection_config['env']
                        ingestion_mode = db_connection_config['ingestion_mode']
                        meta_db = db_connection_config['meta_db']
                        client_id_col = table['client_id_col']
                        src_db = table['src_db']
                        src_schema = table['src_schema']
                        src_table = table['src_table']

                        if archived_tables != None and src_table.lower() in archived_tables:
                            arc_df = (arc_df.where((F.upper(F.col("arc_table")) == f'{src_table.upper()}'.replace('_TAB','_ARCH_TAB')) &
                                                   (F.lower(F.col("src_db")) == f'{src_db.lower()}') &
                                                   (F.lower(F.col("arc_schema")) == f'{src_schema.lower()}')))
                            active_arc_df = remove_inactive_clients(app, ingestion_mode, arc_df, client_id_col,
                                                                    src_db, src_schema, src_table.replace('_tab','_arch_tab'), env, meta_db)
                            arc_count = active_arc_df.select(F.sum(F.coalesce(F.col("arc_count"), F.lit(0)))).collect()[0][0]
                            active_client_count_df = remove_inactive_clients(app, ingestion_mode, result_df, client_id_col,
                                                                    src_db, src_schema, src_table, env, meta_db)
                            src_count = active_client_count_df.select(F.sum(F.coalesce(F.col("src_count"), F.lit(0)))).collect()[0][0]
                            src_count = (src_count or 0) + (arc_count or 0)
                            table['arc_count'] = arc_count
                            table['src_count'] = src_count
                        else:
                            active_client_count_df = remove_inactive_clients(app, ingestion_mode, arc_df, client_id_col,
                                                                    src_db, src_schema, src_table.replace('_tab','_arch_tab'), env, meta_db)
                            # summing client level counts to get table count 
                            table_count = active_client_count_df.select(F.sum("src_count")).collect()[[0]][0]
                            table['arc_count'] = None
                            table['src_count'] = table_count
                            if table_count is None:
                                #case when no active clients in a table
                                table_count = 0
                                table['src_count'] = table_count
                    else:
                        table['src_count'] = result_df.select("src_count").collect()[0][0]
                        table['arc_count'] = None
                else:
                    # if the src table has 0 records , and the query has a group by clause
                    # count should be set to 0
                    # all other fields will be null
                    if "src_count" in result_df.schema.fieldNames() or "SRC_COUNT" in result_df.schema.fieldNames():
                        table['src_count'] = 0
                        table['arc_count'] = None


            table["is_error_src"] = 0
            table["error_msg_src"] = None
            table["attempt_num"] = db_connection_config.get("attempt_num")
            table["query_duration_seconds"] = query_duration_seconds
            table["query"] = query
            if function == "data":
                sor_df = result_df.withColumn("error_msg_src", F.lit(""))
                sor_df.cache()
                sor_df.count()
                table["sor_df"] = sor_df
            return table
        

        except Exception as e:
            if attempt_num < max_no_of_attempts:
                db_connection_config["attempt_num"] = str(attempt_num + 1)
                db_connection_config['max_timeout_seconds'] = max_timeout_seconds + max_timeout_seconds_incr

                # 30 seconds to sleep before retry
                time.sleep(30)

                return self.query_sor((table, db_connection_config, function))
            else:
                # retries exhausted
                # sending back error details 


                table["is_error_src"] = 1
                table["error_msg_src"] = str(e)[:6000]
                # table["error_msg_src"] = str(e.args[1])
                table["attempt_num"] = db_connection_config.get("attempt_num")
                table["query_duration_seconds"] = None
                table["query"] = query
                if function == "data":
                    data = [(table['src_db'], table['src_schema'], table['src_table'], table['error_msg_src'])]
                    data_schema = StructType([StructField('src_db', StringType(), False),
                                              StructField('src_schema', StringType(), False),
                                              StructField('src_table', StringType(), False),
                                              StructField('error_msg_src', StringType(), False)])
                    sor_df = spark.createDataFrame(data=data, schema=data_schema)
                    sor_df.cache()
                    sor_df.count()
                    table["sor_df"] = sor_df
                return table
            


class DeltaQueryUtil:
    def get_sor_schema_client_map(self, app, meta_db, group_by_column):
        sor_schema_client_map_df = (
            spark.read.table(f"{meta_db}.sor_schema_client_map")
                 .filter(F.lower(F.col("src_app") == app.lower()))
                 .filter(F.col("locked_ts").isNull())
                 .withColumn("is_valid_client", is_valid_client(F.col("sgdp_org_id"), F.col("sgdp_org_id"), F.col("temp_sgdp_org_id")))
                 .filter(F.col("is_valid_client") == True)
                 .select("sgdp_org_id", "sgdp_client_id", "schema", "db")
        )
        if group_by_column == 'client_id' and sor_schema_client_map_df.count() == 0:
            sor_schema_client_map_df = (
                spark.read.table(f"{meta_db}.table_map")
                     .filter(F.lower(F.col("src_app")) == app.lower())
                     .filter(F.col("active_flag") == 'Y')
                     .select("client_id", F.lower(F.col("src_schema")).alias("schema"), F.lower(F.col("src_db")).alias("db"))
                     .withColumnRenamed("client_id", "sgdp_org_id")
                     .dropDuplicates()
            )
        return sor_schema_client_map_df
    
    def group_counts_by_schema(self, result_df, sor_schema_client_map_df, group_by_column):
        join_df = (
            result_df.alias("r")
            .join(
                F.broadcast(sor_schema_client_map_df).alias("clnt_map"),
                F.col(f"r.{group_by_column}") == F.col(f"clnt_map.sgdp_org_id"),
            )
        )

        join_df = join_df.select("r.delta_count", F.concat_ws(".", "clnt_map.db", "clnt_map.schema").alias("schema_id"))

        # if group_by_column == client_id:
        #     join_df = not required commented ConnectionAbortedError
        # else:
        #     lorum epsum

        schema_lvl_count_df = (
            join_df.groupBy("schema_id")
            .agg(F.sum("delta_count").alias("delta_count"))
        )

        schema_lvl_count_df = schema_lvl_count_df.select("delta_count", "schema_id")

        return schema_lvl_count_df
    
    def group_count_by_schema_for_wfnpi_and_portal_wfnpi(self, app, env, meta_db, result_df, client_id_col):
        db_schema_spec_df = spark.read.table(f"{meta_db}.__db_schema_spec").filter(F.lower(F.col("app")) == F.lit(app))
        result_df.cache()
        result_df = (
            result_df.alias("r").join(db_schema_spec_df.alias('d'), result_df['__schema_id'] == db_schema_spec_df['id'])
                     .select("r.delta_count", f"r.{client_id_col}", "r.__schema_id", "r.max_op_ts", "r.query_exec_time",
                        "d.src_db", "d.src_schema")
        )

        client_schema_map_df = get_client_schema_map_df(app, env)
        result_df = (
            result_df.alias("r")
            .join(client_schema_map_df.alias("c"),
                  (result_df[client_id_col] == client_schema_map_df[client_id_col]) &
                  (F.lower(result_df["src_db"]) == F.lower(client_schema_map_df["db"])) &
                  (F.lower(result_df["src_schema"]) == F.lower(client_schema_map_df["schema"])))
            .select("r.delta_count", "r.__schema_id", "r.max_op_ts", "r.query_exec_time")
        )
        schema_lvl_count_df = (
            result_df.groupBy("__schema_id")
                .agg(F.sum("delta_count").alias("delta_count"), F.max("max_op_ts").alias("max_op_ts"),
                     F.max("query_exec_time").alias("query_exec_time"))
                .select("delta_count", "__schema_id", "max_op_ts", "query_exec_time")
        )
        return schema_lvl_count_df
    
    def group_counts_by_schema_for_dataflow(self, app, env, meta_db, result_df, client_id_col):
        if app in ("portal-wfnpi","wfnpi"):
            return self.group_count_by_schema_for_wfnpi_and_portal_wfnpi(app, env, meta_db, result_df, client_id_col)
        client_schema_spec_df = spark.read.table(f"{meta_db}.client_schema_spec").filter(F.lower(F.col("app")) == F.lit(app))
        result_df = (
            result_df.aliad("r")
                .join(client_schema_spec_df.alias("c"),
                      F.lower(result_df[client_id_col]) == F.lower(client_schema_spec_df['client_id']))
                .select("r.delta_count", "r.max_op_ts","r.query_exec_time", f"r.{client_id_col}", "c.db", "c.schema")
        )
        db_schema_spec_df = spark.read.table(f"{meta_db}.__db_schema_spec").filter(F.lower(F.col("app")) == F.lit(app))
        result_df = (
            result_df.alias("r")
                .join(
                    db_schema_spec_df.alias("d")
                    (result_df["db"] == db_schema_spec_df["src_db"])
                    & (result_df["schema"] == db_schema_spec_df["src_schema"]),
                )
                .select("r.delta_count","d.id","r.max_op_ts", "r.query_exec_time")
                .withColumnRenamed("id","__schema_id")
        )
        schema_lvl_count_df = (
            result_df.groupBy("__schema_id")
                .agg(F.sum("delta_count").alias("delta_count"), F.max("max_op_ts").alias("max_op_ts"),
                     F.max("query_exec_time").alias("query_exec_time"))
                .select("delta_count", "__schema_id", "max_op_ts", "query_exec_time")
        )
        return schema_lvl_count_df
    
    def handle_purged_tables(self, app, ingestion_mode, meta_db, delta_db, table_name, group_by_column, client_id_col,
                             sor_schema_client_map_df):
        
        purged_tables_df = spark.sql(
            f"""select src_db, src_schema, client_id, predicate_column, predicate_value
            from {meta_db}.datawatch_custom_client_query_spec
            where lower(app) = lower('{app}')
            and ingestion_mode = lower('{ingestion_mode}')
            and lcase(src_table) = '{table_name.replace('__dol','$')}'"""
        )
        purged_tables_df.cache()

        active_clients_data_df = (
            purged_tables_df.alias("p").join(
                F.broadcast(sor_schema_client_map_df).alias("clnt_map"),
                (F.col("p.src_db") == F.lower(F.col("clnt_map.db")))
                & (F.col("p.src_schema") == F.lower(F.col("clnt_map.schema")))
                & (F.col("p.client_id") == F.col("clnt_map.sgdp_client_id")),
                "inner",
            )
        ).select("p.*", "clnt_map.sgdp_client_id", "clnt_map.sgdp_org_id")

        delta_table_df = spark.read.table(f"""{delta_db}.{table_name}""")
        predicate_column = (
            purged_tables_df.select("predicate_column").limit(1).collect()[0][0]
        )

        unfiltered_delta_data_df = (
            delta_table_df.alias("d").join(
                F.broadcast(active_clients_data_df.alias("a")),
                delta_table_df[client_id_col] == active_clients_data_df["client_df"],
            )
        ).select("d.*","a.predicate_value")

        filtered_delta_data_df = unfiltered_delta_data_df.where(
            F.col(f"{predicate_column}") >= F.col("predicate_value")
        )

        active_clients_delta_data_df = (
            (
                filtered_delta_data_df.alias("f").join(
                    F.broadcast(active_clients_data_df.alias("a")),
                    F.col(f"f.{group_by_column}") == F.col("a.sgdp_org_id"),
                )
            )
            .withColumn(
                "schema_id", F.concat_ws(".", F.col("a.src_db"), F.col("a.src_schema"))
            )
            .groupBy("schema_id")
            .agg(F.count("*").alias("delta_count"))
        )

        result_df = active_clients_delta_data_df.select("delta_count","schema_id")

        return result_df
    
    def materialise_delta_query_results(self, delta_query_config):

        app = delta_query_config.app
        env = delta_query_config.env
        table_name = delta_query_config.table_name
        query = delta_query_config.query
        group_by_column = delta_query_config.group_by_column
        ingestion_mode = delta_query_config.ingestion_mode
        meta_db = delta_query_config.meta_db
        delta_db = delta_query_config.delta_db
        purged_tables = delta_query_config.purged_tables
        client_id_col = delta_query_config.client_id_col

        try:
            result_df = spark.sql(query)

            if ingestion_mode == "s3" and group_by_column in ('sgdp_org_id', 'client_id'):
                sor_schema_client_map_df = self.get_sor_schema_client_map(app, meta_db, group_by_column)
                if table_name in purged_tables:
                    result_df = self.handle_purged_tables(app, ingestion_mode, meta_db, delta_db, table_name,
                                                          group_by_column, client_id_col, sor_schema_client_map_df)
                else:
                    # count will be reported at client level
                    # convert it to schema level
                    result_df = self.group_counts_by_schema(result_df, sor_schema_client_map_df, group_by_column)

            if ingestion_mode == "dataflow" and group_by_column != "__schema_id":
                # map schema id uuid value to db.schema
                result_df = self.group_counts_by_schema_for_dataflow(app, env, meta_db, result_df, group_by_column)
            
            result_df = result_df.withColumn("delta_table", F.lit(table_name))\
                                 .withColumn("query", F.lit(query))\
                                 .withColumn("is_error_delta", F.lit(0))\
                                 .withColumn("error_msg_delta", F.lit(None))
            
            if result_df.count() == 0:
                result_dict = {
                    "delta_count" : 0,
                    "query" : query,
                    "group_by_column" : group_by_column,
                    "delta_table" : table_name,
                    "is_error_delta" : 0,
                    "error_msg_delta" : None
                }
                return [result_dict]
            
            return result_df.collect()

        except Exception as e:

            error_row = [{
                "delta_count" : None,
                "query" : query,
                "group_by_column" : None,
                "delta_table" : table_name,
                "is_error_delta" : 1,
                "error_msg_delta" : str(e)[:6000]
            }]
            get_logger(__name__).error(f"Exception in materialize_delta_query_result: {table_name, ste(e)}")
            return error_row
        
    async def run_queries_on_delta(self, delta_query_config: DeltaDbMetadata.DeltaQueryConfig):
        #Executes queries on delta table and gets back a spark collection of the df with counts

        try:

            table_count_collection = await.asyncio.get_running_loop().run_in_executor(
                None,
                self.materialise_delta_query_result,
                delta_query_config
            )

            return table_count_collection
        
        except Exception as e:
            raise e 
            

    async def prepare_delta_query(self, table_query_list: list): #rename to start_query_on_delta
        # takes a list of tables and the query that runs on the table 
        # passes each tuple to the function that runs queries on delta
        try:
            results = await asyncio.gather(*(self.run_queries_on_delta(delta_query_config) for delta_query_config in table_query_list))
            return results
        except Exception as e:
            get_logger(__name__).error(f"Exception in prepare_delta_query")
            raise e
    

class ParallelQueryRunner:

    def __init__(self, target: str, function: str, delta_table_query_list = None, sor_dbs = None, num_parallel_db_connections=None):
        if target.lower() not in ["sor", "delta", "sor_delta"]:
            raise ValueError(f"Invalid target={target} parameter. valid values are 'sor', 'delta', or 'sor_delta'")
        
        self.target = target

        if self.target in ["sor","sor_delta"] and sor_dbs == None:
            raise ValueError(f"sor_dbs must not be None when target = {self.target}")
        
        if self.target in ["delta","sor_delta"] and delta_table_query_list == None:
            raise ValueError(f"delta_table_query_list must not be None when target = {self.target}")
        
        self.function = function
        self.delta_table_query_list = delta_table_query_list
        self.sor_dbs = sor_dbs
        self.num_parallel_db_connections = num_parallel_db_connections
        self.delta_query_util = DeltaQueryUtil()
        self.sor_query_util = SorQueryUtil()
    

    async def query_delta_tables(self):
        delta_count_results = await self.delta_query_util.prepare_delta_query(self.delta_table_query_list)
        return delta_count_results
    
    def query_sor_tables(self):
        sor_results = self.sor_query_util.start_query_on_sor(
            dbs = self.sor_dbs, num_parallel_db_connections=self.num_parallel_db_connections, function=self.function)
        return sor_results
    
    def query_sor_delta_in_parallel(self, sor_delta_input: tuple):
        # sor_delta_input[0] will be either sor or delta 

        # sor_delta_input[1] will be list of (table, query) tuples for delta
        # for SOR, it will be a list of dictionaries with db metadata

        # print(sor_delta_input[0])
        sor_count_results = []
        delta_count_results = None
        if sor_delta_input[0] == "sor":
            sor_dbs = sor_delta_input[1]
            sor_count_results = self.sor_query_util.start_query_on_sor(
                dbs = self.sor_dbs, num_parallel_db_connections=self.num_parallel_db_connections, 
                function=self.function)
            # print("obtained results")
            # print("sor_count_results")
            return ("sor","sor_count_results")
        
        if sor_delta_input[0] == "delta":
            table_query_list = sor_delta_input[1]
            delta_count_results = asyncio.run(self.delta_query_util.prepare_delta_query(table_query_list))

            # print("delta_count_fw_results")
            # print(delta_count_results)
            return ("delta", delta_count_results)
    
    def start_parallel_query_on_sor_delta(self, table_query_list, sor_dbs):
        # constructing input for thread pool .map
        sor_delta_input = [
            ("delta", table_query_list),
            ("sor", sor_dbs)
        ]

        delta_sor_count_map = {}
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = executor.map(self.query_sor_delta_in_parallel, sor_delta_input)
            for item in results:
                # item[0] will be delta or sor 
                delta_sor_count_map[item[0]] == self.flatten_result(item[1], item[0])

        return delta_sor_count_map
    
    def flatten_result(self, result:list, source: str):
        # flattens nested lists to a single list 
        if source == "sor":
            return sum(result, [])
        if source == "delta":
            return sum(result,[])
        
    def run_queries(self):
        get_logger(__name__).info(f"Starting parallel query runner on {','.join(self.target.split('_'))}")
        if self.target == "sor":
            return self.flatten_result(self.query_sor_tables(), 'sor')
        
        if self.target == "delta":
            nest_asyncio.apply()
            delta_query_results = asyncio.run(self.query_delta_tables())
            return self.flatten_result(delta_query_results, 'delta')
        
        if self.target == "sor_delta":
            return self.start_parallel_query_on_sor_delta(self.delta_table_query_list, self.sor_dbs)
        

def get_active_clients_portal_wfn_and_portal_wfnpi(app, env):
    if app == 'portal-wfn':
        client_schema_map_df = spark.sql(
            f"""with pass_Data as
            (
            SELECT CLIENT_OID, concat(POD, '_ACS') as schema, db as portal_db
            FROM ssot_blue_dccommon_{env}.WFN_PORTAL_CLIENT_POD_INFO
            ),
            env_details as
            (
            SELECT DISTINCT pass_data.client_oid, upper(substr(env.SRC_DB,1,(instr(upper(env.SRC_DB),'_SVC1')-1))) as db, schema, pass_data.portal_db
            FROM pass_Data
            INNER JOIN ssot_blue_dccommon_{env}.wfn_env_details env on pass_Data.schema = env.schema AND src_Schema=base_schema
            )
            SELECT CLIENT_OID, db, schema
            FROM
            (
            SELECT DISTINCT CLIENT_OID, db, schema from env_details
            UNION
            SELECT DISTINCT CLIENT_OID, db, replace(schema, '_ACS','_PORTAL') as schema from env_details
            UNION
            SELECT DISTINCT CLIENT_OID, db, 'ADP_CLIENT' as schema from env_details
            UNION
            SELECT DISTINCT CLIENT_OID, portal_db as db, 'COMMON' as schema from env_details
            )
            union
                (
                with pass_Data as
                    (
                        select distinct client_oid, upper(substr(env.SRC_DB,1,(instr(upper(env.SRC_DB), '_SVC1)-1))) as db, concat(POD,'_ACS') as schema
                        from ssot_blue_dccommon_{env}.WFN_PORTAL_CLIENT_POD_INFO
                        inner join ssot_blue_dccommon_{env}.wfn_env_details env on concat(POD, '_ACS') = env.pass_schema_nm and src_schema=base_schema
                    )
                    select * from 
                    (
                        select distinct concat(pass_data.db, '.',pass_Data.schema) as CLIENT_OID, pass_Data.db, pass_data.schema as schema from pass_data
                        union
                        select distinct 'META_V11_0_0_118' as CLIENT_OID, 'WFC077P' AS db, 'META_V11_0_0_118' as schema from pass_data
                        union
                        select distinct 'META_V11_0_0_11G' as CLIENT_OID, 'WFC077P' AS db, 'META_V11_0_0_11G' as schema from pass_data
                        union
                        select distinct concat('WFNADM1P','COMMON') as CLIENT_OID, 'WFNADM1P' as db, 'COMMON' AS schema from pass_data
                    )
                )"""
        )
    else:
        slient_schema_map_df = spark.sql(
            f"""with pass_Data as
            (
            SELECT CLIENT_OID, concat(POD, '_ACS') as schema, db as portal_db
            FROM ssot_blue_dccommon_{env}.WFN_PORTAL_CLIENT_POD_INFO
            ),
            env_details as
            (
            SELECT DISTINCT pass_data.client_oid, upper(substr(env.SRC_DB,1,(instr(upper(env.SRC_DB),'_SVC1')-1))) as db, schema, pass_data.portal_db, c_vw.instance_name, c_vw.schema_name
            FROM pass_Data
            INNER JOIN ssot_blue_dccommon_{env}.pi_client_logon_vw c_vw on pass_data.client_oid = c_vw.organization_oid
            INNER JOIN ssot_blue_dccommon_{env}.pi_env_details env
                ON pass_data.schema = env.pass_schma_nm and src_schema=base_schema
                and upper(substr(env.SRC_DB,1,(instr(upper(env.SRC_DB),'_SVC1')-1))) = upper(substr(c_vw.instance_name,1,(instr(upper(c_vw.instance_name),'_SVC1')-1)))
            )
            SELECT CLIENT_OID, db, schema
            FROM
            (
            SELECT DISTINCT CLIENT_OID, db, schema from env_details
            UNION
            SELECT DISTINCT CLIENT_OID, db, 'ADP_CLIENT' as schema from env_details
            UNION
            SELECT DISTINCT CLIENT_OID, portal_db as db, 'COMMON' as schema from env_details
            )
            union
             (
                with 
                pass_Data as
                    (
                        select distinct pass_data.client_oid, upper(substr(env.SRC_DB,1,(instr(upper(env.SRC_DB), '_SVC1)-1))) as db, concat(pass_data.POD,'_ACS') as schema
                        from ssot_blue_dccommon_{env}.WFN_PORTAL_CLIENT_POD_INFO pass_data
                        inner join ssot_blue_dccommon_{env}.pi_client_logon_vw c_vw on pass_data.client_oid = c_vw.organization_oid
                        inner join ssot_blue_dccommon_{env}.pi_env_details env on concat(POD, '_ACS') = env.pass_schema_nm and src_schema=base_schema
                    )
                    select * from 
                    (
                        select distinct concat(pass_data.db, '.',pass_Data.schema) as CLIENT_OID, pass_Data.db, pass_data.schema as schema from pass_data
                        union
                        select distinct 'META_V11_0_0_118' as CLIENT_OID, 'WPC001P' AS db, 'META_V11_0_0_118' as schema from pass_data
                        union
                        select distinct 'META_V11_0_0_11G' as CLIENT_OID, 'WPC001P' AS db, 'META_V11_0_0_11G' as schema from pass_data
                        union
                        select distinct concat('WPP001P','.COMMON') as CLIENT_OID, 'WPP001P' as db, 'COMMON' AS schema from pass_data
                        union
                        select distinct 'FFFFFFFFFFFFF' as CLIENT_OID,  'WPP001P' as db, 'PAASDP010_ACS' as schema from pass_data
                    )
            )"""
        )       
    return client_schema_map_df


def get_client_schema_map_df(app: str, env: str) -> DataFrame:
    if app in ("portal-wfn", "portal-wfnpi"):
        get_client_schema_map_df = get_active_clients_portal_wfn_and_portal_wfnpi(app, env)
    else:
        sor_client_schema_table_map = {
            "wfn" : "wfn_client_logon_vw",
            "wfnpi" : "pi_client_logon_vw"
        }

        client_schema_map_df = (
            spark.table(f"ssot_blue_dccommon_{env}.{sor_client_schema_table_map[app]}")
                 .withColumn("schema", F.regexp_replace(F.lower(F.col("schema_name")), 'b|g',''))
                 .filter(F.col("schema_version") > "20")
                 .filter(F.col("id_code") == F.col("parent_code"))
                 .filter(F.col("organization_oid").isNotNull())
                 .withColumn("db", F.split(F.lower(F.col("instance_name")), "-svc1").getItem(0))
                 .filter(F.col("organization_oid").cast(LongType()).isNotNull())
        )

        if app == "wfn":
            client_schema_map_df = (
                client_schema_map_df.where("upper(farm_name) not like '%CLOUD%'")
                .where("upper(INSTANCE_NAME) not like 'WFE00%")
            )
        if app == "wfnpi":
            client_schema_map_df = (
                client_schema_map_df.where("upper(farm_name) like '%CLOUD%'")
                .where("upper(instance_name) not like 'WPT%")
            )
    
    return client_schema_map_df


def get_client_schema_map(app: str, env: str, src_db: str, src_schema: str, src_table: str) -> DataFrame:
    client_schema_map_df = get_client_schema_map_df(app, env)

    # if not 
    # client 

    if app in ("wfn", "wfnpi"):
        client_schema_map_df = (
            client_schema_map_df.filter(
                (F.lower(F.col("schema")) == src_schema.lower()) & (F.lower(F.col("db")) == src_db.lower()))
            .select("vpd_key")
            .distinct()
        )
        
    if app in ("portal-wfn", "portal-wfnpi"):
        # renaming column, so that it matches the client_id column in delta table 
        client_schema_map_df = (
            client_schema_map_df.filter(
                (F.lower(F.col("schema")) == src_schema.lower()) & (F.lower(F.col("db")) == src_db.lower()))
            .select("client_oid")
            .distinct()
        )

    return client_schema_map_df


def remove_inactive_clients(app, ingestion_mode, sor_count_results_df, client_id_col, src_db, src_schema, src_table, env, meta_db):
    if ingestion_mode in ("s3", "dataflow"):
        if app in ("wfn", "portal-wfn", "wfnpi", "portal-wfnpi"):

            client_schema_map_df = get_client_schema_map_df(app, env, src_db, src_schema, src_table)

            sor_active_client_count_df = F.broadcast(client_schema_map_df).join(
                sor_count_results_df.alias("count_results"),
                F.lower(sor_count_results_df[client_id_col]) == F.lower(client_schema_map_df[client_id_col])
            ).select("count_results.*")

            if app  == "wfn" and src_schema.lower() == "wfn8sch014982" and src_db.lower() == "wfc077p":
                # ng_common will not be in wfn_client_logon_vw - it is not a client 
                # there are rown with vpd_key = ng_common, so this needs to be considered 
                # ng_common data is only ingested from sfc077p.wfn8sch014892
                sor_active_client_count_df = sor_active_client_count_df.union(
                    sor_count_results_df.filter(F.lower(F.col('vpd_key')).isin("ng_common","tlrecruitment", "totalcompmaster", "talent")))
                
        else:
            sor_schema_clnt_map_df = (
                spark.table(f"{meta_db}.sor_schema_client_map")
                      .filter(F.lower(F.col("src_app")) == app)
                      .filter(F.col("locked_ts").isNull())
                      .filter(F.lower(F.col("db")) == src_db.lower())
                      .filter(F.lower(F.col("schema")) == src_schema.lower())
                      .select("sgdp_client_id")
            )

            sor_active_client_count_df = F.broadcast(sor_schema_clnt_map_df).join(
                sor_count_results_df.alias("count_results"),
                F.lower(sor_count_results_df[client_id_col]) == F.lower(sor_schema_clnt_map_df['sgdp_client_id'])
                ).select("count_results.*")
            
        
        # table without client id column will have a dummy client id value 
        # adding them 
        non_client_id_df = sor_count_results_df.filter(F.col(client_id_col) == "datawatch_dummy_client_id")

        sor_active_client_count_df = sor_active_client_count_df.union(non_client_id_df)
    
    else:
        raise ValueError(f"Unsupported ingestion mode {ingestion_mode}")
    
    return sor_active_client_count_df

        