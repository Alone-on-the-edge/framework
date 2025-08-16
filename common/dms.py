import functools
import itertools
import json
import math
import re
import typing
from dataclasses import dataclass
from datetime import datetime
from multiprocessing.pool  import ThreadPool
from time import sleep

import boto3
from pyspark.sql import SparkSession, functions as F

from context import JobContext
from metadata.config import MetadataConfig
from metadata.oracle import OracleMetadataProvider
from metadata.postgres import PostgresMetadataProvider
from metadata.mysql import MysqlMetadataProvider
from metadata.sqlserver import SqlServerMetadataProvider
from metadata.provider import MetadataProvider


dms_client = boto3.client('dms',region_name = 'us-east-1')

@dataclass
class DMSLoadConfig:
    support_lobs: bool = True
    replication_instance_class: str = 'dms.r5.xlarge'
    dms_engine_version: str = '3.4.7'
    max_lob_size_in_kb: int = None
    skip_connection_tests: bool = False
    delete_existing_endpoints: bool = False

    def __post_init__(self):
        if self.max_lob_size_in_kb is not None:
            raise ValueError("Do not set any value for max_lob_size_in_kb")
        

class DMSRunner:

    def __init__(self, meta_conf_path: str, job_ctx: JobContext):
        self.meta_conf = JobContext.get_metadata_config(meta_conf_path, convert_to_lower=False)
        self.job_ctx = job_ctx
    
    def find_max_lob_size_in_kb(self, db_conf: MetadataConfig.DBConfig, thread_pool_size: int):
        if self.meta_conf.src_db_type == "oracle":
            provider:MetadataProvider = OracleMetadataProvider(self.meta_conf, self.job_ctx)
        elif self.meta_conf.src_db_type == "postgres":
            provider:MetadataProvider = PostgresMetadataProvider(self.meta_conf, self.job_ctx)
        elif self.meta_conf.src_db_type == "mysql":
            provider:MetadataProvider = MysqlMetadataProvider(self.meta_conf, self.job_ctx)
        elif self.meta_conf.src_db_type == "sqlserver":
            provider:MetadataProvider = SqlServerMetadataProvider(self.meta_conf, self.job_ctx)
        else:
            raise ValueError(f"Unsupported db type {self.meta_conf.src_db_type}")
        
        spark = SparkSession.getActiveSession()
        input_schemas = list(map(lambda sch: (sch.lower(),),db_conf.schemas))
        input_schemas_df = spark.createDataFrame(input_schemas, ["src_schema"])
        db_sch_spec_tbl = self.job_ctx.get_db_schema_spec_tbl()
        schema_id_df = (
            spark.table(db_sch_spec_tbl)
            .withColumn("src_schema", F.lower("src_schema"))
            .where(f"app = '{self.job_ctx.app}' AND src_db = '{db_conf.database_alias}'")
            .alias("ds")
            .join(input_schemas_df.alias("is"), "src_schema", "right_outer")
            ).select (
                F.col("ds.id").alias("src_schema_id"),
                F.col("ds.src_schema").alias("src_schema"),
                F.col("is.src_schema").alias("input_src_schema")
              ).cache()

        missing_schemas_df = (
            schema_id_df.where("src_schema_id is null")
            .select("input_src_schema").cache()
        )

        if missing_schemas_df.count() > 0:
            missing_schemas = [row["input_src_schema"] for row in missing_schemas_df.collect()]
            raise ValueError(f"Following schemas weren't found in {db_sch_spec_tbl} - {','.join(missing_schemas)}")
        
        col_spec_tbl = self.job_ctx.get_column_spec_tbl()
        col_spec_df = spark.table(col_spec_tbl).where(f"app = '{self.job_ctx.app}'")
        latest_col_spec_df = provider.get_latest_df(col_spec_df).cache()

        input_tables = list(map(lambda tbl: (tbl.lower(),),self.meta_conf.table_names))
        input_tables_df = spark.createDataFrame(input_tables, ["table_name"])
        tables_df = latest_col_spec_df.select("table_name").distinct().cache()
        missing_tables_df = (
            tables_df.alias("t")
            .join(input_tables_df.alias("it"), "table_name", "right_outer")
            .where("t.table_name is null")
            .select("it.table_name").cache()
        )
        if missing_tables_df.count() > 0:
            missing_tables = [row["table_name"] for row in missing_tables_df.collect()]
            raise ValueError(f"Following tables weren't found in {col_spec_tbl} - {','.join(missing_tables)}")

        cols_df = (
            latest_col_spec_df.alias("col")
            .join(schema_id_df.alias("s"),"src_schema_id")
            .select("s.src_schema", "col.*")
        ).cache()

        lob_cols_specs = (
            cols_df
            .where("is_lob_type = true")
            .select("src_schema","table_name","column_name","column_type")
            .collect()
        )

        def run_max_lob_size_query(lob_col_spec: typing.Tuple[str, str, str, str]):
            sch, tbl, col_name, col_type = lob_col_spec
            run_max_lob_size_query = f"SELECT {provider.get_max_lob_size_column_function(col_name, col_type)} AS max_size from {sch}.{tbl}"
            result =  provider.query_via_jdbc(run_max_lob_size_query, db_conf).collect()[0][0]
            if result is None:
                result = 0
            return float(result)
        
        pool = ThreadPool(thread_pool_size)
        max_lob_sizes = pool.map(run_max_lob_size_query, lob_cols_specs)
        max_lob_size_in_bytes = 0
        if len(max_lob_sizes) > 0:
            max_lob_size_in_bytes = functools.reduce(lambda x, y: max(x,y), max_lob_sizes)

        # min 5kn lob size
        return max(math.ceil(max_lob_size_in_bytes / 1024), 5)
    
    def init_replication_instance(self, instance_name, instance_class, dms_engine_version) -> str:
        rep_inst_status, rep_inst_arn, rep_inst_class = check_replication_instance_status(instance_name)
        dms_conf = self.job_ctx.get_dms_config()
        if rep_inst_status == 'available':
            if rep_inst_class != instance_class.strip():
                print(f"Replication instance {instance_name} exists already updating it's instance class")
                print("Rep Instance arn", rep_inst_arn,"Rep Instance class")
                dms_client.modify_replication_instance(
                    ReplicationInstanceArn = rep_inst_arn,
                    ReplicationInstanceClass = instance_class.strip(),
                    ApplyImmediately = True
                )
                sleep(30)
                wait_for_replication_instance_availability(instance_name)
            else:
                print(f"Replication Instance {instance_name} exists already")
        elif rep_inst_status == 'creating':
            print(f"Replication instance {instance_name} is in creating state, waiting for it to be available")
            wait_for_replication_instance_availability(instance_name)
        elif rep_inst_status == "ResourceNotFoundFault":
            print(f"Replication instance {instance_name} not found, creating it")
            rep_inst_arn = create_replication_instance(
                instance_name, dms_conf.security_grp_id,
                dms_conf.subnet_grp, engine_version = dms_engine_version,
                replication_instance_class = instance_class
            )
        else:
            raise Exception("Unexpected replication instance status", instance_class)
        
        return rep_inst_arn
    
    def init_source_endpoint(self, endpoint_name: str, db_conf: MetadataConfig.DBConfig,
                             timezone: str = None, delete_existing_point: bool = False) -> str:
        endpoint_status, endpoint_arn = check_endpoint_status(endpoint_name)
        if endpoint_status == "active":
            print("Source endpoint", endpoint_name, "exists already")
            if delete_existing_point:
                delete_endpoint(endpoint_arn)
                self.init_source_endpoint(endpoint_name, db_conf, timezone, delete_existing_point=False)
            elif endpoint_status == "ResourceNotFoundFault":
                print("Source endpoint", endpoint_name, "not found, creating it")
                username, password = self.job_ctx.resolve_secrets(db_conf.database_alias, self.meta_conf.secret_mode)
                kms_key_id = self.job_ctx.get_dms_encryption_key()

                if self.meta_conf.src_db_type == "oracle":
                    db_specific_cfg = {'ExtraConnectionAttributes' : 'useLogminerReader=N;useBfile=Y'}
                elif self.meta_conf.src_db_type == 'mysql':
                    if timezone is None:
                        raise ValueError("provide the timezone parameter for mysql")
                    else:
                        timezone_config = f"serverTimezone={timezone}"
                        db_specific_cfg = {'ExtraConnectionAttributes' : timezone_config}
                else:
                    db_specific_cfg = {}

                response = dms_client.create_endpoint(
                    EndpointIdentifier = endpoint_name,
                    ResourceIdentifier = endpoint_name,
                    EndpointType = 'source',
                    KmsKeyId = kms_key_id,
                    EngineName = self.meta_conf.src_db_type,
                    Username = username,
                    Password = password,
                    ServerName = db_conf.host,
                    Port = db_conf.port,
                    DatabaseName = db_conf.database,
                    Tags = [
                        {
                            'key' : 'component',
                            'Value' : 'dc-dms-automation'
                        }
                    ],
                    SslMode = 'none',
                    **db_specific_cfg
                )
                endpoint_arn = response['Endpoint']['EndpointArn']
            else:
                raise Exception("Unexpected target endpoint status", endpoint_name)
            
            return endpoint_arn
        
    def init_target_endpoint(self, endpoint_name: str, target_bucket_folder: str,
                             delete_existing_endpoint: bool = False) -> str :
        endpoint_status, endpoint_arn = check_endpoint_status(endpoint_name)
        if endpoint_status == "active":
            print("Target endpoint", endpoint_name, "exists already")
            if delete_existing_endpoint:
                delete_endpoint(endpoint_arn)
                self.init_target_endpoint(endpoint_name, target_bucket_folder, delete_existing_endpoint=False)
            elif endpoint_status == 'ResourceNotFoundFault':
                print("Target endpoint", endpoint_name,"not found, creating it")
                dms_config = self.job_ctx.get_dms_config()
                response = dms_client.create_endpoint(
                    EndpointIdentifier = endpoint_name,
                    ResourceIdentifier = endpoint_name,
                    EndpointType = 'target',
                    EngineName = 's3',
                    Tags = [
                        {
                            'key' : 'component',
                            'Value' : 'dc-dms-automation'
                        }
                    ],
                    SslMode = 'none',
                    ServiceAccessRoleArn = dms_config.iam_role,
                    S3Settings = {
                        'CsvRowDelimiter' : '\n',
                        'CsvDelimiter' : ',',
                        'BucketFolder' : target_bucket_folder,
                        'BucketName' : self.job_ctx.get_landing_bucket(),
                        'CompressionType' : 'gzip',
                        'DataFormat' : 'parquet',
                        'DatePartitionEnabled' : False
                    }) 
                endpoint_arn = response['Endpoint']['EndpointArn']  
            else:
                raise Exception("Unexpected target endpoint status", endpoint_name)
            
            return endpoint_arn
        
    def _fmt_name(self, db_alias: str  = None, prefix = None, max_len = 31):
        prefix = prefix or ''
        if db_alias:
            name = f"{prefix}{self.job_ctx.app}-{db_alias}".replace("_","-")
        else:
            #shared instances will not have db name in the insance name
            name = f"{prefix}{self.job_ctx.app}".replace("_","-")
            #Valid dms identifiers must begin with a letter, must contain only ASCII letters, digits, hyphens
            #and must not end with a hyphen or contain two consecutive hyphens.

        name = re.sub(r"^[^a-zA-Z]+","",name)
        name = re.sub(r"^[^a-zA-Z0-9-]+","",name)
        name = re.sub(r"-{2,}","-",name)

        return name[0:max_len].strip("-")
    
    def run_load(self, load_conf:DMSLoadConfig): 
        repl_task_name_list = []
        for db in self.meta_conf.src_dbs:
            if load_conf.use_shared_instance:
                rep_inst_name = self._fmt_name()
            else:
                rep_inst_name = self._fmt_name(db.database_alias)

            rep_inst_arn = self.init_replication_instance(
                rep_inst_name,
                load_conf.replication_instance_class,
                load_conf.dms_engine_version
            )

            src_ep_name = self._fmt_name(db.database_alias, prefix="src")
            src_ep_arn = self.init_source_endpoint(src_ep_name,db, load_conf.timezone, load_conf.delete_existing_endpoints)
            if not load_conf.skip_connection_tests:
                test_endpoint_connection(src_ep_arn, rep_inst_arn)

            target_ep_name = self._fmt_name(db.database_alias, prefix="tgt")
            target_bucket_folder = f"{self.job_ctx.get_dms_load_dir(include_bucket=False)}/{db.database_alias}"
            tgt_ep_arn = self.init_target_endpoint(tgt_ep_arn, target_bucket_folder, load_conf.delete_existing_endpoints)
            if not load_conf.skip_connection_tests:
                test_endpoint_connection(tgt_ep_arn, rep_inst_arn)

            rep_task_name = f"{self._fmt_name(db.database_alias, max_len=24)}-{datetime.now().strftime('%H%M%S')}"
            schema_table_list = list(itertools.product(db.schemas, self.meta_conf.table_names))
            schema_table_mapping = create_schema_table_mapping(schema_table_list)

            if load_conf.support_lobs == True and load_conf.max_lob_size_in_kb is None:
                print("Querying source to find max_lob_size_in_kb for limited size lob mode")
                load_conf.max_lob_size_in_kb = self.find_max_lob_size_in_kb(db, load_conf.thread_pool_size)
                print(f"Max lob size (in kb) = '{load_conf.max_lob_size_in_kb}'")
                if load_conf.max_lob_size_in_kb > 102400:
                    raise ValueError("Param max_lob_size_in_kb is larger than 100MB i.e the max recommended value for limited size lob mode")
                
            rep_task_status, rep_task_arn = create_replication_task(rep_task_name, src_ep_arn, tgt_ep_arn, rep_inst_arn, schema_table_mapping, load_conf)

            if rep_task_status == 'ready':
                print("Replication task", rep_task_arn, "created and is in ready state, starting it now")
                start_replication_task(rep_task_arn)

def check_replication_instance_status(replication_instance_name):
    print("Checking status of replication instance", replication_instance_name)
    try:
        response = dms_client.describe_replication_instances(
            Filters = [
                {
                    "Name" : 'replication-instance-id',
                    "Values" : [
                        replication_instance_name
                    ]
                }
            ]
        )
        replication_instance = response['ReplicationInstances'][0]
        return (replication_instance['ReplicationInstanceStatus'],
                replication_instance['ReplicationInstanceArn'],
                replication_instance['ReplicationInstanceClass'])
    
    except dms_client.exceptions.ResourceNotFoundFault as e:
        return e.response['Error']['Code'], None, None
    

def create_replication_instance(replication_instance_name, security_group_id, subnet_group,
                                engine_version, replication_instance_class):
    print("Creating replication instance ", replication_instance_name)
    response = dms_client.create_replication_instance(
        ReplicationInstanceIdentifier = replication_instance_name,
        AllocatedStorage = 123,
        ReplicationInstanceClass = replication_instance_class,
        VpcsecurityGroupIds = [
            security_group_id
        ],
        ReplicationSubnetGroupIdentifier = subnet_group,
        MultiAz = False,
        EngineVersion = engine_version,
        AutoMinorVersion = True,
        Tags = [
                    {
                        'key' : 'component',
                        'Value' : 'dc-dms-automation'
                    }
                ],
        PubliclyAccessible = False,
        ResourceIdentifier = replication_instance_name
    )
    wait_for_replication_instance_availability(replication_instance_name)
    return response['ReplicationInstance']['ReplicationInstanceArn']

def wait_for_replication_instance_availability(replication_instance_name):
    waiter = dms_client.get_waiter('replication_instance_available')
    waiter.wait(
        Filters = [
                {
                    "Name" : 'replication-instance-id',
                    "Values" : [
                        replication_instance_name
                    ]
                }
            ],
            WaiterConfig = {
                'Delay' : 123,
                'MaxAttempts' : 123
            }
    )

def delete_replication_instance(replication_instance_arn):
    print("Deleting replication instance ", replication_instance_arn)
    dms_client.delete_replication_instance(
        ReplicationInstanceArn = replication_instance_arn
    )
    waiter = dms_client.get_waiter('replication_instance_deleted')
    waiter.wait(
        Filters = [
                {
                    "Name" : 'replication-instance-id',
                    "Values" : [
                        replication_instance_arn
                    ]
                }
            ],
            WaiterConfig = {
                'Delay' : 123,
                'MaxAttempts' : 123
            }
    )
    print(replication_instance_arn,': Replication instance deleted')


def check_endpoint_status(endpoint_name):
    print("Checking status of endpoint", endpoint_name)
    try:
        response = dms_client.describe_endpoints(
            Filters = [
                {
                    "Name" : 'endpoint-id',
                    "Values" : [
                        endpoint_name
                    ]
                }
            ]
        )
        return response['Endpoints'][0]['status'], response['Endpoints'][0]['EndpointArn']
    except dms_client.exceptions.ResourceNotFoundFault as e:
        return e.response['Error']['Code'], ''
    
def test_endpoint_connection(endpoint_arn, replication_instance_arn):
    print("Testing connection of endpoint",endpoint_arn,"using the replication instance", replication_instance_arn)
    waiter = dms_client.get_waiter('test_connection_succeeds')
    connection_filter_conf = {
        "Filters" : [
                {
                    "Name" : 'endpoint-arn',
                    "Values" : [endpoint_arn]
                },
                {
                    "Name" : 'replication-instance-arn',
                    "Values" : [replication_instance_arn]
                }
            ]
    }
    connection_waiter_conf = {
        **connection_filter_conf,
        "WaiterConfig" : {
                'Delay' : 10,
                'MaxAttempts' : 120
            }
    }
    waiter.wait(**connection_waiter_conf)
    connection_status = dms_client.describe_connections(**connection_filter_conf)

    return connection_status['Connections'][0]['Status']

def delete_endpoint(endpoint_arn):
    print("Deleting endpoint", endpoint_arn)
    dms_client.delete_endpoint(EndpointArn = endpoint_arn)
    waiter = dms_client.get_waiter('endpoint_deleted')
    waiter.wait(
        Filters = [
                {
                    "Name" : 'endpoint-arn',
                    "Values" : [
                        endpoint_arn
                    ]
                }
            ]
    )
    print(endpoint_arn, ':Endpoint deleted')


def check_replication_task_status(replication_task_name):
    print("Checking status of replication task", replication_task_name)
    try:
        response = dms_client.describe_replication_tasks(
            Filters = [
                {
                    "Name" : 'replication-task-id',
                    "Values" : [
                        replication_task_name
                    ]
                }
            ],
            WithoutSettings = True
        )
        if response['ReplicationTasks'][0]['Status'] == 'stopped':
            return response['ReplicationTasks'][0]['StopReason'], response['ReplicationTasks'][0]['ReplicationTaskArn']
        else:
            return response['ReplicationTasks'][0]['Status'], response['ReplicationTasks'][0]['ReplicationTaskArn']
    except dms_client.exceptions.ResourceNotFoundFault as e:
        return e.response['Error']['Code'], ''
        
    
def create_schema_table_mapping(schema_table_list):
    print("Creating mapping for replication task")
    mapping = {'rules' : []}
    for index, value in enumerate(schema_table_list):
        mapping['rules'].append({
            'rule-type' : 'selection',
            'rule-id' : index + 1,
            'rule-name' : index + 1,
            'object-locator' : {
                'schema-name' : value[0],
                'table-name' : value[0]
            },
            'rule-action' : 'include',
            'load-order' : index + 1
        })
    mapping['rules'].append({
        'rule-type' : 'transformation',
        'rule-id' : len(schema_table_list)+1,
        'rule-name' : "convert all schema names to lowercase",
        'rule-target' : "schema",
        'object-locator' : {
            'schema-name' : "%"
        },
        'rule-action' : 'convert-lowercase'
    })
    mapping['rules'].append({
        'rule-type' : 'transformation',
        'rule-id' : len(schema_table_list)+2,
        'rule-name' : "convert all table names to lowercase",
        'rule-target' : "table",
        'object-locator' : {
            'schema-name' : "%",
            'table-name' : "%"
        },
        'rule-action' : 'convert-lowercase'
    })

    return json.dumps(mapping)


def create_replication_task(replication_task_name, src_endpoint_arn, tgt_endpoint_arn,replication_instance_arn, json_mapping, load_conf: DMSLoadConfig):
    if load_conf.max_lob_size_in_kb is None:
        load_conf.max_lob_size_in_kb = 1

    print("Cration replication task", replication_task_name)
    task_settings = """{
        "Logging" : {
            "EnableLogging" : true,
            "LogComponents" : [
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "TRANSFORMATION"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "SOURCE_UNLOAD"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "IO"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "TARGET_LOAD"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "PERFORMANCE"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "SOURCE_CAPTURE"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "SORTER"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "REST_SERVER"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "VALIDATOR_EXT"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "TARGET_APPLY"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "TASK_MANAGER"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "TABLES_MANAGER"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "METADATA_MANAGER"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "FILE_FACTORY"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "COMMON"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "ADDONS"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "DATA_STRUCTURE"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "COMMUNICATION"
                },
                {
                    "Severity" : "LOGGER_SEVERITY_DEFAULT",
                    "Id" : "FILE_TRANSFER"
                }
            ]
        },
        "StreamBufferSettings" : {
            "StreamBufferCount" : 3,
            "CtrlStreamBufferSizeInMB" : 5,
            "StreamBufferSizeInMB" : 8
        },
        "ErrorBehavior" : {
            "FailOnNoTablesCaptured": true,
            "ApplyErrorUpdatePolicy":"SUSPEND_TABLE",
            "FailOnTransactionConsistencyBreached":false,
            "RecoverableErrorThrottlingMax":1800,
            "DataErrorEscalationPolicy": "SUSPEND_TABLE",
            "ApplyErrorEscalationCount":0,
            "RecoverableErrorStopRetryAfterThrottlingMax":true,
            "RecoverableErrorThrottling":true,
            "ApplyErrorFailOnTruncationDdl":false,
            "DataTruncationErrorPolicy":"SUSPEND_TABLE",
            "ApplyErrorInsertPolicy":"SUSPEND_TABLE",
            "ApplyErrorEscalationPolicy":"SUSPEND_TABLE",
            "RecoverableErrorCount":0,
            "DataErrorEscalationCount": 0,
            "TableErrorEscalationPolicy": "STOP_TASK",
            "RecoverableErrorInterval":5,
            "ApplyErrorDeletePolicy":"SUSPEND_TABLE",
            "TableErrorEscalationCount": 0,
            "FullLoadIgnoreConflicts": true,
            "DataErrorPolicy":"SUSPEND_TABLE",
            "TableErrorPolicy": "SUSPEND_TABLE"
        },
        "FullLoadSettings": {
            "CommitRate": 10000,
            "StopTaskCachedChangesApplied":false,
            "StopTaskCachedChangesNotApplied":true,
            "MaxFullLoadSubTasks":49,
            "TransactionConsistencyTimeout": 600,
            "CreatePkAfterFullLoad": false,
            "TargetTablePrepMode" : "DROP_AND_CREATE"
        },
        "TargetMetadata" : {
            "ParallelApplyBufferSize" : 0,
            "ParallelApplyQueuesPerThread": 0,
            "ParallelApplyThreads":0,
            "TargetSchema":"",
            "InlineLobMaxSize": 0,
            "ParallelLoadQueuesPerThread" : 0,
            "SupportLobs": {support_lobs},
            "LobChunkSize": 64,
            "TaskRecoveryTableEnabled" : false,
            "ParallelLoadThreads" : 0,
            "LoabMaxSize" : {lob_max_size},
            "BatchApplyEnabled" : false,
            "FullLobMode" : false,
            "LimitedSizeLobMode": {support_lobs},
            "LoadMaxFileSize" : 0,
            "ParallelLoadBufferSize" : 0
        },
        "BeforeImageSettings": null,
        "LookbackPreventionSettings": null,
        "CharacterSetSettings": null,
        "FailTaskWhenCleanTaskResourceFailed" : false,
        "PostProcessingRules": null
}""".replace('{lob_max_size}', str(load_conf.max_lob_size_in_kb), 1) \
    .replace('{support_lobs}',str(load_conf.support_lobs).lower(),2)


    dms_client.create_replication_task(
        ReplicationTaskIdentifier = replication_task_name,
        SourceEndpointArn = src_endpoint_arn,
        TargetEndpointArn = tgt_endpoint_arn,
        ReplicationInstanceArn = replication_instance_arn,
        MigrationType = 'full_load',
        TableMappings = json_mapping,
        ReplicationTaskSettings = task_settings,
        Tags = [
                    {
                        'key' : 'component',
                        'Value' : 'dc-dms-automation'
                    }
               ],
        ResourceIdentifier = replication_task_name
    )

    waiter = dms_client.get_waiter('replication_task_ready')
    waiter.wait(
            Filters = [
                    {
                        "Name" : 'replication-task-id',
                        "Values" : [
                            replication_task_name
                        ]
                    }
            ],
            WithoutSettings = True,
            WaiterConfig = {
                'Delay' : 123,
                'MaxAttempts' : 123
            }
    )

    replication_task_status = dms_client.describe_replication_tasks(
        Filters = [
                    {
                        "Name" : 'replication-task-id',
                        "Values" : [
                            replication_task_name
                        ]
                    }
        ],
        WithoutSettings = True
    )
    
    return replication_task_status['ReplicationTasks'][0]['Status'], replication_task_status['ReplicationTasks'][0]['ReplicationTaskArn']


def delete_replication_task(replication_task_arn):
    print("Deletig replication tasl", replication_task_arn)
    dms_client.delete_replication_task(ReplicationTaskArn = replication_task_arn)
    waiter = dms_client.get_waiter('replication_task_deleted')
    waiter.wait(
            Filters = [
                    {
                        "Name" : 'replication-task-arn',
                        "Values" : [
                            replication_task_arn
                        ]
                    }
            ],
            WaiterConfig = {
                'Delay' : 123,
                'MaxAttempts' : 123
            }
    )
    print(replication_task_arn, ": Replication task deleted")

def start_replication_task(replication_task_arn):
    print("Starting replication task ", replication_task_arn)
    dms_client.start_replication_task(ReplicationTaskArn = replication_task_arn,
                                      StartReplicationTaskType = 'start-replication'
    )

def reload_tgt_replication_task(replication_task_arn):
    print("Reloading ttarget of replication task ", replication_task_arn)
    dms_client.start_replication_task(ReplicationTaskArn = replication_task_arn,
                                      StartReplicationTaskType = 'reload-target'
    )

def resume_replication_task(replication_task_arn):
    print("Resuming processing of replication task ", replication_task_arn)
    dms_client.start_replication_task(ReplicationTaskArn = replication_task_arn,
                                      StartReplicationTaskType = 'resume-processing'
    )

#Instantiating dms and running it
dms_runner = DMSRunner(meta_conf_path = "ab.json", job_ctx = JobContext(app='ev5', env='prod', team = 'sfgd'))
load_conf = DMSLoadConfig(support_lobs=True, replication_instance_class='dms.xlarge',skip_connection_tests = True)
load_conf.use_shared_instance=True
dms_runner.run_load(load_conf)    