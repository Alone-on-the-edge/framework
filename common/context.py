import sys
sys.path.append("d:\Project\ssot")

import inspect

import json
import typing
from collections import namedtuple

import boto3
import requests
from pyspark import Row
from pyspark.sql import SparkSession

from common.control_spec import AppSpec,FlowSpec,InitIngestSpec,JobSpec,JobType
from common.databricks1 import NotebookConfig, DLTPipelineUtils, get_db_utils
from common.lrucache import LRUCache
from common.metadata.config import MetadataConfig

PROJECT_NAME = 'dataflow'
SCHEMAS_DIR = 'avro-schemas'
INACTIVE_SCHEMAS_DIR = 'inactive_schemas_dir'
DLT_STORAGE_DIR = 'dlt-storage'
CHECKPOINTS_DIR = 'checkpoints'
TRUNCATES_DIR = 'truncate-events'
MAX_FLOWS_PER_PIPELINE = 40
INIT_SCIRPTS_DIR = 'init_scripts'
DMS_LOAD_DIR = 'dms-load'

NON_PROD_KAFKA_SERVERS = ','.join([
    'b-1.kafka.us-east-1.amazonaws.com:9098',
    'b-2.kafka.us-east-1.amazonaws.com:9098',
    'b-3.kafka.us-east-1.amazonaws.com:9098',
])

PROD_KAFKA_SERVERS = ','.join([
    'b-1.kafka.us-east-1.amazonaws.com:9098',
    'b-2.kafka.us-east-1.amazonaws.com:9098',
    'b-3.kafka.us-east-1.amazonaws.com:9098',
])

FIT_KAFKA_EXTRA_JAAS_CONF = 'awsrole:arn:53538053/non_prod'

spark = SparkSession.getActiveSession()

DMSConfig = namedtuple("DMSConfig", ["iam_role","subnet_grp", "security_grp_id"])


class JobContext:
    """
    The JobContext class provides methods to load and process configuration data, which is represented by the MetadataConfig class. The get_metadata_config method in JobContext 
    reads a JSON file, processes its contents to create a MetadataConfig object, and ensures that the configuration is correctly structured for use in the application.
    """

    def __init__(self,app:str, env: str, team: str):
        self.app = app.lower().strip()
        self.team = team.lower().strip()
        self.ec2_metadata = JobContext.get_ec2.metadata()
        self.region = self.ec2_metadata['region']
        self.ssm_param_cache = LRUCache(max_size=512)

        if self.region != 'us-east-1':
            raise ValueError(f"Unsupported region = '{self.region}'")
        if env.lower().strip() == 'dit':
            self.env= 'dit'
        elif env.lower().strip() == 'fit':
            self.env= 'fit'
        elif env.lower().strip() == 'iat':
            self.env= 'iat'
        elif env.lower().strip() in ('prod', 'prd'):
            self.env= 'prod'
        else:
            raise ValueError(f"Invalid env = '{env}' parameter")
        
    @staticmethod
    def from_notebook_config():
        app = NotebookConfig.get_arg("app")
        env = NotebookConfig.get_arg("env")
        team = NotebookConfig.get_arg("team","sgdp")

        return JobContext(app,env,team)
    
    @staticmethod
    def get_metadata_config(config_path: str, convert_to_lower=False) -> MetadataConfig:
        with open(config_path, 'r') as f:
            meta_conf = json.load(f)  # Read JSON configuration from file
            # Process `src_dbs` to create a list of `MetadataConfig.DBConfig` objects
            meta_conf['src_dbs'] = list(map(lambda db_conf: MetadataConfig.DBConfig(**db_conf), meta_conf['src_dbs']))
            # Process `tenancy_conf` to create a `MetadataConfig.TenancyConfig` object
            meta_conf['tenancy_conf'] = MetadataConfig.TenancyConfig(**meta_conf['tenancy_conf'])
            # Create a `MetadataConfig` object from the processed metadata
            meta_conf = MetadataConfig(**meta_conf)
            if convert_to_lower:
                # Optionally convert table names and schema names to lowercase
                meta_conf.table_names = list(map(lambda tbl: tbl.lower(), meta_conf.table_names))
                for db in meta_conf.src_dbs:
                    db.schemas = list(map(lambda sch: sch.lower(), db.schemas))
            return meta_conf  # Return the `MetadataConfig` object


    # @staticmethod
    # def get_metadata_config(config_path: str, convert_to_lower=False) -> MetadataConfig:
    #     with open(config_path, 'r') as f:
    #         meta_conf = json.load(f)
    #         meta_conf['src_dbs'] = list(map(lambda db_conf: MetadataConfig.DBConfig(**db_conf), meta_conf['src_dbs']))
    #         meta_conf['tenancy_conf'] = MetadataConfig.TenancyConfig(**meta_conf['tenancy_conf'])
    #         meta_conf = MetadataConfig(**meta_conf)
    #         if convert_to_lower:
    #             meta_conf.table_names = list(map(lambda tbl: tbl.lower(), meta_conf.table_names))
    #             for db in meta_conf.src_dbs:
    #                 db.schemas = list(map(lambda sch: sch.lower(), db.schemas))

    #         return meta_conf

    @staticmethod
    def from_pipeline_tag():
        pipeline_api = DLTPipelineUtils()
        return JobContext(app=pipeline_api.get_arg_from_tags('app'),
                          env=pipeline_api.get_arg_from_tags('env'),
                          team=pipeline_api.get_arg_from_tags('team'))

    @staticmethod
    def get_ec2_metadata() -> dict:
        imds_endpoint = f"https://sfjsfs.com/latest"
        api_token = requests.put(f"{imds_endpoint}/api/token",
                                 headers={'X-aws-ec2-metadata-token-ttl-seconds': 300}).text
        
        return requests.get(f"imds_endpoint/dynamic/instance-identity/document",
                             headers={'X-aws-ec2-metadata-token': api_token}).json()
    
    def get_ssm_parameter(self, name) -> str:
        #'ssm' specifies the AWS Systems Manager (SSM) service. SSM is used for configuration management, parameter store, and other system management operations.
        ssm = boto3.client('ssm', region_name=self.region)
        param_name = f"/{PROJECT_NAME}/{self.team}/{self.env}/{name}"
        param_value = self.ssm_param_cache.get(param_name)
        
        if param_value is None:
            param_value = ssm.get_parameter(Name=param_name, WithDecryption=True)['Parameter']['Value']
            self.ssm_param_cache.put(param_name,param_value)

        return param_value
    
    def get_dms_encryption(self) -> str:
        ssm = boto3.client('ssm', region_name=self.region)
        param_name = "__DMS_CMK__"
        param_value = self.ssm_param_cache.get(param_name)
        if param_value is None:
            param_value = ssm.get_parameter(Name=param_name, WithDecryption=True)['Parameter']['Value']
            self.ssm_param_cache.put(param_name, param_value)

        return param_value
    
    def get_landing_bucket(self) -> str:
        return self.get_ssm_parameter("landing_bucket")
    
    def _format_path(self, path: str, include_bucket: bool, bucket: str = None):
        if bucket is None:
            bucket = self.get_landing_bucket()

        if include_bucket:
            return f"s3://{bucket}/{path}"
        else:
            return path
        
    def get_schemas_dir(self, include_bucket: bool = True) -> str:
        return self._format_path(f"{PROJECT_NAME}/{self.app}/{SCHEMAS_DIR}", include_bucket)
    
    def get_inactive_schemas_dir(self, include_bucket: bool = True) -> str:
        return self._format_path(f"{PROJECT_NAME}/{self.app}/{INACTIVE_SCHEMAS_DIR}", include_bucket)

    def get_truncates_dir(self, include_bucket: bool = True) -> str:
        return self._format_path(f"{PROJECT_NAME}/{self.app}/{TRUNCATES_DIR}", include_bucket)

    def get_checkpoints_dir(self, component_name: str, include_bucket: bool = True) -> str:
        return self._format_path(f"{PROJECT_NAME}/{self.app}/{CHECKPOINTS_DIR}/{component_name.lower().strip()}", include_bucket)
    
    def get_dlt_storage(self, layer: str, flow_grp_id: int = None, include_bucket: bool = True) -> str:
        if layer == 'silver' and flow_grp_id is None:
            raise ValueError("Expecting flow group id for silver")
        storage_dir = self._format_path(f"{PROJECT_NAME}/{self.app}/{DLT_STORAGE_DIR}/{layer}", include_bucket)
        if flow_grp_id:
            return f"{storage_dir}-{flow_grp_id}"
        else:
            return storage_dir
        
    def get_dms_load_dir(self, include_bucket: bool = True) -> str:
        return self._format_path(f"{PROJECT_NAME}/{self.app}/{DMS_LOAD_DIR}", include_bucket)

    def get_init_scripts_dir(self, version: str,include_bucket: bool = True):
        if 'snapshot' in version.lower():
            sub_dir = 'snapshot'
        else:
            sub_dir = 'release'
        
        return self._format_path(f"{PROJECT_NAME}/{INIT_SCIRPTS_DIR}/{sub_dir}/{version}", include_bucket, bucket='dgp-artifact-store-5846464')
    
    def get_control_db(self) -> str:
        return self.get_ssm_parameter('control_db')
    
    def get_dlt_pipeline_name(self, layer:str, flow_grp_id: int = None):
        if layer == "silver" and flow_grp_id is None:
            raise ValueError("Expecting flow group id for silver")
        if flow_grp_id is None:
            return f"ssot-{PROJECT_NAME}-{layer}-{self.app}"
        else:
            return f"ssot-{PROJECT_NAME}-{layer}-{self.app}-{flow_grp_id}"
        
    def get_job_name(self, job_type: JobType, flow_grp_id: int = None):
        job_name = f"ssot-{job_type}-{self.app}"
        if job_type == JobType.DATAFLOW_BRONZE:
            job_name = self.get_dlt_pipeline_name(layer='bronze')
        if job_type == JobType.DATAFLOW_SILVER:
            job_name = self.get_dlt_pipeline_name(layer='silver',flow_grp_id=flow_grp_id)    
        
        return job_name
    
    def get_silver_table_name(self, silver_view_name: str):
        return f"{self.get_app_spec().delta_db}.__apply_changes_storage_{silver_view_name}"
    
    def get_app_spec_table(self):
        return f"{self.get_control_db()}.app_spec"
    
    def get_flow_spec_table(self):
        return f"{self.get_control_db()}.flow_spec"
    
    def get_init_load_table(self):
        return f"{self.get_control_db()}.init_ingest_spec"
    
    def get_completed_batches_tbl(self):
        return f"{self.get_control_db()}.__completed_streaming_batches"
    
    def get_flow_errors_tbl(self):
        return f"{self.get_control_db()}.__flow_errors_logs"
    
    def get_truncate_status_tbl(self):
        return f"{self.get_control_db()}.__flow_truncate_status"
    
    def get_db_schema_spec_tbl(self):
        return f"{self.get_control_db()}.__db_schema_spec"
    
    def get_column_spec_tbl(self):
        return f"{self.get_control_db()}.__column_spec"
    
    def get_key_spec_tbl(self):
        return f"{self.get_control_db()}.__key_spec"
    
    def get_job_spec_tbl(self):
        return f"{self.get_control_db()}.job_spec"

    def get_job_monitoring_spec_tbl(self):
        return f"{self.get_control_db()}.job_monitoring_spec"
    
    def get_schema_topic_name(self):
        return f"{self.env}-{self.app}-schemas"
    
    def get_cdc_topic_name(self):
        return f"{self.env}-{self.app}-cdc"
    
    def get_kafka_config(self) -> dict:
        if self.region == 'us-east-1':
            kafka_conf = {
                "kafka.sasl.mechanism" :'AWS-MSK-IAM',
                "kafka.sasl.client.callback.handler.class" : "software.amazon.msk.auth.IAMClientCallbackHandler",
                "kafka.security.protocol" : "SASL_SSL",
                "kafka.sasl.jaas.config" : "software.amazon.msk.auth.iam.IAMLoginModule required;"
            }
        if self.env in {'dit','fit'}:
            kafka_conf['kafka.bootstrap.servers'] = NON_PROD_KAFKA_SERVERS
        else:
            kafka_conf['kafka.bootstrap.servers'] = PROD_KAFKA_SERVERS

        if self.env in {'dit','fit'}:
            kafka_conf['kafka.sasl.jaas.config'] = f"{kafka_conf['kafka.sasl.jaas.config'].rstrip(';')} {FIT_KAFKA_EXTRA_JAAS_CONF}"
        
        return kafka_conf
    
    def get_glue_catalog_id(self) -> str:
        if self.region == 'us-east-1':
            if self.env in {'dit','fit'}:
                return '5465313'
            else:
                return '9464333'
            
    def get_secret_scope(self):
        if self.team == 'sgdp':
            return 'dc-dms-automation'
        else:
            return self.get_ssm_parameter('secret_scope')
    
    def get_dms_config(self):
        if self.env in {'dit','fit'}:
            return DMSConfig(
                iam_role="arn:aws:iam:5238920520:role/platform-ingest-dms-role",
                subnet_grp="dc-dms-automation",
                security_grp_id="sg-080557395"
            )
        else: 
            return DMSConfig(
                iam_role="arn:aws:iam:5238920520:role/platform-ingest-dms-role",
                subnet_grp="full-dataload",
                security_grp_id="sg-073535395"
            )
       
    def resolve_secrets(self, db_name, secret_mode='multi'):
        secret_scope = self.get_secret_scope()

        def resolve_secrets_rec(app_name):
            try:
                if secret_mode == 'single':
                    user_secret_key = f"{app_name}_user_name"
                    password_secret_key = f"{app_name}_password"
                else:
                    user_secret_key = f"{app_name}_{db_name}_user_name"
                    password_secret_key = f"{app_name}_{db_name}_password"
                # import dbutils
                user_name = get_db_utils().secrets.getBytes(scope=secret_scope, key=user_secret_key).decode("utf-8")
                password = get_db_utils().secrets.getBytes(scope=secret_scope, key=password_secret_key).decode("utf-8")

                return user_name, password
            except Exception as e:
                if '-' in app_name and 'Secret does not exist ' in str(e):
                    return resolve_secrets_rec(app_name.replace('-','_'))
                else:
                    raise e
                
        return resolve_secrets_rec(self.app)
    
    def get_app_spec(self) -> AppSpec:

        def parse_flow_spec_row(flow_spec_row: Row) -> FlowSpec:
            flow_spec_dict = flow_spec_row.asDict(recursive=True)
            flow_spec_dict['target_details'] = FlowSpec.TargetDetails(**flow_spec_dict['target_details'])

            return FlowSpec(**flow_spec_dict)
        
        flow_specs = spark.table(self.get_flow_spec_table()) \
                        .where(f"app='{self.app}'") \
                        .collect()
        
        flow_specs = list(map(lambda row: parse_flow_spec_row(row), flow_specs))

        init_ingest_specs = spark.table(self.get_init_load_table()) \
                            .where(f"app='{self.app}' and is_complete = false") \
                            .collect()
        
        init_ingest_specs = list(map(lambda row: InitIngestSpec(**row.asDict(recursive=True)),init_ingest_specs))

        app_spec_rows = spark.table(self.get_app_spec_table()) \
                        .where(f"app='{self.app}'") \
                        .collect()
        
        if len(app_spec_rows) > 1:
            raise ValueError (f"Duplicates for app '{self.app}' found in app_spec table")
        elif len(app_spec_rows) == 0:
            raise ValueError(f"App '{self.app}' not found in app_spec table")
        else:
            app_spec_row = app_spec_rows[0]
            app_spec_dict = app_spec_row.asDict(recursive = True)
            app_spec_dict['bronze_spark_conf'] = AppSpec.BronzeSparkConf(**app_spec_dict['bronze_spark_conf'])
            app_spec_dict['tenancy_conf'] = AppSpec.TenancyConfig(**app_spec_dict['tenancy_conf'])
            app_spec = AppSpec(**{**app_spec_dict, 'flow_specs': flow_specs, 'init_ingest_specs' : init_ingest_specs})

            # app_spec.validate()

            return app_spec

    def get_job_specs(self) -> typing.List[JobSpec]:
        job_spec_rows = spark.table(self.get_job_spec_tbl()) \
                        .where(f"app='{self.app}'") \
                        .collect()
        job_specs = []
        for row in job_spec_rows:
            job_spec_dict = row.asDict(recursive=True)
            tasks = job_spec_dict['tasks']
            job_spec_dict['tasks'] = [JobSpec.TaskConf(**task) for task in tasks]

            cluster_conf = job_spec_dict['cluster_conf']
            autoscale_conf = cluster_conf.get('autoscale_conf')
            if autoscale_conf:
                job_spec_dict['cluster_conf']['autoscale_conf'] = JobSpec.ClusterConf.AutoScaleConf(**autoscale_conf)
            else:
                job_spec_dict['cluster_conf']['autoscale_conf'] = None
            job_spec_dict['cluster_conf'] = JobSpec.ClusterConf(**cluster_conf)

            schedule_conf = job_spec_dict.get('schedule_conf')
            if schedule_conf:
                job_spec_dict['schedule_conf'] = JobSpec.ScheduleConf(**schedule_conf)
            else:
                job_spec_dict['schedule_conf'] = None
            
            retry_conf = job_spec_dict['retry_conf']
            if retry_conf:
                job_spec_dict['retry_conf'] = JobSpec.RetryConf(**retry_conf)
            else:
                job_spec_dict['retry_conf'] = None

            notify_settings = job_spec_dict.get('notification_settings')
            if notify_settings:
                job_spec_dict['notification_settings']  = JobSpec.NotificationSettings(**notify_settings)

            else:
                job_spec_dict['notification_settings']  = None
            
            job_specs.append(JobSpec(**job_spec_dict))

        return job_specs
    
    def get_notification_api_endpoint_url(self) -> str:
        if self.env == 'fit' or self.env == 'dit':
            endpoint_url = "https://ds.api.fit.us-east-1.xxx.yyy/sfshf/event/publish"
        elif self.env == 'iat':
            endpoint_url = "https://ds.api.iat.us-east-1.xxx.yyy/sfshf/event/publish"   
        elif self.env == 'prod':
            endpoint_url = "https://ds.api.prod.us-east-1.xxx.yyy/sfshf/event/publish"

        return endpoint_url
    
    def get_notification_api_token_url(self) -> str:
        if self.env == 'fit':
            token_url = 'https://apoauthfit.nj.zzzo/auth/token'
        elif self.env == 'iat':
            token_url = 'https://apoauthiat.nj.zzzo/auth/token'
        elif self.env == 'prod':
            token_url = 'https://apoauthprd.nj.zzzo/auth/token'

        return token_url

methods = [member[0] for member in inspect.getmembers(JobContext) if inspect.isfunction(member[1])]
print(methods)