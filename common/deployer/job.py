import importlib
import traceback
import typing
from abc import ABC, abstractmethod

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterSpec, InitScriptInfo, S3StorageInfo, AwsAttributes, ClusterLogConf, \
AutoScale, AwsAvailability, RuntimeEngine, DataSecurityMode
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
from databricks.sdk.service.jobs import CreateJob, JobCluster, Task, NotebookTask, CronSchedule, TaskEmailNotifications, \
JobSettings, TaskDependency, Format, Source, PauseStatus
from databricks.sdk.service.pipelines import CreatePipeline, PipelineLibrary, NotebookLibrary, \
Notifications as PipelineNotifications

from ssot.common.context import JobContext
from ssot.common.control_spec import JobSpec, JobType
from ssot.common.databricks1 import JobUtils, DLTPipelineUtils
from ssot.common.databricks_ext import PipelineClusterEnriched, PipelineAutoScale
from ssot.common.logger import get_logger

JOB_CLUSTER_KEY = 'cluster'
PIPELINE_CLUSTER_LABEL = 'default'
PIPELINE_MAINTENANCE_CLUSTER_LABEL = 'maintenance'
PIPLELINE_SCALING_MODE = 'ENCHANCED'
DLT_CURRENT_CHANNEL = 'CURRENT'
DLT_PREVIEW_CHANNEL = 'PREVIEW'

logger = get_logger(__name__)


class Job(ABC):

    def __init__(self, job_ctx: JobContext, project_root: str):
        self.job_ctx = job_ctx
        self.project_root = project_root
        self.meta_conf_path = f"Workspace{self.project_root}/config/{self.job_ctx.env}/{self.job_ctx.app}_config.json"
    
    def get_job_type(self) -> JobType:
        return self.get_default_spec().job_type
    
    @staticmethod
    def get_subclasses():
        importlib.import_module('ssot.common.deployer.datawatch')
        importlib.import_module('ssot.common.deployer.dataflow')
        return Job.__subclasses__()
    
    def get_default_spec(self):
        default_spec = self._get_default_spec()
        if self.job_ctx.env != 'prod':
            default_spec.retry_conf = None
        return default_spec
    
    @abstractmethod
    def _get_default_spec(self) -> JobSpec:
        pass


class JobDeployer:

    def __init__(self, job_ctx: JobContext):
        self.job_ctx = job_ctx
        self.client = WorkspaceClient()
        self.job_utils = JobUtils()
        self.pipeline_utils = DLTPipelineUtils()
        self.current_job_id = self.job_utils.get_current_job_details()['job_id']

    def get_common_spark_conf(self, is_single_node_cluster: bool):
        single_node_cluster_conf = {}
        if is_single_node_cluster:
            single_node_cluster_conf = {
                "spark.master" : "local[*]",
                "spark.databricks.cluster.profile" : "singleNode"
            }
        
        return {
            "spark.databricks.hive.metastroe.glueCatalog.enabled" : "true",
            "spark.hadoop.fs.s3a.acl.default" : "BucketOwnerFullControl",
            "spark.databricks.workspace.matplotlibInline.enabled" : "false",
            "spark.databricks.delta.autoCompact.enabled" : "true" ,
            "spark.databricks.delta.optimizeWrite.enabled" : "true" ,
            "spark.hadoop.hive.metastore.glue.catalogid" : self.job_ctx.get_glue_catalog_id(),
            **single_node_cluster_conf
        }
    
    def validate_version(self, version: str):
        if 'snapshot' in version.lower() and self.job_ctx.env == 'prod':
            raise ValueError("Snapshot versions are not supported in prod")
    
    def get_init_scripts(self, version: str) -> typing.List[InitScriptInfo]:
        def get_aws_account(region: str) -> str:
            if region.startswith('us-'):
                aws_account_id = '12345678'
            elif region.startswith('eu-'):
                aws_account_id = '87456132'
        
            return aws_account_id
        
        self.validate_version(version)
        if 'snapshot' in version.lower() :
            scripts_dir = 'snapshot'
        else:
            scripts_dir = 'release'

        aws_account_id = get_aws_account(self.job_ctx.region)

        return [
            InitScriptInfo(
                s3=S3StorageInfo(
                    destination=f"s3://artifact-store-{aws_account_id}/dataflow/init_scripts/{scripts_dir}/{version}/install_jars.sh"
                )
            ),
            InitScriptInfo(
                s3=S3StorageInfo(
                    destination=f"s3://artifact-store-{aws_account_id}/dataflow/init_scripts/{scripts_dir}/{version}/install_pylibs.sh"
                )
            )
        ]

    def get_aws_attributes(self) -> AwsAttributes:
        return AwsAttributes(
            first_on_demand=1,
            availability=AwsAvailability.SPOT_WITH_FALLBACK,
            zone_id='auto',
            instance_profile_arn=self.job_utils.get_current_cluster()['aws_attributes']['instance_profile_arn'],
            spot_bid_price_percent=100,
            ebs_volume_count=0
        )
    
    def get_common_params(self) -> typing.Dict[str, str]:
        return {
            "app" : self.job_ctx.app,
            "env" : self.job_ctx.env,
            "team" : self.job_ctx.team
        }

    def get_tags(self) -> typing.Dict[str, str]:
        return { **self.job_utils.get_tags(self.current_job_id),
                'Function' : self.job_ctx.app,
                'sor' : self.job_ctx.app,
                'SkipCICDTracking' : 'True',
                'map-migrated': 'mig51257'}
    
    def get_job_acls(self) -> typing.List[AccessControlRequest]:
        return [AccessControlRequest.from_dict(acl) for acl in self.job_utils.get_acls(self.current_job_id)]
    
    def get_pipeline_acls(self) -> typing.List[AccessControlRequest]:
        pipeline_acls = []
        for acl in self.get_job_acls():
            if acl.permission_level == PermissionLevel.CAN_MANAGE_RUN:  
                acl.permission_level = PermissionLevel.CAN_RUN
            pipeline_acls.append(acl)
        return pipeline_acls
    
    def get_notify_emails(self) -> typing.optional[typing.List[str]]:
        if self.job_ctx.enc == 'prod':
            curr_job_emails = self.job_utils.get_failure_notification_emails(self.current_job_id)
            onedata_emails = ['on@gmail.com', 
                                'asfs@gmail.com']
            return list({*curr_job_emails,*onedata_emails})
        else:
            return None
    
    def get_absolute_notebook_path(self, relative_entrypoint_path: str, code_version: str):
        self.validate_version(code_version)
        return f"/ssot-dataflow/{code_version}/ssot/{relative_entrypoint_path}"
    
    def get_cluster_log_conf(self) -> ClusterLogConf:
        workspace_name = self.client.config.hostname.split('.')[0]
        if self.job_ctx.region == "us-east-1":
            if 'dev' in workspace_name:
                logging_bucket = "dslogs-datacloud-nonprod-us-east-1"
            elif 'fit' in workspace_name:
                logging_bucket = "dslogs-datacloud-datascience-nonprod-us-east-1"
            else:
                logging_bucket = "dslogs-datacloud-services-us-east-1"
        elif self.job_ctx.region == "eu-central-1":
            if 'dev' in workspace_name:
                logging_bucket = "dslogs-datacloud-emea-nonprod-eu-central-1"
            else:
                logging_bucket = "dslogs-datacloud-emea-prod-eu-central-1"

        logging_path = f"s3://{logging_bucket}/databricks/{workspace_name}"
        return ClusterLogConf(
            s3 = S3StorageInfo(
                destination=logging_path,
                region=self.job_ctx.region,
                enable_encryption=True,
                canned_acl="bucket-owner-full-control"
            )
        )

    @staticmethod
    def is_single_node_cluster(job_spec: JobSpec) -> bool:
        return job_spec.cluster_conf.driver_node_type is None
    
    @staticmethod
    def should_autoscale(job_spec: JobSpec) -> bool:
        return job_spec.cluster_conf.autoscale_conf and not JobDeployer.is_single_node_cluster(job_spec)
    
    def get_spark_conf(self, job_spec: JobSpec):
        return {
            **self.get_common_spark_conf(self.is_single_node_cluster(job_spec)),
            **(job_spec.cluster_conf.custom_spark_conf or {})
        }
    
    def deploy(self, job_spec: JobSpec, flow_grp_id: typing.Optional[int]) -> bool:
        try:
            if job_spec.is_dlt_pipeline:
                self.deploy_pipeline(job_spec, flow_grp_id)
            else:
                self.deploy_job(job_spec)
            return True
        except Exception:
            logger.error(f"Deployment failed for job {job_spec.job_name}. See exception trace below.")
            logger.error(traceback.format_exc())
            return False
    
    def deploy_job(self, job_spec: JobSpec):
        tasks = []
        retry_conf = job_spec.retry_conf
        notify_settings = job_spec.notification_settings
        notify_emails = self.get_notify_emails

        for task in job_spec.tasks:
            tasks.append(
                Task(
                    task_key = task.name,
                    job_cluster_key=JOB_CLUSTER_KEY,
                    notebook_task=NotebookTask(
                        notebook_path=self.get_absolute_notebook_path(task.entrypoint,
                                                                        job_spec.code_version),
                        base_parameters={**self.get_common_params(),**(task.params or {})},
                        source= Source.WORKSPACE
                    ),
                    depends_on=([TaskDependency(task_key=task.depends_on)] if task.dependens_on else None),
                    description=task.description,
                    email_notifications=TaskEmailNotifications(
                        on_failure=notify_emails
                    ) if notify_emails else None,
                    max_retries=retry_conf.max_retries if retry_conf else None,
                    min_retry_interval_millis=retry_conf.min_retry_interval_millis if retry_conf else None,
                    retry_on_timeout=retry_conf.retry_on_timeout if retry_conf else None,
                    notification_settings=job_spec.notification_settings.to_task_notification_settings() if notify_settings else None
                )                
            )
        autoscale_conf = job_spec.cluster_conf.autoscale_conf
        job_req = CreateJob(
            name=job_spec.job_name,
            job_clusters=[JobCluster(
                job_cluster_key=JOB_CLUSTER_KEY,
                new_cluster=ClusterSpec(
                    driver_node_type_id=job_spec.cluster_conf.driver_node_type,
                    node_type_id=job_spec.cluster_conf.worker_node_type,
                    autoscale=(AutoScale(max_workers=autoscale_conf.max_workers,
                                            min_workers=autoscale_conf.min_workers)
                                if self.should_autoscale(job_spec) else None),
                    num_workers=(0 if self.is_single_node_cluster(job_spec) else None),
                    spark_version=job_spec.cluster_conf.spark_version,
                    spark_conf=self.get_spark_conf(job_spec),
                    custom_tags=(
                        {"ResourceClass" : "SingleNode"} if self.is_single_node_cluster(job_spec) else None),
                        enable_elastic_disk = True,
                        init_scripts = self.get_init_scripts(job_spec.code_version),
                        aws_attributes=self.get_aws_attributes(),
                        runtime_engine=RuntimeEngine[job_spec.cluster_conf.runtime_engine],
                        cluster_log_conf = self.get_cluster_log_conf(),
                        data_security_mode = DataSecurityMode.NONE
                    )
                )],
            tasks=tasks,
            max_concurrent_runs=1,
            timeout_seconds=job_spec.job_timeout_seconds,
            format=Format.SINGLE_TASK if (tasks) == 1 else Format.MULTI_TASK,
            schedule=CronSchedule(
                quartz_cron_expression=job_spec.schedule_conf.cron_expr,
                timezone_id=job_spec.schedule_conf.timezone_id,
                pause_status=PauseStatus.UNPAUSED,
                ) if job_spec.schedule_conf else None,
                access_control_list=self.get_job_acls(),
                tags=self.get_tags()
        )

        existing_job_id = self.job_utils.get_job_id(job_req.name)
        if existing_job_id is None:
            logger.info(f"Job with name {job_req.name} doesn't exist, creating it")
            self.client.jobs.create(**job_req.__dict__)
        else:
            logger.info(f"Job with name {job_req.name} already exists, updating it")
            self.client.jobs.reset(job_id=existing_job_id, new_settings=JobSettings.from_dict(job_req.as_dict()))
            self.client.permissions.set(request_object_type='jobs',
                                        request_object_id=existing_job_id,
                                        access_control_list=self.get_job_acls())
    
    def deploy_pipeline(self, job_spec: JobSpec, flow_grp_id: typing.Optional[int]):
        if len(job_spec.tasks) != 1:
            raise ValueError(
                f"Expecting a single task for dlt pipeline '{job_spec.job_name}' "
                f"in {self.job_ctx.get_job_spec_tbl()} table but found {len(job_spec.tasks)} tasks."
            )
        task = job_spec.tasks[0]
        autoscale_conf = job_spec.cluster_conf.autoscale_conf
        dlt_params = dict((f'__[k]', v) for k, v in self.get_common_params().items())
        single_node_tags = {"Resourceclass": "SingleNode"} if self.is_single_node_cluster(job_spec) else {}
        dlt_base_conf = {
            "pipelines.clusterShutdown.delay" : "900s" if self.job_ctx.env in {"dit","fit"} else "0s"
        }
        notify_emails = self.get_notify_emails()

        if job_spec.job_type == JobType.DATAFLOW_BRONZE.name:
            maintenance_cluster = [
                PipelineClusterEnriched(
                    label=PIPELINE_MAINTENANCE_CLUSTER_LABEL,
                    aws_attributes=self.get_aws_attributes(),
                    init_scripts=self.get_init_scripts(job_spec.code_version),
                    cluster_log_conf=self.get_cluster_log_conf(),
                    custom_tags=self.get_tags()
                )
            ]
            storage_dir = self.job_ctx.get_dlt_storage_dir(layer='bronze')
            edition = 'core'
            default_pipeline_channel = DLT_PREVIEW_CHANNEL
        elif job_spec.job_type == JobType.DATAFLOW_SILVER.name:
            maintenance_cluster = []
            storage_dir = self.job_ctx.get_dlt_storage_dir(layer='silver', flow_grp_id=flow_grp_id)
            edition = 'pro'
            dlt_params['__flow_grp_id'] = flow_grp_id
            default_pipeline_channel = DLT_CURRENT_CHANNEL
        else:
            raise ValueError(f"Unsupported pipeline type '{job_spec.job_type}' ")
        
        create_pipe_req = CreatePipeline(
            name = job_spec.job_name,
            clusters=[PipelineClusterEnriched(
                label=PIPELINE_CLUSTER_LABEL,
                num_workers=(0 if self.is_single_node_cluster(job_spec) else None),
                aws_attributes=self.get_aws_attributes(),
                driver_node_type_id=job_spec.cluster_conf.driver_node_type,
                node_type_id=job_spec.cluster_conf.worker_node_type,
                autoscale=(PipelineAutoScale(max_workers=autoscale_conf.max_workers,
                                                min_workers=autoscale_conf.min_workers,
                                                mode=PIPLELINE_SCALING_MODE)
                            if self.should_autoscale(job_spec) else None),
                init_scripts=self.get_init_scripts(job_spec.code_version),
                cluster_log_conf=self.get_cluster_log_conf(),
                custom_tags={**self.get_tags(),**single_node_tags, **dlt_params}
                ), *maintenance_cluster],
            libraries=[PipelineLibrary(notebook=NotebookLibrary(
                    path = self.get_absolute_notebook_path(task.entrypoint, job_spec.code_version)
            ))],
            development=(True if self.job_ctx.env in {"dit", "fit"} else False),
            continuous=job_spec.is_dlt_pipeline_continuous,
            channel=job_spec.dlt_pipeline_channel or default_pipeline_channel,
            edition=edition,
            photon=(True if job_spec.cluster_conf.runtime_engine.upper() == 'PHOTON' else False),
            storage=storage_dir,
            configuration={**dlt_base_conf, **self.get_spark_conf(job_spec)},
            target=self.job_ctx.get_app_spec().delta_db,
            allow_duplicate_names=False,
            notifications=[PipelineNotifications(
                alerts=["on-update-failure", "on-update-fatal-failure"],
                email_recipients=notify_emails
            )] if notify_emails else None
        )
        try:
            existing_pipeline_id = self.pipeline_utils.get_id(name=create_pipe_req.name)
            logger.info(f"Pipeline with name {create_pipe_req.name} already exists, updating it.")
            self.client.pipelines._api.do('PUT',
                                            f'/api/2.0/pipelines/{existing_pipeline_id}',
                                            body=create_pipe_req.as_dict())
        except ValueError as e:
            if 'No DLT pipeline found with name' in str(e):
                logger.info(f"Pipeline with name {create_pipe_req.name} doesnt exist, creating it.")
                existing_pipeline_id = self.client.pipelines.create(**create_pipe_req.__dict__).pipeline_id
            else:
                raise e
        
        self.client.permissions.set(request_object_type='pipelines',
                                    request_object_id=existing_pipeline_id,
                                    access_control_list=self.get_pipeline_acls())         
