import typing

from delta import DeltaTable
from pyspark.sql import SparkSession

from ssot.common.context import JobContext
from ssot.common.control_spec import JobType, JobSpec
from ssot.common.databricks1 import NotebookConfig, JobUtils
from ssot.common.deployer.job import Job, JobDeployer
from ssot.common.logger import get_logger
from pyspark.sql import functions as F

INGESTION_JOB_TYPES  = {
    JobType.DATAFLOW_SCHEMA_CONTROLLER,
    JobType.DATAFLOW_BRONZE,
    JobType.DATAFLOW_SILVER,
    JobType.DATAFLOW_SILVER_MAINTENANCE,
    JobType.DATAFLOW_L2L_RECONCILER,
    JobType.DATAFLOW_L2L_DMS
}
logger = get_logger(__name__)


def is_job_of_type(job_sepc: JobSpec, typ: str) -> bool:
    if job_sepc.get_job_type().lower().startswith(typ):
        return True
    else:
        return False
    

def deploy_jobs():
    deployer = JobDeployer(job_ctx)
    existing_job_specs = {job_spec.job_name: job_spec for job_spec in job_ctx.get_job_specs()}
    deployed_jobs = typing.List[JobSpec] = []
    failed_job_names: typing.List[str] = []
    triggered_flow_grp_ids: typing.Set[int] = set()

    for job_cls in Job.get_subclasses():
        job = job_cls(job_ctx, project_root)
        if job.get_job_type() in {JobType.DATAFLOW_L2L_DMS, JobType.DATAFLOW_L2L_SCHEMA_ID_UPDATE}:
            #Do not deploy L2l reconciler if
            #1. app is not present in app_spec
            #2. handle_l2l is not set to True
            try:
                if job_ctx.get_app_spec().tenancy_conf.handle_l2l is not True:
                    continue
            except ValueError as e:
                if f"app '{job_ctx.app}' not found in app_spec table." in str(e).lower():
                    continue
                else:
                    raise e
        
        if job.get_job_type() == JobType.DATAFLOW_SILVER:
            try:
                flow_specs = job_ctx.get_app_spec().flow_specs
                flow_grp_ids = {fs.flow_grp_id for fs in flow_specs}
                triggered_flow_grp_ids = {fs.flow_grp_id for fs in flow_specs
                                          if fs.flow_reader_opts.get('priority','p1').lower().strip() != 'p1'}
                job_name_vs_flow_grp_id = [
                    (job_ctx.get_job_name(job.get_job_type(), flow_grp_id), flow_grp_id)
                    for flow_grp_id in flow_grp_ids
                ]
            except ValueError as e:
                if f"app '{job_ctx.app}' not found in app_spec table." in str(e).lower():
                    job_name_vs_flow_grp_id = []
                else:
                    raise e
        else:
            job_name_vs_flow_grp_id = [(job_ctx.get_job_name(job.get_job_type())), None]

        def do_deploy(job_spec: JobSpec, flow_grp_id: typing.Optional[int]):
            is_success = deployer.deploy(job_spec, flow_grp_id)
            if is_success:
                deployed_jobs.append(job_spec)
            else:
                failed_job_names.append(job_spec.job_name)

        for job_name, flow_grp_id in job_name_vs_flow_grp_id:
            job_spec = existing_job_specs(job_name)

            if not job_spec:
                job_spec = job.get_default_spec()
                job_spec.app = job_ctx.app
                job_spec.job_name = job_name
                job_spec.job_type = job.get_job_type().name
                if flow_grp_id in triggered_flow_grp_ids:
                    job_spec.is_dlt_pipeline_continuous = False
            
            if is_job_of_type(job_spec, 'dataflow'):
                job_spec.code_version = dataflow_version
            if is_job_of_type(job_spec, 'datawatch'):
                job_spec.code_version = datawatch_version

            if deploy_specific_jobs:
                if job_name in deploy_specific_jobs:
                    do_deploy(job_spec, flow_grp_id)
            
            elif is_job_of_type(job_spec, 'datawatch'):
                if deploy_datawatch:
                    do_deploy(job_spec, flow_grp_id=None)
            
            elif job_spec.get_job_type() in INGESTION_JOB_TYPES:
                if deploy_dataflow_ingestion:
                    do_deploy(job_spec, flow_grp_id)
            else:
                if deploy_dataflow_setup:
                    do_deploy(job_spec, flow_grp_id=None)

        job_spec_tbl = job_ctx.get_job_spec_tbl()
        deployed_jobs = [job.as_tuple() for job in deployed_jobs]
        job_spec_df = spark.createDataFrame(deploy_jobs, 
                                            schema = spark.table(job_spec_tbl).schema)
        (
            DeltaTable.forName(spark, job_spec_tbl).alias('m')
            .merge(job_spec_df.alias('u'), f"m.app = u.app AND " 
                                           f"m.job_name = u.job_name AND "
                                           f"m.app = '{job_ctx.app}'")
            .whenNotMatchedInsertAll()
            .whenMatchedUpdate(set = {
                'm.code_version' : F.lit(dataflow_version),
                'm.updated_at' : F.current_timestamp(),
                'm.updated_by' : 'u.updated_by'
            }).execute()
        )

        workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
        job_monitoring_spec_df = (
            job_spec_df
            .where(f"job_type IN ('DATAFLOW_SCHEMA_CONTROLLER', 'DATAFLOW_BRONZE', 'DATAFLOW_SILVER')")
            .SELECT(
                    "app",
                    F.lit("dataflow").alias("ingestion_mode"),
                    "job_name",
                    F.lit(workspace_url).alias("workspace"),
                    "is_dlt_pipeline",
                    F.current_timestamp().alias("created_at"),
                    F.lit("JOB_DEPLOYER").alias("created_by"),
                    F.current_timestamp().alias("updated_at"),
                    F.lit("JOB_DEPLOYER").alias("updated_by")
            )
        )
        (
            DeltaTable
            .forName(spark, job_ctx.get_job_monitoring_spec_tbl())
            .alias('m')
            .merge(job_monitoring_spec_df.alias('u'), f"m.app = u.app AND " 
                                           f"m.ingestion_mode = u.ingestion_mode AND " 
                                           f"m.job_name = u.job_name AND "
                                           f"m.app = '{job_ctx.app}'")
            .whenNotMatchedInsertAll()
            .whenMatchedUpdate(set = {
                'm.workspace' : 'u.workspace',
                'm.updated_at' : F.current_timestamp(),
                'm.updated_by' : 'u.updated_by'
            }).execute()
        )

        if len(failed_job_names) > 0:
            logger.error("Following job(s) have failed during deployment\n")
            logger.error("\n".join(failed_job_names))
            raise ValueError("One or more jobs failed during deployment")
        

def do_deploy_jobs():
    job_utils = JobUtils()
    schema_ctrl_job = job_ctx.get_job_name(JobType.DATAFLOW_SCHEMA_CONTROLLER)
    schema_ctrl_job_id = None
    is_schema_ctrl_job_active = False

    if deploy_dataflow_ingestion or schema_ctrl_job in deploy_specific_jobs:
        schema_ctrl_job_id = job_utils.get_job_id(schema_ctrl_job)
        is_schema_ctrl_job_active = job_utils.is_job_active(schema_ctrl_job_id) if schema_ctrl_job_id else False
        if is_schema_ctrl_job_active:
            logger.info(f"Stopping job {schema_ctrl_job} with id {schema_ctrl_job_id}")
            job_utils.stop_job(schema_ctrl_job_id)

    deploy_jobs()

    if deploy_dataflow_ingestion or schema_ctrl_job in deploy_specific_jobs:
        if is_schema_ctrl_job_active:
            logger.info(f"Starting job {schema_ctrl_job} with id {schema_ctrl_job_id}")
            job_utils.start_job(schema_ctrl_job_id)


if __name__ == "__main__":
    spark = SparkSession.getActiveSession()
    job_ctx = JobContext.from_notebook_config()
    project_root = NotebookConfig.get_arg("project_root")
    dataflow_version = NotebookConfig.get_arg("dataflow_version").strip()
    if dataflow_version == "latest":
        raise ValueError ("dataflow version cannot be latest")
    datawatch_version = NotebookConfig.get_arg("datawatch_version", "latest").strip()

    deploy_dataflow_setup = NotebookConfig.get_bool_arg("deploy_dataflow_setup_jobs", default=True)
    deploy_dataflow_ingestion = NotebookConfig.get_bool_arg("deploy_dataflow_ingestion_jobs", default=False)
    deploy_datawatch = NotebookConfig.get_bool_arg("deploy_datawatch_jobs", default=False)
    deploy_all = NotebookConfig.get_bool_arg("deploy_all_jobs", default=False)
    deploy_specific_jobs = NotebookConfig.get_arg("deploy_specific_jobs", default=None, allow_null_default=True)
    deploy_specific_jobs = {job.strip() for job in deploy_specific_jobs.split(",")} if deploy_specific_jobs else {}
    if deploy_all:
        deploy_dataflow_setup = True
        deploy_dataflow_ingestion = True
        deploy_datawatch = True
        deploy_specific_jobs = {}

    do_deploy_jobs()

