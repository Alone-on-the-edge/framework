import typing

from ssot.common.control_spec import JobSpec, JobType
from ssot.common.deployer.job import Job


class MetadataSetupJob(Job):

    def _get_default_spec(self):
        return JobSpec(
            job_type = JobType.DATAFLOW_METADATA_SETUP,
            tasks = [JobSpec.TaskConf(name = 'metadata-setup',
                                      entrypoint='script/metadata_setup',
                                      params={'metadata_config_path' : self.meta_conf_path},
                                      description = "Responsible for connectiong to source db and extracting primary keys,"
                                      "mapping data types and populating control tables that drive the ingestion.")],
            cluster_conf = JobSpec.ClusterConf(
                driver_node_type='m6g.large',
                worker_node_type='m6g.large',
                autoscale_conf=JobSpec.cluster_conf.AutoScaleConf(max_workers=10)
            ),
            job_timeout_seconds = 3600 * 2,
            retry_conf = None
        )


class SetupInitIngestSpecJob(Job):

    def _get_default_spec(self) -> JobSpec:
        return JobSpec(
                job_type = JobType.DATAFLOW_SETUP_INIT_INGEST_SPEC,
                tasks = [JobSpec.TaskConf(name = 'setup-init-ingest-spec',
                                        entrypoint='script/setup-init-ingest-spec_job',
                                        description = "Responsible for populating init_ingest_spec table with paths pointing to initial load data from dms")],
                cluster_conf = JobSpec.ClusterConf(worker_node_type='m6g.large'),
                job_timeout_seconds = 3600,
                retry_conf = None
            )


class InitIngestJob(Job):

    def _get_default_spec(self) -> JobSpec:
        return JobSpec(
                job_type = JobType.DATAFLOW_INIT_INGEST,
                tasks = [JobSpec.TaskConf(
                                        name = 'init-ingest',
                                        entrypoint='script/run_init-ingest',
                                        description = "Responsible for running initial ingestion and casting all data from dms into cdc_schema defined in flow_spec table"
                                        )],
                cluster_conf = JobSpec.ClusterConf(
                                        driver_node_type='m6g.4xlarge',
                                        worker_node_type='r6g.8xlarge',
                                        autoscale_conf=JobSpec.cluster_conf.AutoScaleConf(max_workers=20)
                ),  
                retry_conf = None
            )
    

class SchemaControllerJob(Job):

    def _get_default_spec(self) -> JobSpec:
        return JobSpec(
                job_type = JobType.DATAFLOW_SCHEMA_CONTROLLER,
                tasks = [
                    JobSpec.TaskConf(name = 'schema-writer', entrypoint='schema_controller/schema_writer',
                                        description = "Responsible for saving schemas from schema topic into a directory inside the landing bucket"),
                    JobSpec.TaskConf(name = 'schema-merger', entrypoint='schema_controller/schema_writer',
                                        description = "Autoloader job that streams schemas from landing bucket and performs a schema merge")
                ],
                cluster_conf = JobSpec.ClusterConf(
                                        driver_node_type='m6g.large',
                                        worker_node_type='c6g.large',
                                        autoscale_conf=JobSpec.cluster_conf.AutoScaleConf(max_workers=30),
                                        spark_version='14.3.x-scala2.12'
                )
            )
    

class BronzePipeline(Job):

    def _get_default_spec(self) -> JobSpec:
        return JobSpec(
                job_type = JobType.DATAFLOW_BRONZE,
                tasks = [JobSpec.TaskConf(name = 'bronze-pipeline', 
                                          entrypoint='dlt/bronze_pipeline',
                                          description = "Responsible for appending cdc events obtained from kafka into a bronze table.")],
                cluster_conf = JobSpec.ClusterConf(
                                        driver_node_type='m5a.large',
                                        worker_node_type='c5a.2xlarge',
                                        autoscale_conf=JobSpec.cluster_conf.AutoScaleConf(max_workers=5),
                                        custom_spark_conf = {
                                            "spark.databricks.eventLog.rolloverIntervalSeconds" : "60",
                                            "spark.databricks.delta.optimizeWrite.enabled" : "true",
                                            "spark.databricks.delta.autoCompact.enabled" : "true"
                                        }
                ),
                is_dlt_pipeline = True,
                is_dlt_pipeline_continuous=(True if self.job_ctx.env == 'prod' else False),
                retry_conf = None
            )   
    

class SilverPipeline(Job):

    def _get_default_spec(self) -> JobSpec:
        return JobSpec(
                job_type = JobType.DATAFLOW_SILVER,
                tasks = [JobSpec.TaskConf(name = 'silver-pipeline', 
                                          entrypoint='dlt/silver_pipeline',
                                          description = "Responsible for merging cdc events from bronze table into silver tables.")],
                cluster_conf = JobSpec.ClusterConf(
                                        driver_node_type='m5a.2xlarge',
                                        worker_node_type='c5ad.4xlarge',
                                        autoscale_conf=JobSpec.cluster_conf.AutoScaleConf(max_workers=10),
                                        custom_spark_conf = {
                                            "spark.databricks.io.cache.enabled" : "true",
                                            "spark.databricks.io.cache.compression.enabled" : "true",
                                            "spark.databricks.io.cache.maxDiskUsage" : "75g",
                                            "spark.databricks.io.cache.maxMetaDataCache" : "1g",
                                            "spark.databricks.delta.optimizeWrite.enabled" : "true",
                                            "spark.databricks.delta.autoCompact.enabled" : "true",
                                            "spark.databricks.eventLog.rolloverIntervalSeconds" : "60",
                                            "spark.sql.shuffle.partitions" : "600",
                                            "spark.sql.adaptive.coalescePartitions.minPartitionSize" : "8MB",
                                            "spark.sql.adaptive.advisoryPartitionSizeInBytes" : "64MB",
                                            "pipelines.applyChanges.tombstoneGCThresholdInSeconds" : "432000s",
                                            "pipelines.applyChanges.tombstoneGCFrequencyInSeconds" : "86400s"
                                        }
                ),
                is_dlt_pipeline = True,
                is_dlt_pipeline_continuous=(True if self.job_ctx.env == 'prod' else False),
                retry_conf = None
            )  


class SilverMaintenanceJob(Job):

    def _get_default_spec(self) -> JobSpec:
        return JobSpec(
                job_type = JobType.DATAFLOW_SILVER_MAINTENANCE,
                tasks = [JobSpec.TaskConf(name = 'silver-maintenance', 
                                          entrypoint='script/dlt_maintenance',
                                          description = "Responsible for running vaccum and zorder with optimize on silver layer tables.")],
                cluster_conf = JobSpec.ClusterConf(
                                        driver_node_type='m6g.2xlarge',
                                        worker_node_type='c6g.2xlarge',
                                        autoscale_conf=JobSpec.cluster_conf.AutoScaleConf(max_workers=10),
                                        spark_version = "11.3.x-scala2.12"
                ),
                schedule_conf=JobSpec.ScheduleConf(cron_expr='0 0 3 * * ?')
            ) 


class OrchestratorJob(Job):

    def _get_default_spec(self) -> JobSpec:
        return JobSpec(
                job_type = JobType.DATAFLOW_ORCHESTRATOR,
                tasks = [JobSpec.TaskConf(name = 'job-orchestrator', 
                                          entrypoint='script/orchestrate_jobs',
                                          params={"run_mode": "start", "completion_timeout_in_hours" : "3"},
                                          description = "Orchestrates dataflow/datawatch jobs and pipelines")],
                cluster_conf = JobSpec.ClusterConf(
                                        worker_node_type='m6g.large',
                                        autoscale_conf=None
                ),
                schedule_conf=(
                    JobSpec.ScheduleConf(cron_expr='26 0 1 * * ?')
                    if self.job_ctx.env != 'prod' else None
                ),
                retry_conf = (
                    JobSpec.RetryConf() if self.job_ctx.env == 'prod' else None
                )
            ) 