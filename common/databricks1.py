import sys
sys.path.append("d:\Project\ssot")

import json
import re
import typing
import time
from datetime import timedelta

# from databricks.sdk.service.pipelines import UpdateInfoState
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.pipelines.api import PipelinesApi
from databricks.sdk import WorkspaceClient
from databricks_cli.sdk.api_client import ApiClient

from pyspark.sql import SparkSession
from requests.exceptions import HTTPError

from common.retry import RetryOptions, retry
from common.worker import wait_until

db_retry_opts = RetryOptions(retryable_exception=(HTTPError,))
DLT_UPDATE_ALREADY_EXISTS_ERROR_REGEX = re.compile("An active update [A-Za-z0-9]+ already exists")

def get_db_utils():
    from pyspark.dbutils import DBUtils

    #The get_db_utils() function is designed to retrieve the DBUtils instance from the active Spark session. The dbutils module provides utilities for working with widgets.
    return DBUtils(SparkSession.getActiveSession())

def get_current_notebook_ctx():

    # Retrieves the current notebook context, useful for accessing metadata and parameters.
    return get_db_utils().notebook.entry_point.getDbutils().notebook().getContext()

def get_api_client():
    note_ctx = get_current_notebook_ctx()

    # Creates an API client for interacting with Databricks services using the current notebook context.
    return ApiClient(host=note_ctx.apiUrl().get(), token=note_ctx.apiToken().get())


class NotebookConfig:
    """
    This class(databricks.py) provides set of utilities for managing Databricks jobs and Delta Live Tables (DLT) pipelines. It includes classes and functions to interact with 
    Databricks APIs, retrieve configuration settings, manage job runs, and handle pipeline updates. These utilities are essential for automating workflows and managing 
    resources within Databricks environments.
    """

    @staticmethod
    def get_arg(name: str, default=None, allow_null_default=False) -> str:
        try:
            return get_db_utils().widgets.get(name)
        except Exception as e:
            if 'InputWidgetNotDefined' in str(e):
                if default is not None or allow_null_default:
                    return default
                else:
                    raise ValueError(f"Param '{name}' not found in notebook parameters")
            else:
                raise e
            
    
    @staticmethod
    def get_bool_arg(arg: str, default: bool) -> bool:
        return NotebookConfig.get_arg(arg, str(default).lower()).lower().strip() == "true"
    

class JobUtils:
    """
    JobUtils class is responsible for interacting with Databricks jobs through various API calls. It uses the Databricks CLI Jobs API to manage job
    """
    def __init__(self, api_client=None):
        if api_client:
            self.api_client = api_client
        else:
            self.api_client = get_api_client()
            self.notebook_ctx = get_current_notebook_ctx()
            self.jobs_api = JobsApi(self.api_client)
            self.cluster_api = ClusterApi(self.api_client)
            self.runs_api = RunsApi(self, api_client)

    def get_current_job_details(self):
        curr_job_tags = json.loads(self.notebook_ctx.toJson())['tags']
        curr_job_id = curr_job_tags.get('jobId')
        curr_run_id = curr_job_tags.get('multitaskParentRunId')

        return {'job_id': curr_job_id, 'run_id': curr_run_id}
    
    def get_job_id(self, name: str):
        workspace_jobs_list = self.jobs_api.list_jobs()
        if 'jobs' in workspace_jobs_list:
            for job in workspace_jobs_list['jobs']:
                if job['settings']['name'] == name:

                    return job['job_id']
                
    def wait_until_run_starts(self, run_id: str):
        def is_running_run(run_id: str):
            return self.runs_api.get_run(run_id).get('state',{}).get('life_cycle_state') == "RUNNING"

        def is_job_running(run_ids):
            return all([is_running_run(run_id) for run_id in run_ids])
        
        task_run_ids = []
        run = self.runs_api.get_run(run_id=run_id)
        if run['format'] == "SINGLE_TASK":
            task_run_ids.append(run_id)
        else:
            task_run_ids([task['run_id'] for task in run["tasks"]])

        wait_until(is_job_running, args=[task_run_ids], timeout_seconds= 15 * 60)

    def stop_job(self, job_id:str):
        runs_resp = self.runs_api.list_runs(job_id=job_id, active_only=True,completed_only = None, offset=None, limit=1)

        if 'runs' in runs_resp:
            for run in runs_resp.get('runs',[]):
                run_id = run['run_id']
                if run_id is not None:
                    self.runs_api.cancel_run(run_id=run_id)
                    run_is_cancelled = False
                    while not run_is_cancelled:
                        run_state = self.runs_api.get_run(run_id=run_id)['state'].get('result_state', '').upper()
                        time.sleep(10)
                        if run_state == 'CANCELLED' or run_state == 'FAILED':
                            run_is_cancelled = True
    
    def start_job(self, job_id: str):
        run_now_resp = self.jobs_api.run_now(
            job_id = job_id,
            jar_params = None,
            notebook_params = None,
            python_params = None,
            spark_submit_params = None,
            python_named_params = None
        )

        return run_now_resp['run_id']
    
    def get_current_cluster(self):
        current_cluster_id = json.loads(self.notebook_ctx.toJson())['tags']['clusterId']

        return self.cluster_api.get_cluster(cluster_id = current_cluster_id)
    
    def get_acls(self, job_id:str):
        job_acls = self.api_client.perform_query(method='GET', path=f'/permissions/jobs/{job_id}')['access_control_list']

        acls = []
        for job_acl in job_acls:
            acl = {'permission_level' : job_acl['all_permissions'][0]['permission_level']}
            if 'user_name' in job_acls:
                acl = {'user_name':job_acl['user_name'], **acl}
            elif 'group_name' in job_acl:
                acl = {'group_name':job_acl['group_name'], **acl}
            else:
                raise ValueError("Expecting either user or group while retrieving acls")
            
            acls.append(acl)
        
        return acls
    
    def get_failure_notification_emails(self, job_id: str) -> typing.List[str]:
        job_settings = self.jobs_api.get_job(job_id,version='2.1')['settings']
        job_failure_emails = job_settings.get('email_notifications', {}).get('on_failure', [])
        task_failure_emails = []
        for task in job_settings.get('tasks'):
            task_failure_emails.extend(task.get('email_notifications', {}).get('on_failure', []))

        return list({email.lower().strip() for email in [*job_failure_emails, *task_failure_emails]})
    
    def get_tags(self, job_id:str):
        return self.jobs_api.get_job(job_id = job_id,version='2.1')['settings']['tags']
    
    def is_scheduled_job(self, job_id:str) -> bool:
        return 'schedule' in self.jobs_api.get_job(job_id = job_id,version='2.1')['settings']
    

class DLTPipelineUtils:
    """
    DLTPipelineUtils class is designed to manage Delta Live Tables (DLT) pipelines in Databricks. It provides methods for starting, stopping, and retrieving pipeline information
    """

    def __init__(self, api_client=None):
        if api_client:
            self.api_client = api_client
        else:
            self.api_client = get_api_client()
        self.notebook_ctx = get_current_notebook_ctx()
        self.pipeline_api = PipelinesApi(self.api_client)
        self.sdk_pipeline_api = WorkspaceClient().pipelines

    def start(self, pipeline_id: str, full_refresh=False):
        @retry(retry_options=db_retry_opts)
        def start_():
            try:
                return self.pipeline_api.start_update(pipeline_id=pipeline_id, full_refresh=full_refresh)['update_id']
            except HTTPError as e:
                err_resp = e.response
                err_status_code = err_resp.status_code
                try:
                    err_resp_json = err_resp.json()
                except:
                    err_resp_json = {}
                
                err_resp_code = err_resp_json.get('error_code')
                err_resp_message = err_resp_json.get('message')
                if not (err_status_code == 409 and err_resp_code == 'INVALID_STATE_TRANSITION' and re.match(DLT_UPDATE_ALREADY_EXISTS_ERROR_REGEX, err_resp_message)):
                    raise e
            

            state = self.pipeline_api.get(pipeline_id=pipeline_id['state'])
            if state.upper() != "RUNNING":
                raise ValueError(f"Error occured while starting pipeline {pipeline_id}")


        return start_()
    
    def get_id(self, name:str):
        req_params = {'filter': f"name LIKE '{name}'" , 'max_results' : '2'}
        pipelines_resp = self.pipeline_api.client.client.perform_query('GET', '/pipelines', data=req_params)
        pipelines = pipelines_resp.get('statuses', [])

        if len(pipelines) == 0:
            raise ValueError(f"No DLT pipeline found with name {name}")
        elif len(pipelines) > 1:
            raise ValueError(f"More than one DLT pipelines shouldn't exist with name {name}")
        else:
            return pipelines[0]['pipeline_id']
        
    def get_cluster_tags(self, pipeline_id) -> dict:
        @retry(retry_options=db_retry_opts)
        def get_cluster_tags_(pipeline_id):
            get_resp = self.pipeline_api.get(pipeline_id=pipeline_id)

            return get_resp['spec']['clusters'][0]['custom_tags']
        
        return get_cluster_tags_(pipeline_id)
    
    def get_current_pipeline_id(self) -> str:
        
        return json.loads(self.notebook_ctx.toJson())['tags']['deltapipelinesPipelineId']
    
    def get_arg_from_tags(self,arg_name: str) -> str:
        arg_prefix = '__'
        current_pipeline_id = self.get_current_pipeline_id()
        cluster_tags = self.get_cluster_tags(current_pipeline_id)

        return cluster_tags[f'{arg_prefix}{arg_name}']
    
    def stop(self,pipeline_id: str):
        @retry(retry_options=db_retry_opts)
        def stop_():
            def pipeline_has_stopped():

                return self.pipeline_api.get(pipeline_id=pipeline_id)['state'].upper() == 'IDLE'
            
            self.pipeline_api.stop(pipeline_id=pipeline_id)
            wait_until(pipeline_has_stopped, args=None, timeout_seconds= 20 * 60)
        
        stop_()

    def get_pipeline_state(self, pipeline_id: str):
        
        return self.pipeline_api.get(pipeline_id = pipeline_id)['state'].upper()
    
    def is_continuous_pipeline(self, pipeline_id: str) -> bool:

        return self.pipeline_api.get(pipeline_id = pipeline_id).get('conyonuous',False)
    
    def wait_for_idle_pipeline(self, pipeline_id: str, timeout: timedelta = timedelta(minutes=30)):
        self.sdk_pipeline_api.wait_get_pipeline_idle(pipeline_id, timeout)

    def is_successful_update(self, pipeline_id: str, update_id: str):

        return self.sdk_pipeline_api.get_update(pipeline_id, update_id).update.state == UpdateInfoState.COMPLETED
    
