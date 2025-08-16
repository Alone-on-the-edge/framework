import sys
sys.path.append("d:\Project\ssot")

import requests
import logging
import json
import uuid
from datetime import datetime
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from dataclasses import dataclass, asdict
from requests.exceptions import HTTPError

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession, functions as F

from ssot.common.types.merge_type import MergedSchema
from ssot.common.context import JobContext
from ssot.common.control_spec import FlowSpec
from ssot.common.databricks1 import get_db_utils
from ssot.common.logger import get_logger
from ssot.common.retry import RetryOptions, retry
from ssot.common.utils import raise_alert

_logger = get_logger(__name__)
spark = SparkSession.getActiveSession()

@dataclass
class DDLImpactedField:
    name: str
    data_type: str

@dataclass
class DDLEvent:
    @dataclass
    class DDLEventDetails:
        table_name : str = None
        action_result : str = "PROCESSED"
        action_type : str = None
        action_info: list[DDLImpactedField] = None

    src_app : str = None
    action_result : str = None
    action_type : str = None
    event: str = "DDL"
    table_name : str = None
    # event_time : str = None
    # details : DDLEventDetails = None

def get_oauth_token(token_url, client_id, client_secret, scope) -> str:
    """
    Generates OAuth token required for authentication in the notification API request
    """
    try:
        logging.debug("Retrieving oauth token")
        client = BackendApplicationClient(client_id=client_id)
        oauth = OAuth2Session(client=client)
        token = oauth.fetch_token(
            token_url= token_url,
            client_id = client_id,
            client_secret=client_secret,
            scope = scope
        )
        return token['access_token']
    except Exception as ex:
        logging.error(f"Error occureed ", exc_info=True)

def log_notification_event(job_ctx: JobContext, ddl_event: DDLEvent, tx_id: str, error_msg: str) -> str:
    """
    Logs the notification metadata to a log table and returns the unique id generated for the log entered
    """
    meta_db = job_ctx.get_control_db()
    # generating a unique id for the notification log
    notification_id = str(uuid.uuid4())

    data = {
        "app" : job_ctx.app,
        "ddl_event" : json.dumps(asdict(ddl_event)),
        "error_msg" : error_msg,
        "transaction_id" : tx_id,
        "notification_id" : notification_id
    }

    schema = StructType(
        [
            StructField("app", StringType()),
            StructField("ddl_event", StringType()),
            StructField("error_msg", StringType(), True),
            StructField("transaction_id", StringType()),
            StructField("notification_id", StringType())
        ]
    )

    notification_log_df = (
        spark.createDataFrame([data], schema)
        .withColumn("created_at", F.current_timestamp())
        .withColumn("created_by", F.current_user())
        .withColumn("yyyymm", F.date_format("created_at", 'yyyMM'))
    )

    notification_log_df.write.mode('append').saveAsTable(f"{meta_db}.ddl_event_notification_logs")

    return notification_id

def get_api_auth_header_dict(job_ctx: JobContext) -> tuple:
    """
    Constructs the header required for the notification api call.
    Returns a tuple with the api header and the error message if any.
    """
    scope = "SGDP/api/notification/v1/event/publish"
    err_msg = None

    try:
        secret_scope = job_ctx.get_secret_scope()
        client_id = get_db_utils().secrets.get(scope=secret_scope, key = 'ddl_alerts_client_id')
        client_secret = get_db_utils().secrets.get(scope=secret_scope, key = 'ddl_alerts_secret')
        x_api_key = get_db_utils().secrets.getBytes(scope=secret_scope, key = "x_api_key_notify_onedata").decode('utf-8')
        token_url = job_ctx.get_notification_api_token_url()

        token = get_oauth_token(token_url, client_id, client_secret, scope)

        headers = {
            "content-type" : "application/json",
            "x-api-key" : x_api_key,
            "Authorization" : "{}".format(token)
        }

    except Exception as e:
        headers = {
            "content-type" : None,
            "x-api-key" : None,
            "Authorization" : None
        }

        err_msg = str(e)

    return (headers, err_msg)

def send_ddl_notification(ddl_event: DDLEvent, job_ctx: JobContext):
    """
    Invokes API call to OneData notification framework to notify of the DDL change.
    """
    env = job_ctx.env

    endpoint_url = job_ctx.get_notification_api_endpoint_url()

    headers, err_msg = get_api_auth_header_dict(job_ctx)

    data = {
        "event_id" : "sgdp-ddl-alerts",
        "event_region" : job_ctx.get_ec2_metadata()["region"],
        "event_summary" : "OneData Maintenance: DDL CHANGE",
        "event_description" :  "OneData Maintenance: DDL CHANGE "+ json.dumps(asdict(ddl_event), indent = 2, separators=(',',": ")),
        "event_payoad" : asdict(ddl_event),
    }
    json_data = json.dumps(data).encode('utf-8')

    try:
        if job_ctx.team == 'sgdp':
            #send out notification only for hub owned ingestion
            response = requests.post(
                url = endpoint_url,
                headers = headers,
                data = json_data,
                verify = False
            )

            response_dict = response.json()
        else:
            response_dict = {}

        txn_id = response_dict.get("tx_id") #tx_id is only present for succeeded requests, failed requests get none

        # response has field name message in some cases and message in other cases.
        if 'message' in response_dict:
            err_msg = response_dict.get('message') if response.status_code != 200 else None
        elif 'Message' in response_dict:
            err_msg = response_dict.get('Message') if response.status_code != 200 else None

        response.raise_for_status()
    except Exception as e:
        txn_id = None
        err_msg = str(e)[:6000]

    return (txn_id, err_msg)

def create_alert(job_ctx: JobContext, ddl_event: DDLEvent, notification_id: str):
    """Constructs an alert message and invokes a function that creates an opsgenie alert"""
    env = job_ctx.env

    alert_msg = f"""{env.upper()} : Error in notifying DDL change. \n
    Payload: {json.dumps(asdict(ddl_event))} \n
    To get error details, query {job_ctx.get_control_db()}.ddl_event_notification_logs
    with notification_id: {notification_id}
    """

    _logger.error(alert_msg)

    raise_alert(msg=alert_msg)

def notify_ddl_event(merged_schema: MergedSchema, flow_id: str, job_ctx: JobContext):
    """
    Analyses all of the DDL changes, constructs payload accordingly and invokes the function to send out notification.
    """
    app = job_ctx.app
    # flow_id is the app_table_name
    # after removing app and replacing illegal characters like $ in the table name, we get the delta table
    delta_table = FlowSpec.get_delta_table(flow_id.replace(f"{app}_",""))

    added_columns = [DDLImpactedField(name=field.name, data_type=str(field.dataType)) for field in merged_schema.merged_schema.fields if field.name in merged_schema.new_fields]
    dropped_columns = [DDLImpactedField(name=field.name, data_type=str(field.dataType)) for field in merged_schema.merged_schema.fields if field.name in merged_schema.missing_fields]
    modified_columns = [DDLImpactedField(name=merged_data_type.field_name, data_type=str(merged_data_type.merged_type)) for merged_data_type in merged_schema.merged_data_types]

    event_time = datetime.now().strftime("%Y%m%dT%H%M")

    if added_columns:
        add_columns_event = DDLEvent(
            src_app=app,
            action_result="PROCESSED",
            action_type= "ADD_COLUMN",
            table_name=delta_table
            # event_time = event_time,
            # details = DDLEvent.DDLEventDetails(
            #         table_name = delta_table, action_type = "ADD_COLUMN", action_info = added_columns
            # )
        )

        tx_id, err_msg = send_ddl_notification(add_columns_event, job_ctx)
        notification_id = log_notification_event(job_ctx, add_columns_event, tx_id, err_msg)

        if err_msg is not None:
            create_alert(job_ctx, add_columns_event, notification_id)

    if modified_columns:
        modified_columns_event = DDLEvent(
            src_app=app,
            action_result="PROCESSED",
            action_type= "MODIFY_COLUMN",
            table_name=delta_table
            # event_time = event_time,
            # details = DDLEvent.DDLEventDetails(
            #         table_name = delta_table, action_type = "MODIFY_COLUMN", action_info = modified_columns
            # )
        )

        tx_id, err_msg = send_ddl_notification(modified_columns_event, job_ctx)
        notification_id = log_notification_event(job_ctx, modified_columns_event, tx_id, err_msg)

        if err_msg is not None:
            create_alert(job_ctx, modified_columns_event, notification_id)

    if dropped_columns:
        dropped_columns_event = DDLEvent(
            src_app=app,
            action_result="IGNORED",
            action_type= "DROP_COLUMN",
            table_name=delta_table
            # event_time = event_time,
            # details = DDLEvent.DDLEventDetails(
            #         table_name = delta_table, action_type = "DROP_COLUMN", action_info =dropped_columns, action_result = 'IGNORED'
            # )
        )

        notification_id = log_notification_event(job_ctx, dropped_columns_event, None, None)
