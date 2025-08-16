import sys
sys.path.append("d:\Project\ssot")

import inspect

import dataclasses
import itertools
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass, astuple
from datetime import datetime
from enum import Enum

from databricks.sdk.service.jobs import TaskNotificationSettings
from pyspark.sql import SparkSession

from common.constants import GG_ILLEGAL_VS_REPLACED_CHAR

spark = SparkSession.getActiveSession()

SUPPORTED_DATABASES = {'oracle', 'mysql', 'postgres', 'sqlserver'}


class ValidatorMixin(ABC):
    """
    Mixin class to provide validation functionalities for classes.The class inherits from ABC, which stands for Abstract Base Class. 
    This indicates that ValidatorMixin is meant to be used as a base class for other classes and contains abstract methods.
    """

    _validation_errors: typing.List[str] = []

    @abstractmethod
    def validate(self) -> None:
        """
        Abstract method to be implemented for validation.
        """
        pass

    def _format_validation_error(self, validation_error):
        """
        Formats validation error message.
        --type(self) gets the class type of the current object (self).
        --.__name__ retrieves the name of that class as a string

        --type(self) retrieves the class type of the object that self refers to.
        --Essentially, it tells you which class the current object was instantiated from.
        """

        return f"[TABLE={type(self).__name__}] {validation_error}"

    def add_validation_errors(self, validation_errors: typing.Union[typing.Tuple[str], typing.List[str]]):
        """
        Adds validation errors to the list.
        --typing.Union[typing.Tuple[str], typing.List[str]] indicates that validation_errors can be either a:
        --tuple of strings (tuple[str])
        --list of strings (list[str])
        --In python 3.10 : validation_errors: tuple[str] | list[str]
        """
        validation_errors = list(map(lambda ve: self._format_validation_error(ve), validation_errors))
        self._validation_errors.extend(validation_errors)

    def get_validation_errors(self):
        """
        Returns validation errors.
        """
        return tuple(self._validation_errors)

    def check_at_least_one(self, col_name, col_value, context_string):
        """
        Checks if at least one value is present.
        """
        if not len(col_value) > 0:
            self._validation_errors.append(f"Atleast one value expected in '{col_name}' for {context_string} ")

    def is_valid(self):
        """
        Checks if object is valid.
        """
        return len(self._validation_errors) == 0


@dataclass
class Auditable:
    """
    Mixin class providing fields for audit trail.
    The @dataclass decorator is used in Python to automatically generate special methods for managing attributes in a class. 
    It reduces boilerplate code and makes it easier to create classes whose primary purpose is to store data.
    Automatically generates __init__(), __repr__(), and other methods
    """
    created_at: datetime
    updated_at: datetime
    created_by: str
    updated_by: str


@dataclass(init=False) 
class BackwardCompatibleSpec:
    """
    Represents a backward compatible specification.
    @dataclass(init=False) : It initializes its attributes based on the provided kwargs in its __init__ method.
    Example: TargetDetails defined in FlowSpec class.
    details = TargetDetails(
                            delta_table="  MyTable  ",
                            delta_table_path="  /path/to/delta  ",
                            partition_cols=["  COL1  ", "  Col2 "],
                            schema_refresh_done=True
                            )
    Output will be :                            
                    delta_table: " MyTable " -> self.delta_table is set to " MyTable ".
                    delta_table_path: " /path/to/delta " -> self.delta_table_path is set to " /path/to/delta ".
                    partition_cols: [" COL1 ", " Col2 "] -> self.partition_cols is set to [" COL1 ", " Col2 "].
                    schema_refresh_done: True -> self.schema_refresh_done is set to True.                            
    """
    def __init__(self, **kwargs):
      """
      Initializes the object with attributes based on the provided keyword arguments.

      Args:
          **kwargs: Keyword arguments representing attribute names and their corresponding values.

      Returns:
          None

      Example:
          Initialize an object of the class:
          obj = ClassName(attr1=value1, attr2=value2)
      """
      # Retrieve the names of attributes defined in the class
      attribute_names = set([f.name for f in dataclasses.fields(self)])

      # Iterate over the keyword arguments. Whatever values are passed during object creation, they will be mapped as key, value pairs
      for k, v in kwargs.items():
          # Check if the attribute name exists in the set of class attribute names
          if k in attribute_names:
              # Set the attribute value using setattr
              setattr(self, k, v)

    
@dataclass(init=False)
class FlowSpec(BackwardCompatibleSpec, Auditable, ValidatorMixin):
    """
    Represents a flow specification.
    
    It inherits from BackwardCompatibleSpec, Auditable, and ValidatorMixin.
    """

    @dataclass(init=False)
    class TargetDetails(BackwardCompatibleSpec):
        """
        Represents details about the target of the flow.
        """
        delta_table: str
        delta_table_path: str
        partition_cols: typing.List[str]
        schrema_refresh_done: bool

        def __post_init__(self):
            self.delta_table = self.delta_table.lower().strip()
            self.delta_table_path = self.delta_table_path.lower().strip()
            self.partition_cols = list(map(lambda part_col: part_col.lower().strip(), self.partition_cols))

    flow_id: str
    app: str
    src_table: str
    target_details: TargetDetails
    cdc_keys: typing.List[str]
    cdc_schema: str
    cdc_lob_columns: typing.List[str]
    cdc_schema_fingerprints: typing.List[str]
    flow_grp_id: int
    flow_reader_opts: typing.Dict[str, str]
    flow_spark_conf: typing.Dict[str, str]
    inactive_schemas: typing.List[str]
    is_active: bool

    @staticmethod
    def get_delta_table(src_table):
        """
        Returns delta table after replacing illegal characters.
        """
        for illegal_char, replaced_char in GG_ILLEGAL_VS_REPLACED_CHAR.items():
            src_table = src_table.replace(illegal_char.lower(), replaced_char.lower())

        return src_table

    def __post_init__(self):
        """
        Initializes the instance and processes attributes.
        """
        self.flow_id = self.flow_id.lower().strip()
        self.app = self.app.lower().strip()
        self.src_table = self.src_table.lower().strip()
        self.inactive_schemas = list(map(lambda sch: sch.lower().strip(), self.inactive_schemas))

    def validate(self) -> None:
        """
        Validates the flow specification.
        """
        if self.flow_id != f"{self.app}_{self.src_table}":
            self.add_validation_errors([f"Invalid flow_id ='{self.flow_id}' found, flow_id must be equal to {{app}}_{{src_table}}"])

        if FlowSpec.get_delta_table(self.src_table) != self.target_details.delta_table:
            self.add_validation_errors([f"Invalid 'delta_tbl' for flow_id = '{self.src_table}', "
                                        f" src_table must be equal to delta_tbl after replacing '$' with '__DOL' in src_table"])

        at_least_one_fields = {'cdc_keys': self.cdc_keys,
                               'target_details.partition_cols': self.target_details.partition_cols}

        for fld_name, fld_value in at_least_one_fields.items():
            self.check_at_least_one(fld_name, fld_value, f"src_table={self.src_table}")

        for schema in self.inactive_schemas:
            if len(schema.split('.')) != 2:
                self.add_validation_errors([
                    f"Invalid schema_id = '{schema}' in inactive_schemas for src_table '{self.src_table}' "
                    f"Ensure schema_id is <db>.<schema>"
                ])


@dataclass(init=False)
class InitIngestSpec(BackwardCompatibleSpec, Auditable, ValidatorMixin):
    """
    Represents initialization ingest specification.
    """
    app: str
    table_name: str
    schema_id: str
    load_type: str
    init_load_path: str
    is_complete: str
    error_trace: str
    duplicate_count: int

    def __post_init__(self):
        """
        Initializes the instance and processes attributes.
        """
        self.app = self.app.lower().strip()
        self.delta_table = self.table_name.lower().strip()
        self.schema_id = self.schema_id.lower().strip()
        self.load_type = self.load_type.lower().strip()

    def validate(self) -> None:
        """
        Validates the initialization ingest specification.
        """
        if self.load_type not in ['delta', 'dms']:
            self.add_validation_errors([f"load_type value not in 'delta' or 'dms' for the table {self.delta_table}"])


@dataclass(init=False)
class AppSpec(BackwardCompatibleSpec, Auditable, ValidatorMixin):
    """
    Represents application specification.
    """

    @dataclass(init=False)
    class BronzeSparkConf(BackwardCompatibleSpec):
        """
        Represents Spark configuration for bronze layer.
        """
        active_flow_spec: typing.Dict[str, str]
        inactive_flow_spec: typing.Dict[str, str]

    @dataclass(init=False)
    class TenancyConfig(BackwardCompatibleSpec):
        """
        Represents tenancy configuration.
        """
        tenancy_type: str
        client_id_col: str
        handle_l2l: bool = False

    app: str
    src_db_type: str
    delta_db: str
    kafka_conf: typing.Dict[str, str]
    schema_topic: str
    cdc_topics: typing.List[str]
    bronze_reader_opts: typing.Dict[str, str]
    bronze_spark_conf: BronzeSparkConf
    silver_spark_conf: typing.Dict[str, str]
    bronze_table_props: typing.Dict[str, str]
    silver_table_props: typing.Dict[str, str]
    tenancy_conf: TenancyConfig
    flow_specs: typing.List[FlowSpec]
    init_ingest_specs: typing.List[InitIngestSpec]

    def __post_init__(self):
        """
        Initializes the instance and processes attributes.
        """
        self.app = self.app.lower().strip()
        self.src_db_type = self.src_db_type.lower().strip()
        self.src_db_type = self.delta_db.lower().strip()
        self.schema_topic = self.schema_topic.strip()
        self.cdc_topics = list(map(lambda topic: topic.strip(), self.cdc_topics))

    def validate(self) -> None:
        """
        Validates the application specification.
        """
        if self.src_db_type not in SUPPORTED_DATABASES:
            self.add_validation_errors([f"Value in src_db_type must be one one of {','.join(SUPPORTED_DATABASES)}"])

        at_least_one_fields = {'cdc_topics': self.cdc_topics, 'flow_specs': self.flow_specs}
        for fld_name, fld_value in at_least_one_fields.items():
            self.check_at_least_one(fld_name, fld_value, f"app={self.app}")

        flow_vs_specs = itertools.groupby(self.flow_specs, lambda flo_spec: flo_spec.flow_id)
        for flow_id, flow_specs in flow_vs_specs:
            if len(list(flow_specs)) > 1:
                self.add_validation_errors([f"Duplicates for src_table='{flow_id}' found."])

        tbl_path_vs_specs = itertools.groupby(self.flow_specs, lambda flo_spec: flo_spec.target_details.delta_table_path)
        for tbl_path, flow_specs in tbl_path_vs_specs:
            if len(list(flow_specs)) > 1:
                self.add_validation_errors([f"Duplicates for target_details_delta.tbl_path= '{tbl_path}' found."])

        for spec in self.flow_specs:
            spec.validate()
            self.add_validation_errors(spec.get_validation_errors())

        if not self.is_valid():
            validation_errors_str = '\n'.join(self.get_validation_errors())
            raise ValueError(
                f"Input validation failure, see validation error(s) below.\n {validation_errors_str}")


class JobType(str, Enum):
    """
    Enum representing different job types.
    """
    DATAFLOW_METADATA_SETUP = 'dataflow-metadata-setup'
    DATAFLOW_SETUP_INIT_INGEST_SPEC = 'dataflow-setup-init-ingest-spec'
    DATAFLOW_INIT_INGEST = 'dataflow-init-ingest'
    DATAFLOW_SCHEMA_CONTROLLER = 'dataflow-schema-controller'
    DATAFLOW_BRONZE = 'dataflow-bronze'
    DATAFLOW_SILVER = 'dataflow-silver'
    DATAFLOW_SILVER_MAINTENANCE = 'dataflow-silver-maintenance'
    DATAFLOW_ORCHESTRATOR = 'dataflow-orchestrator'
    DATAWATCH_HEARTBEAT_MONITORING = 'datawatch-heartbeat-monitoring'
    DATAWATCH_COUNT_VALIDATION = 'datawatch-count-validation'
    DATAWATCH_REPLICATION_LAG_MONITORING = 'datawatch-replication-lag-monitoring'
    DATAWATCH_REPLICATION_STATS_COLLECTOR = 'datawatch-replication-stats-collector'
    DATAWATCH_DATA_VALIDATION = 'datawatch-data-validation'


@dataclass(init=False)
class JobSpec(BackwardCompatibleSpec):
    """
    Represents a job specification.
    """

    @dataclass(init=False)
    class ClusterConf(BackwardCompatibleSpec):
        """
        Represents cluster configuration.
        """

        @dataclass(init=False)
        class AutoScaleConf(BackwardCompatibleSpec):
            """
            Represents autoscale configuration.
            """
            max_workers: int
            min_workers: int = 1

        driver_node_type: str = None
        worker_node_type: str = None
        spark_version: str = "12.2.x-scala2.12"
        runtime_engine: str = 'STANDARD'
        autoscale_conf: AutoScaleConf = None
        custom_spark_conf: typing.Dict[str, str] = None

    @dataclass(init=False)
    class RetryConf(BackwardCompatibleSpec):
        """
        Represents retry configuration.
        """
        max_retries: int = -1
        min_retry_interval_millis: int = 120000
        retry_on_timeout: bool = True

    @dataclass(init=False)
    class NotificationSettings(BackwardCompatibleSpec):
        """
        Represents notification settings.
        """

        no_alert_for_skipped_runs: bool = True
        no_alert_for_cancelled_runs: bool = False
        alert_on_last_attempt: bool = False

        def to_task_notification_settings(self) -> TaskNotificationSettings:
            """
            Returns TaskNotificationSettings instance.
            """
            return TaskNotificationSettings(
                no_alert_for_skipped_runs=self.no_alert_for_skipped_runs,
                no_alert_for_cancelled_runs=self.no_alert_for_cancelled_runs,
                alert_on_last_attempt=self.alert_on_last_attempt
            )

    @dataclass(init=False)
    class ScheduleConf(BackwardCompatibleSpec):
        """
        Represents schedule configuration.
        """
        cron_expr: str
        timezone_id: str = "America/New_York"

    @dataclass(init=False)
    class TaskConf(BackwardCompatibleSpec):
        """
        Represents task configuration.
        """
        name: str
        description: str
        entrypoint: str
        params: typing.Dict[str, str] = None
        depends_on: str = None

    app: str = None
    job_name: str = None
    job_type: str = None
    is_dlt_pipeline: bool = False
    is_dlt_pipeline_continuous: bool = None
    dlt_pipeline_channel: str = None
    tasks: typing.List[TaskConf] = None
    code_version: str = None
    cluster_conf: ClusterConf = None
    schedule_conf: ScheduleConf = None
    retry_conf: RetryConf = RetryConf()
    notification_settings: NotificationSettings = NotificationSettings()
    job_timeout_seconds: int = None
    created_at: datetime = datetime.now()
    created_by: str = 'JOB_DEPLOYER'
    updated_at: datetime = datetime.now()
    updated_by: str = 'JOB_DEPLOYER'

    def get_job_type(self) -> JobType:
        """
        Returns the job type.
        """
        return JobType[self.job_type]

    def as_tuple(self):
        """
        Returns the instance as tuple.
        """
        return astuple(self)

methods = [member[0] for member in inspect.getmembers(InitIngestSpec) if inspect.isfunction(member[1])]
print(methods)