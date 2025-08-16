import dataclasses
import itertools
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass, astuple
from datetime import datetime
from enum import Enum

# from databricks.sdk.service.jobs import TaskNotificationSettings
# from pyspark.sql import SparkSession

from constants import GG_ILLEGAL_VS_REPLACED_CHAR

# spark = SparkSession.getActiveSession()

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
        """
        return f"[TABLE={type(self).__name__}] {validation_error}"

    def add_validation_errors(self, validation_errors: typing.Union[typing.Tuple[str], typing.List[str]]):
        """
        Adds validation errors to the list.
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
    """

    def __init__(self, **kwargs):
        names = set([f.name for f in dataclasses.fields(self)])
        for k, v in kwargs.items():
            if k in names:
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


# Sample data for the FlowSpec instance
sample_target_details = FlowSpec.TargetDetails(
    delta_table="sample_delta_table",
    delta_table_path="/sample/delta/table/path",
    partition_cols=["col1", "col2"],
    schrema_refresh_done=True
)

sample_flow_reader_opts = {"option1": "value1", "option2": "value2"}
sample_flow_spark_conf = {"spark_option1": "value1", "spark_option2": "value2"}
sample_inactive_schemas = ["schema1", "schema2"]

# Creating a sample instance of FlowSpec
sample_flow_spec = FlowSpec(
    flow_id="sample_flow_id",
    app="sample_app",
    src_table="sample_src_table",
    target_details=sample_target_details,
    cdc_keys=["cdc_key1", "cdc_key2"],
    cdc_schema="sample_cdc_schema",
    cdc_lob_columns=["lob_col1", "lob_col2"],
    cdc_schema_fingerprints=["schema_fingerprint1", "schema_fingerprint2"],
    flow_grp_id=123,
    flow_reader_opts=sample_flow_reader_opts,
    flow_spark_conf=sample_flow_spark_conf,
    inactive_schemas=sample_inactive_schemas,
    is_active=True,
    created_at=datetime.now(),
    updated_at=datetime.now(),
    created_by="user1",
    updated_by="user2"
)


# Instantiate FlowSpec
sample_flow_spec = FlowSpec(
    flow_id="sample_flow",
    app="sample_app",
    src_table="sample_src_table",
    target_details=sample_target_details,  # Assuming sample_target_details is defined
    cdc_keys=["cdc_key1", "cdc_key2"],
    cdc_schema="sample_cdc_schema",
    cdc_lob_columns=["lob_col1", "lob_col2"],
    cdc_schema_fingerprints=["schema_fingerprint1", "schema_fingerprint2"],
    flow_grp_id=123,
    flow_reader_opts=sample_flow_reader_opts,
    flow_spark_conf=sample_flow_spark_conf,
    inactive_schemas=sample_inactive_schemas,
    is_active=True,
    created_at=datetime.now(),
    updated_at=datetime.now(),
    created_by="user1",
    updated_by="user2"
)

# Inspect the instance
print("BackwardCompatibleSpec Attributes:")
print("Flow ID (Inherited):", sample_flow_spec.flow_id)
print("App (Inherited):", sample_flow_spec.app)


# Accessing individual attributes and printing them
# print("Flow ID:", sample_flow_spec.flow_id)
# print("App:", sample_flow_spec.app)
# print("Source Table:", sample_flow_spec.src_table)
# print("Target Details:")
# print("    Delta Table:", sample_flow_spec.target_details.delta_table)
# print("    Delta Table Path:", sample_flow_spec.target_details.delta_table_path)
# print("    Partition Columns:", sample_flow_spec.target_details.partition_cols)
# print("    Schema Refresh Done:", sample_flow_spec.target_details.schrema_refresh_done)
# print("CDC Keys:", sample_flow_spec.cdc_keys)
# print("CDC Schema:", sample_flow_spec.cdc_schema)
# print("CDC LOB Columns:", sample_flow_spec.cdc_lob_columns)
# print("CDC Schema Fingerprints:", sample_flow_spec.cdc_schema_fingerprints)
# print("Flow Group ID:", sample_flow_spec.flow_grp_id)
# print("Flow Reader Options:", sample_flow_spec.flow_reader_opts)
# print("Flow Spark Configuration:", sample_flow_spec.flow_spark_conf)
# print("Inactive Schemas:", sample_flow_spec.inactive_schemas)
# print("Is Active:", sample_flow_spec.is_active)
# print("Created At:", sample_flow_spec.created_at)
# print("Updated At:", sample_flow_spec.updated_at)
# print("Created By:", sample_flow_spec.created_by)
# print("Updated By:", sample_flow_spec.updated_by)

# Or you can print the entire instance
# print(sample_flow_spec)

