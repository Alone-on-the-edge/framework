import sys
sys.path.append("d:\Project\ssot")

import typing
from dataclasses import dataclass

from common.control_spec import SUPPORTED_DATABASES

@dataclass
class MetadataConfig:
    @dataclass
    class TenancyConfig:
        tenancy_type: str
        client_id_col: str = None
        handle_l2l: bool = False

        def __post__init__(self):
            self.tenancy_type = self.tenancy_type.lower()
            possible_tenancy_type = ('single','multi')

            if self.tenancy_type not in possible_tenancy_type:
                raise ValueError(f"Expecting tenancy type to be one of {possible_tenancy_type}")
            if self.tenancy_type == 'multi' and self.client_id_col is None:
                raise ValueError(f"Expecting client_id_col when tenancy_type = 'multi'")
            if self.handle_l2l == True:
                raise ValueError(f"Handling L2L migration is currently not supported")
            
    @dataclass
    class DBConfig:
        host: str
        port: int
        database: str
        schemas: typing.List[str] =  None
        database_alias: str = None

        def __post__init__(self):
            self.host = self.host.lower().strip()
            self.database = self.database.lower().strip()
            if self.schemas is not None:
                self.schemas = list(map(lambda sch: sch.strip(), self.schemas))
            
            if self.database_alias is None:
                self.database_alias = self.database

            else:
                self.database_alias = self.database_alias.lower().strip()
    
    src_db_type: str
    src_dbs: typing.List[DBConfig]
    table_names: typing.List[str]
    delta_db: str
    tenancy_conf: TenancyConfig
    delta_partition_cols: typing.List[str] = None
    flows_per_pipeline: int = None
    secret_mode: str = 'multi'
    delta_table_version: int = 1
    thread_pool_size: int = None
    preserve_case: bool = False

    def __post__init(self):
        if self.delta_partition_cols is None:
            self.delta_partition_cols = ['__schema_id']
        
        if self.flows_per_pipeline is None:
            self.flows_per_pipeline = 30

        self.src_db_type = self.src_db_type.lower().strip()
        if self.src_db_type not in SUPPORTED_DATABASES:
            raise ValueError(f"Unsupported database type '{self.src_db_type}'")
        
        if self.secret_mode.lower().strip() not in {'single','multi'}:
            raise ValueError("Excepting secret mode to be single/multi")
        
        if self.thread_pool_size is not None and self.thread_pool_size < 1:
            raise ValueError("Thread pool size must be atleast 1")
        
        if self.preserve_case not in [True, False]:
            raise ValueError("Expecting preserve case to be true/false")
        
        if self.src_db_type == 'mysql':
            for db_config in self.src_dbs:
                if db_config.schemas is None:
                    db_config.schemas = [db_config.database]
                else:
                    raise ValueError("Please do not provide the schemas for mysql database")

        else:
            for db_config in self.src_dbs:
                if db_config.schemas is None:
                    raise ValueError("Expecting schema list")

        self.table_names = list(map(lambda tbl: tbl.strip(), self.table_names))

# import typing
# from dataclasses import dataclass

# from control_spec import SUPPORTED_DATABASES

# @dataclass
# class MetadataConfig:
#     """
#     Class representing metadata configuration for data extraction.
#     """

#     @dataclass
#     class TenancyConfig:
#         """
#         Configuration for tenancy settings.
#         """

#         tenancy_type: str  # Type of tenancy, either 'single' or 'multi'
#         client_id_column: str = None  # Name of the client ID column
#         handle_l2l_migration: bool = False  # Flag indicating whether to handle L2L migration

#         def __post_init__(self):
#             """
#             Post-initialization method to perform additional configuration checks.
#             """

#             # Convert tenancy type to lowercase for consistency
#             self.tenancy_type = self.tenancy_type.lower()

#             # Check if tenancy type is valid
#             possible_tenancy_types = ('single', 'multi')
#             if self.tenancy_type not in possible_tenancy_types:
#                 raise ValueError(f"Expecting tenancy type to be one of {possible_tenancy_types}")

#             # Check if client ID column is provided for multi-tenancy
#             if self.tenancy_type == 'multi' and self.client_id_column is None:
#                 raise ValueError("Expecting client_id_column when tenancy_type is 'multi'")

#             # Check if handling L2L migration is supported
#             if self.handle_l2l_migration:
#                 raise ValueError("Handling L2L migration is currently not supported")

#     @dataclass
#     class DBConfig:
#         """
#         Configuration for database connection.
#         """

#         host: str  # Hostname or IP address of the database server
#         port: int  # Port number of the database server
#         database_name: str  # Name of the database
#         schemas: typing.List[str] = None  # List of schemas within the database
#         alias: str = None  # Alias for the database

#         def __post_init__(self):
#             """
#             Post-initialization method to perform additional configuration.
#             """

#             # Convert host and database name to lowercase and strip whitespace for consistency
#             self.host = self.host.lower().strip()
#             self.database_name = self.database_name.lower().strip()

#             # Strip whitespace from schema names
#             if self.schemas is not None:
#                 self.schemas = [schema.strip() for schema in self.schemas]

#             # Set database alias if not provided
#             if self.alias is None:
#                 self.alias = self.database_name
#             else:
#                 self.alias = self.alias.lower().strip()
    
#     # Below attributes belong to main class MetadataConfig
#     source_database_type: str  # Type of the source database
#     source_databases: typing.List[DBConfig]  # List of source database configurations
#     table_names: typing.List[str]  # List of table names
#     delta_database: str  # Name of the delta database
#     tenancy_configuration: TenancyConfig  # Tenancy configuration
#     delta_partition_columns: typing.List[str] = None  # List of partition columns for delta tables
#     flows_per_pipeline: int = None  # Number of flows per pipeline
#     secret_mode: str = 'multi'  # Secret mode for data extraction
#     delta_table_version: int = 1  # Version of delta tables
#     thread_pool_size: int = None  # Size of the thread pool
#     preserve_case: bool = False  # Flag indicating whether to preserve case

#     def __post_init__(self):
#         """
#         Post-initialization method to perform additional configuration checks.
#         """

#         # Set default values for delta partition columns and flows per pipeline
#         if self.delta_partition_columns is None:
#             self.delta_partition_columns = ['__schema_id']
#         if self.flows_per_pipeline is None:
#             self.flows_per_pipeline = 30

#         # Convert source database type to lowercase and strip whitespace for consistency
#         self.source_database_type = self.source_database_type.lower().strip()

#         # Check if source database type is supported
#         if self.source_database_type not in SUPPORTED_DATABASES:
#             raise ValueError(f"Unsupported database type: {self.source_database_type}")

#         # Check if secret mode is valid
#         if self.secret_mode.lower().strip() not in {'single', 'multi'}:
#             raise ValueError("Expecting secret mode to be 'single' or 'multi'")

#         # Check if thread pool size is valid
#         if self.thread_pool_size is not None and self.thread_pool_size < 1:
#             raise ValueError("Thread pool size must be at least 1")

#         # Check database configuration based on source database type
#         if self.source_database_type == 'mysql':
#             for db_config in self.source_databases:
#                 # If schemas are not provided, set them to the database name
#                 if db_config.schemas is None:
#                     db_config.schemas = [db_config.database_name]
#                 else:
#                     raise ValueError("Please do not provide schemas for MySQL databases")
#         else:
#             for db_config in self.source_databases:
#                 if db_config.schemas is None:
#                     raise ValueError("Expecting a list of schemas")
        
#         # Strip whitespace from table names
#         self.table_names = [table_name.strip() for table_name in self.table_names]




