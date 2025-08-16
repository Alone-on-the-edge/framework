import sys
sys.path.append("d:\Project\ssot")

from common.context import JobContext
from databricks import NotebookConfig
from metadata.mysql import MysqlMetadataProvider
from metadata.oracle import OracleMetadataProvider
from metadata.postgres import PostgresMetadataProvider
from metadata.sqlserver import SqlserverMetadataProvider
from metadata.provider import MetadataProvider


def populate_metadata():
    meta_conf = JobContext.get_metadata_config(meta_conf_path, convert_to_lower=False)
    meta_conf.thread_pool_size = thread_pool_size
    provider : MetadataProvider
    if meta_conf.src_db_type == 'oracle':
        provider = OracleMetadataProvider(meta_conf, job_ctx) #It mainly provide jdbc connection, lob cols, oracle metadata tables(dba_tab_colums,dba_constraints etc) to be queried,
        # primary keys and unique indexes
    elif meta_conf.src_db_type == 'postgres':
        provider = PostgresMetadataProvider(meta_conf, job_ctx)
    elif meta_conf.src_db_type == 'mysql':
        provider = MysqlMetadataProvider(meta_conf, job_ctx)
    elif meta_conf.src_db_type == 'sqlserver':
        provider = SqlserverMetadataProvider(meta_conf, job_ctx)
    else:
        raise ValueError(f"Incompatible database type {meta_conf.src_db_type}")
    
    provider.populate_metadata()

if __name__ == "__main__":
    job_ctx = JobContext.from_notebook_config() #Gets app, env , and team. No need to pass args as they are derived in from_notebook_config() method.
    # get_current_notebook_ctx() understand what this methods returns by running it in notebook
    meta_conf_path = NotebookConfig.get_arg('metadata_config_path')  # Gets the path of config file.Here path is not defined as a widget.Hence, calling directly
    thread_pool_size = int(NotebookConfig.get_arg('thread_pool_size',40))
    populate_metadata()
    print(job_ctx)


    # How populate_metadata() works and key functionalites
    # Metadata Extraction:

    # extract_metadata_tables: Extracts metadata from system tables and updates the corresponding Delta tables.

    # Schema Management:
    #     _resolve_meta_table_on_delta: Resolves the metadata table name on Delta.
    #     __update_meta_table_schema: Updates the schema of a Delta table based on the extracted metadata.

    # Table and Column Specifications:
    #     __populate_db_schema_spec: Populates database schema specifications.
    #     __populate_column_spec: Populates column specifications and handles column type conversions.

    # Key Specifications:
    #     __populate_key_spec: Populates primary, unique, and unique index key specifications.
    #     __resolve_table_keys: Resolves table keys and ensures consistency.

    # Schema and Key Resolution:
    #     __resolve_table_schemas: Resolves table schemas by merging column definitions.
    #     get_latest_df: Retrieves the latest metadata DataFrame based on the timestamp.

    # Metadata Population:
    #     populate_metadata: Orchestrates the entire process of extracting, transforming, and loading metadata into Delta tables and updates the application specification table.