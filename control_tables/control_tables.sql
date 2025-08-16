CREATE TABLE IF NOT EXISTS ${hiveconf:__SSOT_META_DB__}.app_spec(
    app STRING NOT NULL,
    src_db_type STRING NOT NULL,
    delta_db STRING NOT NULL,
    kafka_conf MAP<STRING, STRING> NOT NULL,
    schema_topic STRING NOT NULL,
    cdc_topics ARRAY<STRING> NOT NULL,
    bronze_reader_opts MAP<STRING, STRING> NOT NULL,
    bronze_spark_conf STRUCT<active_flow_conf: MAP<STRING, STRING> NOT NULL,
                           inactive_flow_conf: MAP<STRING, STRING> NOT NULL> NOT NULL,
    silver_spark_conf MAP<STRING, STRING> NOT NULL,
    bronze_table_props MAP<STRING, STRING> NOT NULL,
    silver_table_props MAP<STRING, STRING> NOT NULL,
    tenancy_conf STRUCT<tenancy_type: STRING NOT NULL, client_id_col: STRING, handle_l2l: BOOLEAN NOT NULL> NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    created_by STRING NOT NULL,
    updated_by STRING NOT NULL
) USING DELTA 
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'    
);

CREATE TABLE IF NOT EXISTS ${hiveconf:__SSOT_META_DB__}.__db_schema_spec(
    id STRING NOT NULL,
    app STRING NOT NULL,
    src_db STRING NOT NULL,
    src_schema STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    created_by STRING NOT NULL,
    updated_by STRING NOT NULL
) USING DELTA PARTITIONED BY (app)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'    
);
CREATE TABLE IF NOT EXISTS ${hiveconf:__SSOT_META_DB__}.flow_spec(
    flow_id STRING NOT NULL,
    app STRING NOT NULL,
    src_table STRING NOT NULL,
    target_details STRUCT<delta.table: STRING NOT NULL,
                          delta_table_path: STRING NOT NULL,
                          partition_cols: ARRAY<STRING> NOT NULL,
                          schema_refresh_done: BOOLEAN NOT NULL> NOT NULL,
    cdc_keys ARRAY<STRING> NOT NULL,
    cdc_schema STRING NOT NULL,
    cdc_lob_columns ARRAY<STRING> NOT NULL,
    cdc_schema_fingerprints ARRAY<LONG> NOT NULL,
    flow_grp_id INT, 
    flow_reader_opts MAP<STRING, STRING> NOT NULL,
    flow_spark_conf MAP<STRING, STRING> NOT NULL,
    inactive_schemas ARRAY<STRING> NOT NULL,
    is_active BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    created_by STRING NOT NULL,
    updated_by STRING NOT NULL
)USING DELTA 
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'    
);

CREATE TABLE IF NOT EXISTS ${hiveconf:__SSOT_META_DB__}.init_ingest_spec (
    app STRING NOT NULL,
    table_name STRING NOT NULL,
    schema_id STRING,
    load_type STRING NOT NULL,
    init_load_path STRING NOT NULL,
    is_complete BOOLEAN NOT NULL,
    error_trace STRING,
    duplicate_count INT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    created_by STRING NOT NULL,
    updated_by STRING NOT NULL
)USING DELTA 
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'    
);  

CREATE TABLE IF NOT EXISTS ${hiveconf:__SSOT_META_DB__}.__column_spec (
    app STRING NOT NULL,
    src_schema_id STRING NOT NULL,
    table_name STRING NOT NULL,
    column_name STRING NOT NULL,
    column_type STRING NOT NULL,
    column_length INT,
    column_precision INT,
    column_scale INT,
    column_nullable BOOLEAN,
    column_position INT NOT NULL,
    column_delta_type STRING NOT NULL,
    is_lob_type BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL,
    created_by STRING NOT NULL
)USING DELTA PARTITIONED BY (app)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'    
); 

CREATE TABLE IF NOT EXISTS ${hiveconf:__SSOT_META_DB__}.__key_spec (
    app STRING NOT NULL,
    src_schema_id STRING NOT NULL,
    table_name STRING NOT NULL,
    primary_key_cols ARRAY<STRING>,
    unique_key_cols ARRAY<STRING>,
    unique_index_cols ARRAY<STRING>,
    all_cols ARRAY<STRING> NOT NULL,
    cdc_keys ARRAY<STRING> NOT NULL,
    cdc_keys_type STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    created_by STRING NOT NULL
)USING DELTA PARTITIONED BY (app)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'    
);  
-------
-------
CREATE TABLE IF NOT EXISTS ${hiveconf:__SSOT_META_DB__}.job_spec (
    app STRING NOT NULL,
    job_name STRING NOT NULL,
    job_type STRING NOT NULL,
    is_dlt_pipeline BOOLEAN NOT NULL,
    is_dlt_pipeline_continuous BOOLEAN,
    dlt_pipeline_channel STRING,
    tasks ARRAY<struct <name: STRING, description: STRING, entrypoint: STRING,
                        params: MAP<STRING, STRING>, depends_on: STRING>> NOT NULL,
    code_version STRING NOT NULL,
    cluster_conf STRUCT<driver_node: STRING,
                        worker_node_type: STRING,
                        spark_version: STRING NOT NULL,
                        runtime_engine: STRING NOT NULL,
                        autoscale_conf: STRUCT<max_workers: INT, min_workers: INT>,
                        custom_spark_conf: MAP<STRING, STRING>> NOT NULL,    
    schedule_conf STRUCT<cron_expr: STRING, timezone_id: STRING>,
    retry_conf STRUCT<max_retries: INT, min_retry_interval_millis: LONG, retry_on_timeout: BOOLEAN>,
    notification_settings STRUCT<no_alert_for_skipped_runs: BOOLEAN,
                                 no_alert_for_cancelled_runs: BOOLEAN,
                                 alert_on_last_attempt: BOOLEAN>,
    job_timeout_seconds LONG,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    created_by STRING NOT NULL,
    updated_by STRING NOT NULL
) USING DELTA PARTITIONED BY (app)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'    
);