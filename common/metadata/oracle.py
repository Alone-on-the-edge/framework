import sys
sys.path.append("d:\Project\ssot")

import typing

from pyspark.sql import DataFrame, SparkSession, functions as F

from common.context import JobContext
from common.metadata.provider import MetadataProvider, JDBCOptions, SystemTableSpec, MetadataConfig

spark = SparkSession.getActiveSession()


class OracleMetadataProvider(MetadataProvider):

    def __init__(self, meta_conf: MetadataConfig, job_ctx: JobContext):
        super().__init__(meta_conf, job_ctx)

    @property
    def jdbc_options(self) -> JDBCOptions:
        return JDBCOptions(
            jdbc_driver='oracle.jdbc.driver.OracleDriver',
            jdbc_url_prefix='jdbc:oracle:thin:@//',
            extra_opts={'oracle.jdbc.timezoneAsRegion' : 'false'}
        )
    
    @property
    def system_tables(self) -> typing.List[SystemTableSpec]:
        """
        In Python, the property decorator is used to define properties on classes. Properties allow you to define methods that can be accessed like attributes. 
        In the context of the OracleMetadataProvider class, the property decorator is used to define the system_tables property.
        """
        return [
            SystemTableSpec(
                table_name='dba_tab_columns',
                table_filter_col='table_name',
                schema_filter_col='owner'
            ),
            SystemTableSpec(
                table_name='dba_constraints',
                table_filter_col='table_name',
                schema_filter_col='owner'
            ),
            SystemTableSpec(
                table_name='dba_cons_columns',
                table_filter_col='table_name',
                schema_filter_col='owner'
            ),
            SystemTableSpec(
                table_name='dba_indexes',
                table_filter_col='table_name',
                schema_filter_col='table_owner'
            ),
            SystemTableSpec(
                table_name='dba_ind_columns',
                table_filter_col='table_name',
                schema_filter_col='table_owner'
            )
        ]
    
    # https://help.qlik.com/en-US/replicate/May2023/Content/Replicate/Main/Oracle/ora_source_data_types.htm
    @staticmethod
    def is_lob_type(column_type, column_length) -> bool:
        column_type = column_type.lower().strip()
        if 'char' in column_type and column_length > 4000:
            return True
        if column_type == 'raw' and column_length > 2000:
            return True
        if 'lob' in column_type:
            return True
        if column_type == 'bfile':
            return True
        if column_type in ('longraw','long raw'):
            return True
        if column_type == 'long':
            return True
        if column_type == 'xmltype':
            return True
        return False
    
    @staticmethod
    def get_max_lob_size_column_function(lob_column_name: str, lob_column_type: str) -> str:
        buffer = '1.5'
        lob_column_type = lob_column_type.lower()
        if 'lob' in lob_column_type or 'char' in lob_column_type or lob_column_type == 'bfile':
            return f"MAX(DBMS_LOB.GETLENGTH(XMLTYPE.GETCLOBVAL{lob_column_name})) * {buffer}"
        elif lob_column_type == 'xmltype':
            return f"MAX(DBMS_LOB.GETLENGTH({lob_column_name})) * {buffer}"
        else:
            raise ValueError(f"Unsupported type {lob_column_type} for column name {lob_column_name}")
    
    def get_column_spec_df(self) -> DataFrame:
        (
            self._get_latest_meta_df('dba_tab_columns')
                .withColumn("data_length", F.col("data_length").cast("int"))
                .withColumn("data_precesion", F.col("data_precesion").cast("int"))
                .withColumn("data_scale", F.col("data_scale").cast("int"))
                .createOrReplaceTempView("dba_tab_columns")
        )
        return spark.sql("""
                SELECT 
                         app,
                         src_db,
                         lower(owner) AS src_schema,
                         lower(table_name) AS table_name,
                         column_name,
                         data_type AS column_type,
                         data_length AS column_length,
                         data_precision AS column_precesion,
                         data_scale AS column_scale,
                         CASE WHEN nullable = 'Y' THEN true WHEN nullable = 'N' THEN false END AS column_nullable,
                         CAST(column_id AS int) AS column_position,
                         CASE
                            WHEN data_type IN (
                                'NVARCHAR2',
                                'CHAR',
                                'NCHAR',
                                'VARCHAR',
                                'VARCHAR2',
                                'LONG',
                                'LONG VARCHAR',
                                'CLOB',
                                'NCLOB',
                                'UROWID',
                                'ROWID',
                                'XMLTYPE',
                                'CHAR VARYING',
                                'CHARACTER VARYING',
                                'CHARACTER',
                                'NATIONAL CHAR VARYING',
                                'NATIONAL CHARACTER VARYING',
                                'NATIONAL CHAR',
                                'NATIONAL CHARACTER'
                            ) THEN 'STRING'
                            WHEN data_type like 'INTERVAL%' THEN 'STRING'
                            WHEN data_type = 'NUMBER'
                                AND data_precision IS NOT NULL
                                AND data_scale IS NOT NULL THEN 'DECIMAL(' || data_precision || ',' || data_scale || ')'
                            
                            WHEN data_type = 'NUMBER' AND data_precision IS NULL THEN 'DECIMAL(' || 38 ||',' || 6 || ')'

                            WHEN data_type IN ('DECIMAL','FLOAT') THEN 'DECIMAL(' || 38 ||',' || 6 || ')'

                            WHEN data_type IN (
                                'BINARY_FLOAT',
                                'BINARY_DOUBLE',
                                'REAL',
                                'DOUBLE PRECISION'
                            ) THEN 'DOUBLE'

                            WHEN data_type LIKE 'TIMESTAMP%' THEN 'TIMESTAMP'
                            WHEN data_type = 'DATE' THEN 'TIMESTAMP'
                            WHEN data_type IN ('BLOB','RAW','LONG RAW' 'BFILE') THEN 'BINARY'
                         END AS column_delta_type
                    FROM
                        dba_tab_columns
                """)
    
    def get_primary_keys_df(self) -> DataFrame:
        self._get_latest_meta_df('dba_constraints').createOrReplaceTempView('dba_constraints')
        self._get_latest_meta_df('dba_cons_columns').createOrReplaceTempView('dba_cons_columns')
        return spark.sql("""
                SELECT 
                         app,
                         src_db,
                         lower(owner) AS src_schema,
                         lower(table_name) AS table_name,
                         TRANSFORM(
                            ARRAY_SORT(
                                ARRAY_AGG(STRUCT(b.position, b.column_name))
                            ),
                         col_pos_name -> col_pos_name['column_name']
                         ) AS column_names
                FROM
                    dba_constaraints a
                JOIN dba_cons_columns b ON a.constraint_type = 'P'
                AND a.constraint_name = b.constraint_name
                AND a.table_name = b.table_name
                AND a.owner = b.owner
                AND a.src_db = b.src_db
                AND a.app = b.app
                GROUP BY
                    a.app,
                    a.src_db,
                    a.owner,
                    a.table_name
        """)

    def get_unique_keys_df(self) -> DataFrame:
        self._get_latest_meta_df('dba_constraints').createOrReplaceTempView('dba_constraints')
        self._get_latest_meta_df('dba_cons_columns').createOrReplaceTempView('dba_cons_columns')
        return spark.sql("""
                SELECT 
                    app,
                    src_db,
                    lower(src_schema) AS src_schema,
                    lower(table_name) AS table_name,
                    column_names
                FROM(
                    SELECT
                        app,
                        src_db,
                        src_schema,
                        table_name,
                        column_names,
                        ROW_NUMBER() OVER(
                        PARTITION BY app,
                            src_db,
                            src_schema,
                            table_name
                        ORDER BY
                            SIZE(column_names)
                        ) AS rn
                    FROM(
                        SELECT 
                            a.app,
                            a.src_db,
                            lower(a.owner) AS src_schema,
                            lower(a.table_name) AS table_name,
                            a.constraint_name,
                            TRANSFORM(
                                ARRAY_SORT(
                                    ARRAY_AGG(STRUCT(b.position, b.column_name))
                                ),
                            col_pos_name -> col_pos_name['column_name']
                            ) AS column_names
                        FROM
                            dba_constaraints a
                        JOIN dba_cons_columns b ON a.constraint_type = 'U'
                        AND a.constraint_name = b.constraint_name
                        AND a.table_name = b.table_name
                        AND a.owner = b.owner
                        AND a.src_db = b.src_db
                        AND a.app = b.app
                        GROUP BY
                            a.app,
                            a.src_db,
                            a.owner,
                            a.table_name,
                            a.constraint_name
                    )tmp
                )tmp1
                WHERE 
                    rn = 1
        """)
    
    def get_unique_indexes_df(self) -> DataFrame:
        self._get_latest_meta_df('dba_indexes').createOrReplaceTempView('dba_indexes')
        self._get_latest_meta_df('dba_ind_columns').createOrReplaceTempView('dba_ind_columns')
        return spark.sql("""
                SELECT 
                    app,
                    src_db,
                    lower(src_schema) AS src_schema,
                    lower(table_name) AS table_name,
                    column_names
                FROM(
                    SELECT
                        app,
                        src_db,
                        src_schema,
                        table_name,
                        column_names,
                        ROW_NUMBER() OVER(
                        PARTITION BY app,
                            src_db,
                            src_schema,
                            table_name
                        ORDER BY
                            SIZE(column_names)
                        ) AS rn
                    FROM(
                        SELECT 
                            a.app,
                            a.src_db,
                            lower(a.table_owner) AS src_schema,
                            lower(a.table_name) AS table_name,
                            a.index_name,
                            TRANSFORM(
                                ARRAY_SORT(
                                    ARRAY_AGG(STRUCT(b.column_position, b.column_name))
                                ),
                            col_pos_name -> col_pos_name['column_name']
                            ) AS column_names
                        FROM
                            dba_indexes a
                        JOIN dba_ind_columns b ON a.uniqueness = 'UNIQUE'
                        AND a.index_type = 'NORMAL'
                        AND a.index_name NOT LIKE 'SYS%'
                        AND a.index_name = b.index_name
                        AND a.table_name = b.table_name
                        AND a.table_owner = b.table_owner
                        AND a.src_db = b.src_db
                        AND a.app = b.app
                        GROUP BY
                            a.app,
                            a.src_db,
                            a.table_owner,
                            a.table_name,
                            a.index_name
                    )tmp
                )tmp1
                WHERE 
                    rn = 1
        """)
    
