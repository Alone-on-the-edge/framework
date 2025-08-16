import json

from metadata.config import MetadataConfig

test_json = """{
    "src_db_type" : "oracle",
    "src_dbs" : [
        {
            "host" : "abc.com",
            "port" : 1521,
            "database" : "database1",
            "database_alias" : "db_alias1",
            "schemas" : [
                "schema1", "common"
            ]
        }
    ],
    "table_names" : ["table1","table2","table3","table4","table5","table6","table7","table8","table9","table10","table11"],
    "delta_db" : "test_fit_db",
    "delta_partition_cols" : [ "__schema_id"],
    "tenancy_conf" : { "tenancy_type" : "single"}
}"""

def get_metadata_config(config_path: str, convert_to_lower=False) -> MetadataConfig:
        # with open(config_path, 'rb') as f:
        #     meta_conf = json.load(f)
        #     meta_conf['src_dbs'] = list(map(lambda db_conf: MetadataConfig.DBConfig(**db_conf), meta_conf['src_dbs']))
        #     meta_conf['tenancy_conf'] = MetadataConfig.TenancyConfig(**meta_conf['tenancy_conf'])
        #     meta_conf = MetadataConfig(**meta_conf)
        #     if convert_to_lower:
        #         meta_conf.table_names = list(map(lambda tbl: tbl.lower(), meta_conf.table_names))
        #         for db in meta_conf.src_dbs:
        #             db.schemas = list(map(lambda sch: sch.lower(), db.schemas))

        #     return meta_conf
    meta_conf = json.loads(config_path)
    meta_conf['src_dbs'] = list(map(lambda db_conf: MetadataConfig.DBConfig(**db_conf), meta_conf['src_dbs']))
    meta_conf['tenancy_conf'] = MetadataConfig.TenancyConfig(**meta_conf['tenancy_conf'])
    meta_conf = MetadataConfig(**meta_conf)
    if convert_to_lower:
        meta_conf.table_names = [tbl.lower() for tbl in meta_conf.table_names]
        for db in meta_conf.src_dbs:
            db.schemas = [sch.lower() for sch in db.schemas]
    return meta_conf

result = get_metadata_config(test_json) 
print(result)       

















# from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()

# data = [{"Category": 'A', "ID": 1, "Value": 121.44, "Truth": True},
#         {"Category": 'B', "ID": 2, "Value": 300.01, "Truth": False},
#         {"Category": 'C', "ID": 3, "Value": 10.99, "Truth": None},
#         {"Category": 'E', "ID": 4, "Value": 33.87, "Truth": True}
#         ]

# df = spark.createDataFrame(data)
# type(df)