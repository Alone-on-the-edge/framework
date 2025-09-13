# Databricks notebook source
from delta import DeltaTable
from pyspark.sql import SparkSession

from common.context import JobContext
from common.databricks1 import NotebookConfig
from script.mongo.common import MongoUtils, delta_table_schema

spark = SparkSession.getActiveSession()


def create_table(table_name: str):
    (
    DeltaTable.create(spark)
    .tableName(table_name)
    .addColumns(delta_table_schema)
    .clusterBy("sgdp_org_id", "event_date")
    .property("delta.enableChangeDataFeed", "true")
    .property("delta.deletedFileRetentionDuration", "4 days")
    .property("delta.logRetentionDuration", "14 days")
    .property("delta.autoOptimize.autoCompact", "auto")
    .property("delta.autoOptimize.optimizeWrite", "true")
    .execute()
    )


if __name__ == "__main__":
    job_ctx = JobContext.from_notebook_config()
    config_path = NotebookConfig.get_arg("mongo_config_path")
    mongo_utils = MongoUtils(config_path, job_ctx)
    mongo_config = mongo_utils.config
    [create_table(f"{mongo_config.delta_db}.{coll.delta_table}") for coll in mongo_config.collections]

