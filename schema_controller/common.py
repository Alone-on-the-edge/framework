import sys
sys.path.append("d:\Project\ssot")

import typing
from datetime import datetime
from enum import Enum

from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, functions as F

from common.context import JobContext
from common.control_spec import FlowSpec
from common.databricks1 import DLTPipelineUtils
from common.utils import to_yyyymm
from common.worker import BackgroundWorker

spark = SparkSession.getActiveSession()


class StreamType(Enum):
    SCHEMA_WRITER = 1
    SCHEMA_MERGER = 2
    TRUNCATE_HANDLER = 3


class StreamingBatchManager:
    def __init__(self, stream_type: StreamType, job_ctx: JobContext):
        self.stream_type = stream_type
        self.job_ctx = job_ctx

    def log_completed_batch(self, streaming_query_id: str, batch_id: str):
        now_ts = datetime.now()
        completed_batches_tbl = self.job_ctx.get_completed_batches_tbl()
        spark.createDataFrame(
            [(self.job_ctx.app, self.stream_type.name, streaming_query_id, batch_id, now_ts, to_yyyymm(now_ts))],
            schema = spark.table(completed_batches_tbl).schema
        ).writeTo(completed_batches_tbl) \
        .partitionedBy(F.col("app"), F.col("stream_name"), F.col("yyyymm")) \
        .append()

    def is_completed_batch(self,streaming_query_id: str, batch_id: str):
        # Check if completed batch exists in the current month's or previous month's partition
        # This is done to handle an edge case where the batch was logged on the last second of the month
        # in that case we check if the batch_id is found in current/previous month
        now_ts = datetime.now()
        current_yyyymm = to_yyyymm(now_ts)
        prev_month_ts = now_ts - relativedelta(months = 1)
        prev_yyyymm = to_yyyymm(prev_month_ts)

        return spark.table(self.job_ctx.get_completed_batches_tbl()) \
                .where (f"""app = '{self.job_ctx.app}' AND stream_name = '{self.stream_type.name}' AND
                        streaming_query_id = '{streaming_query_id}' AND batch_id = '{batch_id}' AND
                        yyyymm >= {prev_yyyymm} AND yyyymm <= {current_yyyymm}""").limit(1).count()> 0
    

class FlowLifeCycleManager:

    def __init__(self, flow_ids: set, job_ctx: JobContext):
        self.job_ctx = job_ctx
        flow_specs:typing.List[FlowSpec] = self.job_ctx.get_app_spec().flow_specs
        flow_grps = [ spec.flow_grp_id for spec in flow_specs if spec.flow_id in flow_ids]
        pipeline_names = [self.job_ctx.get_dlt_pipeline_name(layer='silver', flow_grp_id=grp_id) for grp_id in flow_grps]
        self.dlt_api = DLTPipelineUtils()
        self.pipelines_ids = set(map(lambda name: self.dlt_api.get_id(name), pipeline_names))
        self._pipelines_terminator = typing.Union[BackgroundWorker, None] = None

    def start_pipelines(self):
        self._pipelines_terminator.terminate()
        for pid in self.pipelines_ids:
            self.dlt_api.start(pid)

    def stop_pipelines(self):
        def stop_pipelines_():  # what is the meaning of suffix "_"
            for pid in self.pipelines_ids:
                self.dlt_api.stop(pid)
                
        stop_pipelines_()
        self._pipelines_terminator = BackgroundWorker(function=stop_pipelines_, args = None)
        self._pipelines_terminator.run()
