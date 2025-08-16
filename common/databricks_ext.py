import typing
from dataclasses import dataclass

from databricks.sdk.service.compute import InitScriptInfo, AutoScale
from databricks.sdk.service.pipelines import PipelineCluster


@dataclass
class PipelineClusterEnriched(PipelineCluster):
    init_scripts: typing.Optional[typing.List[InitScriptInfo]] = None

    def as_dict(self):
        body = super().as_dict()
        body['init_scripts'] = list(map(lambda script_info: script_info.as_dict(), self.init_scripts))
        return body
    
@dataclass
class PipelineAutoScale(AutoScale):
    mode: typing.Optional[str] = None

    def as_dict(self) -> dict:
        body = super().as_dict()
        body['mode'] = self.mode
        return body
