from fastapi import Depends
from typing import Annotated
from bento_etl.loaders.base import BaseLoader
from bento_etl.loaders.phenopackets_loader import PhenopacketsLoader
from bento_etl.logger import LoggerDependency
from bento_etl.models import Job
from bento_etl.config import ConfigDependency
from bento_etl.pipeline_config import EtlPipelines

__all__ = ["get_loader", "LoaderDep"]

def get_loader(job: Job, logger: LoggerDependency, config: ConfigDependency, registry: EtlPipelines) -> BaseLoader:
    if job.loader.data_type == "phenopackets":
        pipeline_name = job.pipeline_type.value
        pipeline_cfg = registry.pipelines.get(pipeline_name, {})
        service_name = pipeline_cfg.loader.service_name if pipeline_cfg else "katsu"
        return PhenopacketsLoader(
            logger=logger,
            config=config,
            dataset_id=job.loader.dataset_id,
            batch_size=job.loader.batch_size,
            expected_status_code=job.loader.expected_status_code,
            service_name=service_name
        )
    else:
        raise NotImplementedError(f"Loader for data_type '{job.loader.data_type}' not implemented")

LoaderDep = Annotated[BaseLoader, Depends(get_loader)]