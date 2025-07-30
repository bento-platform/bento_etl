from fastapi import Depends
from typing import Annotated

from bento_etl.extractors.base import BaseExtractor, ApiPollExtractor
from bento_etl.logger import LoggerDependency
from bento_etl.models import Job
from bento_etl.pipeline_config import PipelineRegistryDep

__all__ = ["get_extractor", "ExtractorDep"]


""" def get_extractor(job: Job, logger: LoggerDependency) -> BaseExtractor:
    pipeline_name = job.loader.data_type  # e.g., "phenopackets"
    pipeline = PIPELINE_REGISTRY.pipelines.get(pipeline_name) """

def get_extractor(job: Job, logger: LoggerDependency, pipeline_registry: PipelineRegistryDep) -> BaseExtractor:
    pipeline_name = job.loader.data_type
    pipeline = pipeline_registry.pipelines.get(pipeline_name)
    
    if not pipeline:
        logger.error(f"No pipeline configuration found for {pipeline_name}")
        raise ValueError(f"No pipeline configuration for {pipeline_name}")

    extractor_config = pipeline.extractor
    if extractor_config.type == "api-fetch":
        return ApiPollExtractor(
            logger=logger,
            endpoint=extractor_config.endpoint,
            frequency=extractor_config.interval
        )
    
    logger.error(f"Unsupported extractor type: {extractor_config.type}")
    raise ValueError(f"Unsupported extractor type: {extractor_config.type}")

ExtractorDep = Annotated[BaseExtractor, Depends(get_extractor)]