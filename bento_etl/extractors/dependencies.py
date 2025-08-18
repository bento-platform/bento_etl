from fastapi import Depends
from typing import Annotated

from bento_etl.config import ConfigDependency
from bento_etl.extractors.api_fetch_extractor import ApiPollExtractor
from bento_etl.extractors.base import BaseExtractor
from bento_etl.logger import LoggerDependency
from bento_etl.models import Job
from bento_etl.pipeline_config import PipelineRegistryDep

__all__ = ["get_extractor", "ExtractorDep"]


""" def get_extractor(job: Job, logger: LoggerDependency) -> BaseExtractor:
    pipeline_name = job.loader.data_type  # e.g., "phenopackets"
    pipeline = PIPELINE_REGISTRY.pipelines.get(pipeline_name) 
    , pipeline_registry: PipelineRegistryDep
        #pipeline_name = job.loader.data_type
    #pipeline = pipeline_registry.pipelines.get(pipeline_name)
    
    #if not pipeline:
    #    logger.error(f"No pipeline configuration found for {pipeline_name}")
    #    raise ValueError(f"No pipeline configuration for {pipeline_name}")

    #extractor_config = pipeline.extractor"""

def get_extractor(job: Job, logger: LoggerDependency, config: ConfigDependency) -> BaseExtractor:
    # returns the appropriate extractor instance depending on the job description
    if job.extractor.type == "api-fetch":
        return ApiPollExtractor(
            logger=logger,
            config=config,
            endpoint=job.extractor.extract_url,
            frequency=job.extractor.frequency_ms
        )
    else:
        raise NotImplementedError


ExtractorDep = Annotated[BaseExtractor, Depends(get_extractor)]