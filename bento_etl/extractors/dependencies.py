from fastapi import Depends
from typing import Annotated
from bento_etl.extractors.base import BaseExtractor, ApiPollExtractor
from bento_etl.logger import LoggerDependency
from bento_etl.models import Job

__all__ = ["get_extractor", "ExtractorDep"]

def get_extractor(job: Job, logger: LoggerDependency) -> BaseExtractor:
    if job.extractor.type == "api-fetch":
        # Convert endpoint to string to match ApiPollExtractor expectation
        endpoint = str(job.extractor.endpoint)
        return ApiPollExtractor(
            logger=logger,
            endpoint=endpoint,
            frequency=job.extractor.interval,
            http_verb="GET"
        )
    else:
        logger.error(f"Unknown extractor type: {job.extractor.type}")
        raise NotImplementedError(f"Extractor for type '{job.extractor.type}' not implemented")

ExtractorDep = Annotated[BaseExtractor, Depends(get_extractor)]