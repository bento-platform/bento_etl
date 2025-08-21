from fastapi import Depends
from typing import Annotated

from bento_etl.config import ConfigDependency
from bento_etl.extractors.api_fetch_extractor import ApiPollExtractor
from bento_etl.extractors.base import BaseExtractor
from bento_etl.logger import LoggerDependency
from bento_etl.models import Job

__all__ = ["get_extractor", "ExtractorDep"]


def get_extractor(
    job: Job, logger: LoggerDependency
) -> BaseExtractor:
    # returns the appropriate extractor instance depending on the job description
    if job.extractor.type == "api-fetch":
        return ApiPollExtractor(
            logger=logger,
            endpoint=job.extractor.extract_url,
            frequency=job.extractor.frequency_ms,
        )
    else:
        raise NotImplementedError


ExtractorDep = Annotated[BaseExtractor, Depends(get_extractor)]
