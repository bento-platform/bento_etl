from fastapi import Depends
from typing import Annotated

from bento_etl.config import ConfigDependency
from bento_etl.extractors.api_fetch_extractor import ApiPollExtractor
from bento_etl.extractors.s3_extractor import S3Extractor
from bento_etl.extractors.base import BaseExtractor
from bento_etl.logger import LoggerDependency
from bento_etl.models import Job, ApiFetchExtractStep, S3ExtractStep

__all__ = ["get_extractor", "ExtractorDep"]


def get_extractor(
    job: Job, logger: LoggerDependency, config: ConfigDependency
) -> BaseExtractor:
    # returns the appropriate extractor instance depending on the job description
    if isinstance(job.extractor, ApiFetchExtractStep):
        return ApiPollExtractor(
            logger=logger,
            endpoint=job.extractor.extract_url,
            http_verb=job.extractor.http_verb,
            expected_status_code=job.extractor.expected_status_code,
            bearer_token=config.extractor_bearer_token,
        )
    elif isinstance(job.extractor, S3ExtractStep):
        return S3Extractor(logger=logger, config=config, ext_config=job.extractor)
    else:
        raise NotImplementedError


ExtractorDep = Annotated[BaseExtractor, Depends(get_extractor)]
