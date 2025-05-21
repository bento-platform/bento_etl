from fastapi import Depends
from typing import Annotated

from bento_etl.extractors.base import BaseExtractor
from bento_etl.logger import LoggerDependency
from bento_etl.models import Job

__all__ = ["get_extractor", "ExtractorDep"]


def get_extractor(job: Job, logger: LoggerDependency):
    # returns the appropriate extractor instance depending on the job description
    # TODO: implement dependency injection logic once we have Job model and concrete extractors
    # if job.extractor.type == "http_poll":
    #   return HTTPExtractor(job.extractor)

    # TODO: should probably raise if dependency cannot be provided from job details
    return BaseExtractor(logger)


ExtractorDep = Annotated[BaseExtractor, Depends(get_extractor)]
