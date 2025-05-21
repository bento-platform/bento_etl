from fastapi import Depends
from typing import Annotated

from bento_etl.loaders.base import BaseLoader
from bento_etl.logger import LoggerDependency
from bento_etl.models import Job

__all__ = ["get_loader", "LoaderDep"]


def get_loader(job: Job, logger: LoggerDependency):
    # returns the appropriate loader instance depending on the job description
    # TODO: implement dependency injection logic once we have Job model and concrete loaders
    # e.g:
    # if job.loader.type == "phenopacket":
    #   return LoadPhenopackets(job.loader)

    # TODO: should probably raise if dependency cannot be provided from job details
    return BaseLoader(logger)


LoaderDep = Annotated[BaseLoader, Depends(get_loader)]
