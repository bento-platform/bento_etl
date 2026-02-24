from fastapi import Depends
from typing import Annotated

from bento_etl.loaders.base import BaseLoader
from bento_etl.loaders.experiments_loader import ExperimentsLoader
from bento_etl.loaders.phenopackets_loader import PhenopacketsLoader
from bento_etl.loaders.print_loader import PrintLoader
from bento_etl.logger import LoggerDependency
from bento_etl.models import Job
from bento_etl.config import ConfigDependency

__all__ = ["get_loader", "LoaderDep"]


def get_loader(job: Job, logger: LoggerDependency, config: ConfigDependency):
    # returns the appropriate loader instance depending on the job description
    if job.loader.data_type == "phenopackets":
        return PhenopacketsLoader(
            logger, config, job.loader.dataset_id, job.loader.batch_size
        )
    elif job.loader.data_type == "experiments":
        return ExperimentsLoader(
            logger, config, job.loader.dataset_id, job.loader.batch_size
        )
    elif job.loader.data_type == "print":
        return PrintLoader(logger, config)
    else:
        raise NotImplementedError


LoaderDep = Annotated[BaseLoader, Depends(get_loader)]
