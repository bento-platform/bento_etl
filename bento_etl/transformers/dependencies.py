from fastapi import Depends
from typing import Annotated

from bento_etl.logger import LoggerDependency
from bento_etl.models import Job
from bento_etl.transformers.base import BaseTransformer

__all__ = ["get_transformer", "TransformerDep"]


def get_transformer(job: Job, logger: LoggerDependency):
    # returns the appropriate transformer instance depending on the job description
    # TODO: implement dependency injection logic once we have Job model and concrete transformers
    # e.g:
    # if job.transformer.type == "phenopacket.json.pcgl":
    #   return TransformPhenoPCGL(job.transformer)
    if job.transformer.type == "None":
        return None
    else:
        raise NotImplementedError


TransformerDep = Annotated[BaseTransformer, Depends(get_transformer)]
