from fastapi import Depends, HTTPException, status
from typing import Annotated
from .base import BaseTransformer
from bento_etl.logger import LoggerDependency
from bento_etl.models import Job

__all__ = ["get_transformer", "TransformerDep"]

def get_transformer(job: Job, logger: LoggerDependency) -> BaseTransformer:
    return BaseTransformer(logger, job.transformer.plugin)

TransformerDep = Annotated[BaseTransformer, Depends(get_transformer)]