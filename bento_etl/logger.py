from structlog.stdlib import BoundLogger, get_logger as get_struct_logger
from bento_lib.logging.structured.configure import (
    configure_structlog_from_bento_config,
    configure_structlog_uvicorn,
)
from fastapi import Depends
from functools import lru_cache
from typing import Annotated

from .config import ConfigDependency
from .constants import BENTO_SERVICE_KIND

__all__ = [
    "get_logger",
    "LoggerDependency",
]


@lru_cache
def get_logger(config: ConfigDependency) -> BoundLogger:
    configure_structlog_from_bento_config(config)
    configure_structlog_uvicorn()

    return get_struct_logger(f"{BENTO_SERVICE_KIND}.logger")


LoggerDependency = Annotated[BoundLogger, Depends(get_logger)]
