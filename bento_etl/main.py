from typing import AsyncGenerator
from bento_lib.apps.fastapi import BentoFastAPI
from bento_lib.service_info.types import BentoExtraServiceInfo
from contextlib import asynccontextmanager
from fastapi import FastAPI

from bento_etl.db import get_job_status_db


from . import __version__
from .authz import authz_middleware
from .config import get_config
from .logger import get_logger
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE
from .routers.jobs import job_router

BENTO_SERVICE_INFO: BentoExtraServiceInfo = {
    "serviceKind": BENTO_SERVICE_KIND,
    "dataService": False,
    "workflowProvider": False,
    "gitRepository": "https://github.com/bento-platform/bento_etl",
}

config = get_config()
logger = get_logger()
db = get_job_status_db()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("Starting up database...")
    db.setup()
    yield
    logger.info("Shutting down database...")
    logger.info("Finished shutting down database.")


app = BentoFastAPI(
    authz_middleware,
    config,
    logger,
    BENTO_SERVICE_INFO,
    SERVICE_TYPE,
    __version__,
    configure_structlog_access_logger=True,
    lifespan=lifespan,
)

app.include_router(job_router)

# Dummy data source router for dev work
if config.bento_debug:
    from .routers.test_sources import test_data_source_router

    app.include_router(test_data_source_router)
