from bento_lib.apps.fastapi import BentoFastAPI
from bento_lib.service_info.types import BentoExtraServiceInfo

from . import __version__
from .authz import authz_middleware
from .config import get_config
from .logger import get_logger
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE
from .pipeline_config import EtlPipelines
from .routers.jobs import set_pipeline_registry, job_router

config = get_config()
logger = get_logger(config)

try:
    PIPELINE_REGISTRY = EtlPipelines.load_from_env()
    set_pipeline_registry(PIPELINE_REGISTRY)
    # Log extractor types for debugging
    for pipeline_name, pipeline in PIPELINE_REGISTRY.pipelines.items():
        logger.info(f"Pipeline {pipeline_name} extractor type: {pipeline.extractor.type}")
except ValueError as e:
    logger.critical(f"Failed to load pipeline config: {e}", exc_info=True)
    PIPELINE_REGISTRY = EtlPipelines(pipelines={})
    set_pipeline_registry(PIPELINE_REGISTRY)
    logger.warning("Initialized empty pipeline registry as fallback")

BENTO_SERVICE_INFO: BentoExtraServiceInfo = {
    "serviceKind": BENTO_SERVICE_KIND,
    "dataService": False,
    "workflowProvider": False,
    "gitRepository": "https://github.com/bento-platform/bento_etl",
}

app = BentoFastAPI(
    authz_middleware,
    config,
    logger,
    BENTO_SERVICE_INFO,
    SERVICE_TYPE,
    __version__,
    configure_structlog_access_logger=True,
)

@app.on_event("startup")
async def log_loaded_pipelines():
    pipeline_names = list(PIPELINE_REGISTRY.pipelines.keys())
    logger.info("Loaded pipelines", pipelines=pipeline_names)

app.include_router(job_router)

# Dummy data source router for dev work
if config.bento_debug:
    from .routers.test_sources import test_data_source_router
    app.include_router(test_data_source_router)