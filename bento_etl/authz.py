from bento_lib.auth.middleware.fastapi import FastApiAuthMiddleware
from .config import get_config
from .logger import get_logger

__all__ = ["authz_middleware"]

config = get_config()
authz_middleware = FastApiAuthMiddleware.build_from_fastapi_pydantic_config(
    config, get_logger(config)
)
