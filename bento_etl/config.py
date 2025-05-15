from bento_lib.config.pydantic import BentoFastAPIBaseConfig
from fastapi import Depends
from functools import lru_cache
from typing import Annotated

from .constants import SERVICE_GROUP, SERVICE_ARTIFACT

__all__ = [
    "Config",
    "get_config",
    "ConfigDependency",
]


class Config(BentoFastAPIBaseConfig):
    # Service Info
    service_id: str = f"{SERVICE_GROUP}:{SERVICE_ARTIFACT}"
    service_name: str = "Bento ETL Service"
    service_description: str = (
        "ETL service for external data ingestion into the Bento platform"
    )

    # Service Configs


@lru_cache
def get_config():
    return Config()


ConfigDependency = Annotated[Config, Depends(get_config)]
