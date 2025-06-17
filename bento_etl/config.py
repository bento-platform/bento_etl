from bento_lib.config.pydantic import BentoFastAPIBaseConfig
from fastapi import Depends
from functools import lru_cache
from typing import Annotated
import os

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
    katsu_url: str = os.getenv("KATSU_URL", "http://localhost:8000")
    bento_validate_ssl: bool = os.getenv("BENTO_VALIDATE_SSL", True)
    etl_client_id: str = os.getenv("ETL_CLIENT_ID", "")
    etl_client_secret: str = os.getenv("ETL_CLIENT_SECRET", "")
    openid_config_url: str = os.getenv("BENTO_OPENID_CONFIG_URL", "https://bentov2auth.local/realms/bentov2/.well-known/openid-configuration")

    # Service Configs


@lru_cache
def get_config():
    return Config()


ConfigDependency = Annotated[Config, Depends(get_config)]
