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
    katsu_url: str = "http://localhost:8000/"
    etl_client_id: str = ""
    etl_client_secret: str = ""
    bento_openid_config_url: str = ""
    testing: bool = False

    # database
    # TODO: mount to volume, allow test with a PG DB
    db_name: str = "bento_etl.db"

    # Extractor API auth
    # TODO: temp hack to authenticate with PCGL submission service, replace with a generic OIDC service flow later
    extractor_bearer_token: str = None


@lru_cache
def get_config():
    return Config()  # type: ignore


ConfigDependency = Annotated[Config, Depends(get_config)]
