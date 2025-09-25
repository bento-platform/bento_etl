from bento_lib.config.pydantic import BentoFastAPIBaseConfig
from fastapi import Depends
from functools import lru_cache
from typing import Annotated
from pathlib import Path
from pydantic import Field

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

    # database
    data_dir: Path = Field(Path("etl", "data"), validation_alias="BENTO_ETL_INTERNAL_DATA_DIR")
    db_name: str = "bento_etl.db"
    database_path: Path = data_dir / db_name


@lru_cache
def get_config():
    return Config.model_validate({}) # Load from env (mode_validate prevents pylance errors)


ConfigDependency = Annotated[Config, Depends(get_config)]
