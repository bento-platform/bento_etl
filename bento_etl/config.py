from bento_lib.config.pydantic import BentoFastAPIBaseConfig
from fastapi import Depends
from functools import lru_cache
from typing import Annotated
from pathlib import Path
from pydantic import Field, computed_field

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
    data_dir: Path = Field(
        Path("etl", "data"), validation_alias="BENTO_ETL_INTERNAL_DATA_DIR"
    )
    db_name: str = "bento_etl.db"

    @computed_field(return_type=Path)
    def database_path(self) -> Path:
        return self.data_dir / self.db_name


@lru_cache
def get_config():
    # Load from env (mode_validate prevents pylance errors)
    return Config.model_validate({})


ConfigDependency = Annotated[Config, Depends(get_config)]
