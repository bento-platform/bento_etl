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

    # Service Configs
    katsu_url: str = "http://localhost:8000/"
    etl_client_id: str = ""
    etl_client_secret: str = ""
    bento_openid_config_url: str = ""

    # Mapping paths phenopackets
    phenopackets_mappings_path: str = os.path.join(
        os.path.dirname(__file__),
        "transformers/plugins/phenopackets/mappings/pcgl_phenopackets_mappings.json",
    )
    phenopackets_ontology_mappings_path: str = os.path.join(
        os.path.dirname(__file__),
        "transformers/plugins/phenopackets/mappings/pcgl_phenopackets_ontology_mappings.json",
    )

    # Mapping paths experimets
    experiments_mappings_path: str = os.path.join(
        os.path.dirname(__file__),
        "transformers/plugins/experiments/mappings/pcgl_experiments_mappings.json",
    )
    experiments_ontology_mappings_path: str = os.path.join(
        os.path.dirname(__file__),
        "transformers/plugins/experiments/mappings/pcgl_experiments_ontology_mappings.json",
    )


@lru_cache
def get_config():
    return Config()


ConfigDependency = Annotated[Config, Depends(get_config)]
