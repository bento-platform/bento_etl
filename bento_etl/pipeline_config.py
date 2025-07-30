from typing import Dict
from pydantic import BaseModel
from fastapi import Depends
from typing import Annotated
import yaml
import os
from .config import get_config
from .logger import get_logger

config = get_config()
logger = get_logger(config)

class ExtractorConfig(BaseModel):
    type: str
    endpoint: str
    interval: str

class LoaderConfig(BaseModel):
    data_type: str
    service_name: str

class PipelineConfig(BaseModel):
    extractor: ExtractorConfig
    loader: LoaderConfig

class EtlPipelines(BaseModel):
    pipelines: Dict[str, PipelineConfig]

    @classmethod
    def load_from_yaml(cls) -> "EtlPipelines":
        config_path = os.path.join(os.path.dirname(__file__), "..", "config.yml")
        logger.info(f"Loading pipeline config from {config_path}")

        if not os.path.exists(config_path):
            logger.error(f"Config file not found at {config_path}")
            raise ValueError(f"Config file not found at {config_path}")

        try:
            with open(config_path, "r") as f:
                raw_data = yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to parse config file {config_path}: {e}")
            raise ValueError(f"Failed to parse config file: {e}")

        if not isinstance(raw_data, dict) or "pipelines" not in raw_data:
            logger.error(f"Invalid config structure in {config_path}")
            raise ValueError("Config file must contain a 'pipelines' key")

        return cls(pipelines=raw_data["pipelines"])

def get_pipeline_registry() -> EtlPipelines:
    return EtlPipelines.load_from_yaml()

PipelineRegistryDep = Annotated[EtlPipelines, Depends(get_pipeline_registry)]