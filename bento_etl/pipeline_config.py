import os
from typing import Dict, Optional
from pydantic import BaseModel
import yaml
from .logger import get_logger
from .config import get_config

config = get_config()
logger = get_logger(config)

class ExtractorConfig(BaseModel):
    type: str
    endpoint: str
    interval: str

class TransformerConfig(BaseModel):
    plugin: str

class LoaderConfig(BaseModel):
    expected_status_code: int = 204
    batch_size: int = 0
    data_type: str
    service_name: Optional[str] = None

class PipelineConfig(BaseModel):
    extractor: ExtractorConfig
    transformer: TransformerConfig
    loader: LoaderConfig

class EtlPipelines(BaseModel):
    pipelines: Dict[str, PipelineConfig]

    @classmethod
    def load_from_env(cls) -> "EtlPipelines":
        config_env = os.getenv("PIPELINE_CONFIG")
        print("config_envconfig_envconfig_env", config_env)
        if config_env:
            config_path = config_env
            logger.info(f"Loading pipeline config from {config_path}")
        else:
            default_path = os.path.join(os.getcwd(), "config.yml")
            logger.info(f"Loading pipeline config from default path: {default_path}")
            config_path = default_path

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