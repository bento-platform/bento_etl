import importlib
import re
from fastapi import Depends, HTTPException, status
from typing import Annotated
from logging import Logger

from bento_etl.logger import LoggerDependency
from bento_etl.models import Job
from bento_etl.transformers.base import BaseTransformer
from bento_etl.config import Config, ConfigDependency

__all__ = ["get_transformer", "TransformerDep"]


def _plugin_name_to_module(plugin_name: str) -> str:
    """
    Convert a plugin string into a Python module path:
    """
    module_name = plugin_name.replace("-", "_")
    return f"bento_etl.transformers.plugins.{module_name}"


def _plugin_name_to_class(plugin_name: str) -> str:
    """
    Convert plugin string (e.g. "phenopacket-v2-json") into a CamelCase class name:
    E.g. "phenopacket-v2-json" → "PhenopacketV2JsonTransformer"
    """
    parts = plugin_name.split("-")
    # Capitalize each part; e.g. ["phenopacket","v2","json"] → ["Phenopacket","V2","Json"]
    camel_parts = [p.capitalize() for p in parts]
    # Join + add "Transformer"
    return "".join(camel_parts) + "Transformer"


def get_transformer(job: Job, logger: LoggerDependency,  config: ConfigDependency) -> BaseTransformer:
    """
    Dynamically load a transformer plugin based on job.transformer.plugin.

    If plugin = "phenopacket-v2-json", we attempt to import:
      bento_etl.transformers.plugins.phenopacket_v2_json.PhenopacketV2JsonTransformer
    If that fails for any reason, fall back to BaseTransformer.
    """
    plugin_name = job.transformer.plugin  # e.g. "phenopacket-v2-json"
    module_path = _plugin_name_to_module(plugin_name)
    class_name = _plugin_name_to_class(plugin_name)

    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Transformer plugin module '{module_path}' not found"
        )

    if not hasattr(module, class_name):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Transformer class '{class_name}' not found in '{module_path}'"
        )

    transformer_cls = getattr(module, class_name)

    try:
        instance = transformer_cls(logger, config)  # instantiate with logger
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to instantiate transformer '{class_name}': {e}"
        )

    return instance


TransformerDep = Annotated[BaseTransformer, Depends(get_transformer)]
