from typing import Any
from logging import Logger
import json

from bento_etl.config import Config
from bento_etl.transformers.base import BaseTransformer
from bento_etl.transformers.api_schema_fetch import ApiSchemaFetch
from .utils import perform_transformation


class ExperimentsTransformer(BaseTransformer):
    def __init__(self, logger: Logger, config: Config):
        super().__init__(logger)
        self.config = config
        self.schema_endpoint = f"{config.katsu_url}api/schemas/experiment"
        self.mappings = (
            json.load(open(config.experiments_mappings_path))
            if hasattr(config, "experiments_mappings_path")
            else {}
        )
        self.ontology_mappings = (
            json.load(open(config.experiments_ontology_mappings_path))
            if hasattr(config, "experiments_ontology_mappings_path")
            else {}
        )

    def transform(self, raw: Any) -> list[dict]:
        schema = self._fetch_schema()
        return perform_transformation(
            raw, schema, self.mappings, self.ontology_mappings, self.logger, schema_type="experiment"
        )

    def _fetch_schema(self) -> dict:
        fetcher = ApiSchemaFetch(self.logger, self.schema_endpoint)
        return fetcher.fetch_schema()