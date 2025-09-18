from logging import Logger
import json

from bento_etl.config import Config
from bento_etl.transformers.base import BaseTransformer
from bento_etl.transformers.api_schema_fetch import ApiSchemaFetch


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

    def transform(self, raw: dict) -> list[dict]:
        try:
            schema = self._fetch_schema().get("properties", {})

        except Exception as e:
            print(f"Error fetching schema: {e}")

        try:
            data = data.get("records", {})
        except Exception as e:
            print(f"Error fetching data: {e}")

        # TODO: Inject logic here to transform raw PCGL JSON into valid experiments
        # using schema for structure validation, mappings for field renaming/population,
        # and ontology_mappings for term standardization (e.g., experimental protocols).
        # Example skeleton:
        # transformed = []
        # for item in raw.get("items", []):
        #     experiment = {"id": item["experiment_id"], ...}  # Map fields per schema
        #     # Apply ontology_mappings to populate terms like assay types
        #     transformed.append(experiment)
        # # Validate against schema if needed
        # return transformed

        raise NotImplementedError("Transformation logic to be injected")

    def _fetch_schema(self) -> dict:
        fetcher = ApiSchemaFetch(self.logger, self.schema_endpoint)
        return fetcher.fetch_schema()
