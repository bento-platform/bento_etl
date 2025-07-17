from logging import Logger

from bento_etl.config import Config
from .base import BaseLoader


class PhenopacketsLoader(BaseLoader):
    def __init__(
        self, logger: Logger, config: Config, dataset_id: str, batch_size: int = 0
    ):
        if not dataset_id:
            raise ValueError("Dataset ID must be non-empty")
        load_url = f"{config.katsu_url}ingest/{dataset_id}/phenopackets_json"
        super().__init__(logger, config, load_url, "katsu", 204, batch_size)

    async def load(self, data: list[dict]):
        await self._load(data)
