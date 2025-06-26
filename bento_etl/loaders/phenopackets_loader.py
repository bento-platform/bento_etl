from .base import BaseLoader


class PhenopacketsLoader(BaseLoader):
    def __init__(self, logger, config, dataset_id, batch_size):
        super().__init__(logger, config)
        if batch_size < 0:
            raise ValueError("Batch size must be at least 0")
        self.batch_size = batch_size
        self.load_url = (
            f"{self.config.katsu_url}ingest/{dataset_id}/phenopackets_json"
        )

    async def load(self, data:list):
        await self._load(data, self.load_url, self.batch_size)

