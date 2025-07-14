from .base import BaseLoader


class PhenopacketsLoader(BaseLoader):
    def __init__(self, logger, config, load_url, service_name, expected_status_code, batch_size):
        if batch_size < 0:
            raise ValueError("Batch size must be at least 0")
        if not load_url:
            raise ValueError("Load URL must be non-empty")
        if not service_name:
            raise ValueError("Service name must be non-empty")
        if not 200 <= expected_status_code <= 299:
            logger.warning(f"Expected status code {expected_status_code} is outside the expected [200-299] range")
        #self.load_url = f"{self.config.katsu_url}ingest/{dataset_id}/phenopackets_json"
        super().__init__(logger, config, load_url, service_name, expected_status_code, batch_size)


    async def load(self, data: list):
        await self._load(data, self.load_url, self.expected_status_code, self.batch_size)
