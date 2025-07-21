from .base import BaseLoader

class PhenopacketsLoader(BaseLoader):
    def __init__(self, logger, config, dataset_id: str, batch_size: int, expected_status_code: int, service_name: str = "katsu"):
        if batch_size < 0:
            raise ValueError("Batch size must be at least 0")
        if not dataset_id:
            raise ValueError("Dataset ID must be non-empty")
        if not service_name:
            raise ValueError("Service name must be non-empty")
        if not 200 <= expected_status_code <= 299:
            logger.warning(f"Expected status code {expected_status_code} is outside the expected [200-299] range")

        # Dynamically construct load_url using config.katsu_url
        load_url = f"{config.katsu_url.rstrip('/')}/ingest/{dataset_id}/phenopackets_json"
        super().__init__(logger, config, load_url, service_name, expected_status_code, batch_size)

    async def load(self, data: list[dict]):
        if not data:
            self.logger.warning("Received empty data for loading")
            return
        await self._load(data)