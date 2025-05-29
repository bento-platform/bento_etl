import httpx
import json
import asyncio
from fastapi import status
from .base import BaseLoader

class PhenopacketsLoader(BaseLoader):
    def __init__(self, logger, config, dataset_id):
        super().__init__(logger, config)
        self.dataset_id = dataset_id

    async def load(self, data:json):
        katsu_ingest_url = f'http://{self.config.katsu_endpoint}/ingest/{self.dataset_id}/phenopackets_json'
        
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=len(data))

        async with httpx.AsyncClient(limits=limits) as client:
            katsu_requests = [client.post(katsu_ingest_url, json=data[index]) for index in range(len(data))]
            result = await asyncio.gather(*katsu_requests)
            return result