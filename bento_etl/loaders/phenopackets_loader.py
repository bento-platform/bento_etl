import httpx
import json
import asyncio
from asyncio import Task
from httpx import AsyncClient

from bento_etl import authz
from .base import BaseLoader


class PhenopacketsLoader(BaseLoader):
    def __init__(self, logger, config, dataset_id, batch_size):
        super().__init__(logger, config)
        if batch_size < 0:
            raise ValueError("Batch size must be at least 0")
        self.dataset_id = dataset_id
        self.batch_size = batch_size
        self.load_url = (
            f"{self.config.katsu_url}ingest/{self.dataset_id}/phenopackets_json"
        )
        
    async def load(self, data):
        await self._load_json(data)

