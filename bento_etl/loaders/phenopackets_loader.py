import httpx
import json
import asyncio
from asyncio import Task
from httpx import AsyncClient
from fastapi import status
from .base import BaseLoader

class PhenopacketsLoader(BaseLoader):
    def __init__(self, logger, config, dataset_id, batch_size):
        super().__init__(logger, config)
        self.dataset_id = dataset_id
        self.batch_size = batch_size
        self.load_url = f'http://{self.config.katsu_endpoint}/ingest/{self.dataset_id}/phenopackets_json'

    async def load(self, data:json):
        load_requests = []
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=len(data))

        async with AsyncClient(limits=limits) as client:
            for index in range(0, len(data), self.batch_size):
                batch = data[index:index + self.batch_size]
                request = asyncio.ensure_future(self.send_request(client, self.load_url, batch))
                load_requests.append(request)

            try:
                await asyncio.gather(*load_requests)
            except Exception as ex:
                await self.logger.warning("Cancelling all Phenopacket uploads")
                self.cancel_all_requests(load_requests)
                raise ex


    async def send_request(self, client:AsyncClient, request_url:str, data:json):
        request = await client.post(request_url, json=data)

        if request.status_code != status.HTTP_204_NO_CONTENT:
            error_message = f"Phenopacket upload to Katsu failed with status code {request.status_code}"
            await self.logger.error(error_message)
            raise Exception(error_message)


    def cancel_all_requests(self, requests:list[Task]):
        for request in requests:
            request.cancel()
