import httpx
import json
import asyncio
from asyncio import Task
from httpx import AsyncClient
from fastapi import status

from bento_etl import authz
from .base import BaseLoader


class PhenopacketsLoader(BaseLoader):
    def __init__(self, logger, config, dataset_id, batch_size):
        super().__init__(logger, config)
        if batch_size < 1:
            raise ValueError("Batch size must be at least 1")
        self.dataset_id = dataset_id
        self.batch_size = batch_size
        self.load_url = f"{self.config.katsu_url}ingest/{self.dataset_id}/phenopackets_json"


    async def load(self, data: json):
        load_requests = []
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=len(data))
        headers = {'Authorization': authz.get_bearer_token(self.config.openid_config_url, 
                                                           self.config.etl_client_id, 
                                                           self.config.etl_client_secret, 
                                                           self.config.bento_validate_ssl)}
        async with AsyncClient(limits=limits, 
                               verify=self.config.bento_validate_ssl,
                               headers=headers) as client:
            try:
                for index in range(0, len(data), self.batch_size):
                    batch = data[index : index + self.batch_size]
                    request = asyncio.ensure_future(
                        self.send_request(client, batch)
                    )
                    load_requests.append(request)
                await asyncio.gather(*load_requests)
            except Exception as ex:
                self.logger.warning("Cancelling all Phenopacket uploads")
                self.cancel_all_requests(load_requests)
                raise ex

    async def send_request(self, client: AsyncClient, data: json):
        response = await client.post(self.load_url, json=data)

        if response.status_code != status.HTTP_204_NO_CONTENT:
            error_message = f"Phenopacket upload to Katsu failed with status code {response.status_code}"
            self.logger.error(error_message)
            raise Exception(error_message)

    def cancel_all_requests(self, requests: list[Task]):
        for request in requests:
            request.cancel()
