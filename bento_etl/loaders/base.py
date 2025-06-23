from asyncio import Task
import asyncio
import json
from logging import Logger
from fastapi import status
from httpx import AsyncClient
import httpx

from bento_etl.config import Config
from bento_etl import authz


__all__ = ["BaseLoader"]


class BaseLoader:
    """
    Base class for ETL loader implementation.

    Loaders are the final step of an ETL pipeline, they receive transformed data from their upstream
    and load it into the target destination.
    """

    def __init__(self, logger: Logger, config: Config):
        self.logger = logger
        self.config = config

    def load(self, data):
        pass
    
    
    async def _load_json(self, data: json):
        load_requests = []
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=len(data))
        headers = {
            "Authorization": authz.get_bearer_token_from_config(self.config)
        }

        async with AsyncClient(
            limits=limits, verify=self.config.bento_validate_ssl, headers=headers
        ) as client:
            try:
                if self.batch_size == 0:
                    request = asyncio.ensure_future(self._send_json_data(client, data))
                    load_requests.append(request)
                else:
                    load_requests = await self.send_batch_requests(client, data)
                await asyncio.gather(*load_requests)
            except Exception as ex:
                self.logger.warning("Cancelling all uploads")
                self.cancel_all_requests(load_requests)
                raise ex

    async def send_batch_requests(self, client: AsyncClient, data:json) -> list[Task]:
        requests = []
        for index in range(0, len(data), self.batch_size):
            batch = data[index : index + self.batch_size]
            request = asyncio.ensure_future(self._send_json_data(client, batch))
            requests.append(request)
        return requests

    async def _send_json_data(self, client: AsyncClient, data: json):
        response = await client.post(self.load_url, json=data)

        if response.status_code != status.HTTP_204_NO_CONTENT:
            error_message = f"Upload to Katsu failed with status code {response.status_code}"
            self.logger.error(error_message)
            raise Exception(error_message)

    def cancel_all_requests(self, requests: list[Task]):
        for request in requests:
            request.cancel()


# TODO: implement loaders for phenopackets and experiments
