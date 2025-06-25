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

    async def _load_json(self, data: json, url:str, batch_size:int = 0):
        load_requests = []
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=len(data))
        headers = {
            "Authorization": authz.get_bearer_token_from_config(self.config)
        }

        async with AsyncClient(
            limits=limits, verify=self.config.bento_validate_ssl, headers=headers
        ) as client:
            try:
                if batch_size == 0:
                    load_requests = [asyncio.ensure_future(self._send_json_data(client, data, url))]
                else:
                    batches = self._create_data_batches(data, batch_size)
                    load_requests = [asyncio.ensure_future(self._send_json_data(client, batch, url)) for batch in batches]
                await asyncio.gather(*load_requests)
            except Exception as ex:
                self.logger.warning("Cancelling all uploads")
                self._cancel_all_requests(load_requests)
                raise ex

    def _create_data_batches(self, data, batch_size) -> list:
        return [data[index : index + batch_size] for index in range(0, len(data), batch_size)]

    async def _send_json_data(self, client: AsyncClient, data: json, url:str):
        response = await client.post(url, json=data)

        if response.status_code != status.HTTP_204_NO_CONTENT:
            error_message = f"Upload to Katsu failed with status code {response.status_code}"
            self.logger.error(error_message)
            raise Exception(error_message)

    def _cancel_all_requests(self, requests: list[Task]):
        for request in requests:
            request.cancel()


# TODO: implement loaders for phenopackets and experiments
