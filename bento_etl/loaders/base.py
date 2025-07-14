from asyncio.tasks import Task
import asyncio
from logging import Logger
from types import CoroutineType
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

    async def _load(self, data: list[dict], load_url: str, batch_size: int = 0):
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=len(data))
        headers = {"Authorization": authz.get_bearer_token_from_config(self.config)}

        async with AsyncClient(
            limits=limits, verify=self.config.bento_validate_ssl, headers=headers
        ) as client:
            try:
                load_requests = self.generate_requests(client, data, load_url, batch_size)
                await asyncio.gather(*load_requests)
            except Exception:
                self.logger.warning("Cancelling all uploads")
                self._cancel_all_requests(load_requests)
                raise

    def generate_requests(self, client:AsyncClient, data:list[dict], load_url:str, batch_size:int = 0) -> set[Task]:
        load_requests = set()
        batches = data if batch_size == 0 else self._create_data_batches(data, batch_size)

        for batch in batches:
            load_task = asyncio.create_task(self._send_json_data(client, batch, load_url))
            load_requests.add(load_task)
            load_task.add_done_callback(load_requests.discard)

        return load_requests

    def _create_data_batches(self, data: list, batch_size: int) -> list:
        return [
            data[index : index + batch_size]
            for index in range(0, len(data), batch_size)
        ]

    async def _send_json_data(self, client: AsyncClient, data: list, load_url: str):
        response = await client.post(load_url, json=data)

        if response.status_code != status.HTTP_204_NO_CONTENT:
            error_message = (
                f"Upload to Katsu failed with status code {response.status_code}"
            )
            self.logger.error(error_message)
            raise Exception(error_message)

    def _cancel_all_requests(self, requests: set[Task]):
        for request in requests:
            request.cancel()
