from asyncio.tasks import Task
import asyncio
from logging import Logger
from httpx import AsyncClient
import httpx

from bento_etl.config import Config
from bento_etl.authz import get_bearer_token_from_config


__all__ = ["BaseLoader"]


class BaseLoader:
    """
    Base class for ETL loader implementation.

    Loaders are the final step of an ETL pipeline, they receive transformed data from their upstream
    and load it into the target destination.
    """

    def __init__(self, logger: Logger, config: Config, load_url:str, service_name:str, expected_status_code:int, batch_size:int = 0):
        if batch_size < 0:
            raise ValueError("Batch size must be at least 0")
        if not load_url:
            raise ValueError("Load URL must be non-empty")
        if not service_name:
            raise ValueError("Service name must be non-empty")
        if not 200 <= expected_status_code <= 299:
            logger.warning(f"Status code {expected_status_code} is outside the expected [200-299] range")

        self.logger = logger
        self.config = config
        self.load_url = load_url
        self.service_name = service_name
        self.expected_status_code = expected_status_code
        self.batch_size = batch_size

    async def _load(self, data: list[dict]):
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=len(data))
        headers = {"Authorization": get_bearer_token_from_config(self.config)}
        load_requests = set()

        async with AsyncClient(
            limits=limits, verify=self.config.bento_validate_ssl, headers=headers
        ) as client:
            try:
                data_batches = data if self.batch_size == 0 else self._create_data_batches(data)

                for batch in data_batches:
                    load_task = asyncio.create_task(self._send_json_data(client, batch))
                    load_requests.add(load_task)
                    load_task.add_done_callback(load_requests.discard)

                await asyncio.gather(*load_requests)
            except Exception:
                self.logger.warning("Cancelling all uploads")
                self._cancel_all_requests(load_requests)
                raise

    def _create_data_batches(self, data: list[dict]) -> list[dict]:
        return [
            data[index : index + self.batch_size]
            for index in range(0, len(data), self.batch_size)
        ]

    async def _send_json_data(self, client: AsyncClient, data: list[dict]):
        response = await client.post(self.load_url, json=data)

        if response.status_code != self.expected_status_code:
            error_message = (
                f"Upload to {self.service_name} failed with status code {response.status_code}"
            )
            self.logger.error(error_message)
            raise Exception(error_message)

    def _cancel_all_requests(self, requests: set[Task]):
        for request in requests:
            request.cancel()
