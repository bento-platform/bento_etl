from logging import Logger
from fastapi import status
import httpx

from bento_etl.config import Config
from bento_etl.extractors.base import BaseExtractor


class ApiPollExtractor(BaseExtractor):
    def __init__(
        self,
        logger: Logger,
        config: Config,
        endpoint: str,
        frequency: int,
        http_verb: str = "GET",
    ):
        self.endpoint = endpoint
        self.frequency = frequency
        self.http_verb = http_verb
        super().__init__(logger, config)

    def extract(self) -> dict:
        response = httpx.request(self.http_verb, self.endpoint)

        if (
            response.status_code != status.HTTP_200_OK
        ):  # TODO set expected range of values or custom status?
            error_message = f"API request failed with response: {response}"
            self.logger.error(error_message)
            raise Exception(error_message)

        try:
            data = response.json()
        except Exception as e:
            self.logger.error(e)
            raise
        return data
