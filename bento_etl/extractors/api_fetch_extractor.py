from logging import Logger
from fastapi import status
import httpx

from bento_etl.config import Config
from bento_etl.extractors.base import BaseExtractor


class ApiPollExtractor(BaseExtractor):
    def __init__(self, logger: Logger, config: Config, endpoint: str, frequency: int, http_verb: str = "GET"):
        self.endpoint = endpoint
        self.frequency = frequency
        self.http_verb = http_verb
        super().__init__(logger, config)

    def extract(self) -> dict:
        response = httpx.request(self.http_verb, self.endpoint)

        if response.status_code != status.HTTP_200_OK:  #TODO set expected range of values or custom status?
            error_message = f"API request failed with status {response.status_code}"
            self.logger.error(error_message)
            raise Exception(error_message)

        data = response.json()
        if not data:
            error_message = "Extracted payload is empty"
            self.logger.error(error_message)
            raise Exception(error_message)
        return data
