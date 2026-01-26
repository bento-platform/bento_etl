from logging import Logger
import httpx

from bento_etl.extractors.base import BaseExtractor


class ApiPollExtractor(BaseExtractor):
    def __init__(
        self,
        logger: Logger,
        endpoint: str,
        http_verb: str = "GET",
        expected_status_code=200,
        bearer_token: str = None,
    ):
        self.endpoint = endpoint
        self.http_verb = http_verb
        self.expected_status_code = expected_status_code
        self.bearer_token = bearer_token
        super().__init__(logger)

    def extract(self) -> dict:
        headers = {}
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"

        response = httpx.request(self.http_verb, self.endpoint, headers=headers)

        if response.status_code != self.expected_status_code:
            error_message = f"API request failed with response: {response}"
            self.logger.error(error_message)
            raise Exception(error_message)

        try:
            data = response.json()
        except Exception as e:
            self.logger.error(e)
            raise
        return data
