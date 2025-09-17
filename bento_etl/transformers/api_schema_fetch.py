from logging import Logger
import httpx
from bento_etl.transformers.base import BaseTransformer

class ApiSchemaFetch(BaseTransformer):
    def __init__(
        self,
        logger: Logger,
        endpoint: str,
        http_verb: str = "GET",
        expected_status_code=200,
    ):
        self.endpoint = endpoint
        self.http_verb = http_verb
        self.expected_status_code = expected_status_code
        super().__init__(logger)

    def fetch_schema(self) -> dict:
        response = httpx.request(self.http_verb, self.endpoint, verify=False)
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