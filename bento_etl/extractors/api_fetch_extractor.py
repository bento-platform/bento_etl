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
    ):
        self.endpoint = endpoint
        self.http_verb = http_verb
        self.expected_status_code = expected_status_code
        super().__init__(logger)

    # TODO: deduplication mechanism
    # modified to extract all pages
    def extract(self) -> list[dict]:
        all_records = []
        page = 1
        prev_total_pages = None

        while True:
            response = httpx.request(
                self.http_verb, self.endpoint, params={"currentPage": page}
            )
            if response.status_code != self.expected_status_code:
                error_message = f"API request failed with response: {response}"
                self.logger.error(error_message)
                raise Exception(error_message)

            try:
                data = response.json()
            except Exception as e:
                self.logger.error(e)
                raise

            records = data.get("records", [])
            all_records.extend(records)

            pagination = data.get("pagination", {})
            total_pages = pagination.get("totalPages", 1)

            # Log if total_pages changes (indicating dynamic data)
            if prev_total_pages is not None and total_pages != prev_total_pages:
                self.logger.warning(
                    f"Total pages changed from {prev_total_pages} to {total_pages} at page {page}"
                )
            prev_total_pages = total_pages

            # Break if no more records or reached/exceeded total pages
            if not records or page >= total_pages:
                break

            page += 1

        return all_records
    
