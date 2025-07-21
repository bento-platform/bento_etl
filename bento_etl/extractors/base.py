import httpx
from logging import Logger
from fastapi import status

__all__ = ["BaseExtractor", "ApiPollExtractor"]

class BaseExtractor:
    def __init__(self, logger: Logger):
        self.logger = logger

    def extract(self) -> list[dict]:
        raise NotImplementedError("BaseExtractor.extract() must be overridden.")

class ApiPollExtractor(BaseExtractor):
    def __init__(self, logger: Logger, endpoint: str, frequency: str, http_verb: str = "GET"):
        super().__init__(logger)
        self.endpoint = endpoint
        self.frequency = frequency
        self.http_verb = http_verb
        self._payload = None  # Store JSON payload

    def extract(self) -> list[dict]:
        with httpx.Client() as client:
            try:
                r = client.request(self.http_verb, self.endpoint)
                if r.status_code != status.HTTP_200_OK:
                    self.logger.error(f"HTTP {r.status_code} from {self.endpoint}")
                    raise Exception(f"Failed to fetch data: HTTP {r.status_code}")

                # Parse JSON payload
                self._payload = r.json()
                if not isinstance(self._payload, dict):
                    self.logger.error(f"Expected JSON object with 'records' key from {self.endpoint}, got {type(self._payload)}")
                    raise ValueError("API response must be a JSON object")

                # Extract 'records' field
                records = self._payload.get("records")
                print("cvcv", records)
                if not isinstance(records, list):
                    self.logger.error(f"Expected 'records' to be a JSON array from {self.endpoint}, got {type(records)}")
                    raise ValueError("API response 'records' must be a JSON array")

                # Check for empty records
                if not records:
                    self.logger.warning("Extracted 'records' is empty")
                    raise Exception("Extractor returned empty dataset")

                self.logger.info(f"Extracted {len(records)} items from {self.endpoint}")
                self._payload = records  # Store the records as the payload
                return self._payload

            except Exception as e:
                self.logger.error(f"Extraction failed: {e}")
                raise

    @property
    def payload(self):
        """Return the stored JSON payload."""
        return self._payload