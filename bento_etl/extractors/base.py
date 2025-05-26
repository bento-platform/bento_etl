import httpx
import polars as pl
from logging import Logger
from fastapi import status

__all__ = ["BaseExtractor"]


class BaseExtractor:
    """
    Base class to implement ETL extractors.
    An Extractor is configured to read data from a source (local or remote).

    Concrete extractor implementations should not modify the data obtained from the source,
    this is the Tranformer's job in an ETL pipeline.

    Concrete extractors should be configured in the constructor and implement the `extract` function, which returns
    a Polars DataFrame or a LazyFrame.
    """

    def __init__(self, logger: Logger):
        self.logger = logger

    def extract(self) -> pl.DataFrame | pl.LazyFrame:
        pass


class ApiPollExtractor(BaseExtractor):
    def __init__(self, logger, endpoint: str, frequency: str, http_verb: str = "GET"):
        self.endpoint = endpoint
        self.frequency = frequency
        self.http_verb = http_verb
        super().__init__(logger)

    def extract(self) -> pl.DataFrame:
        df: pl.DataFrame = None
        with httpx.Client() as client:
            r = client.request(self.http_verb, self.endpoint)
            if r.status_code != status.HTTP_200_OK:
                # TODO: more specific exception
                raise Exception()

            # Response implements read() function
            df = pl.read_json(r)

        if df.is_empty():
            self.logger.warning("Extracted payload results in an empty data frame")
            # TODO: more specific exception
            raise Exception()

        return df
