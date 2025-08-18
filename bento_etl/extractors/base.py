from logging import Logger
from bento_etl.config import Config

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

    def __init__(self, logger: Logger, config: Config):
        self.logger = logger
        self.config = config

    def extract(self) -> dict:
        pass
