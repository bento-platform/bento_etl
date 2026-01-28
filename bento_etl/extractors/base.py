from logging import Logger

__all__ = ["BaseExtractor"]


class BaseExtractor:
    """
    Base class to implement ETL extractors.
    An Extractor is configured to read data from a source (local or remote).

    Concrete extractor implementations should not modify the data obtained from the source,
    this is the Tranformer's job in an ETL pipeline.

    Concrete extractors should be configured in the constructor and implement the `extract` function, which returns
    a json dict.
    """

    def __init__(self, logger: Logger):
        self.logger = logger

    def extract(self) -> dict:
        raise NotImplementedError
