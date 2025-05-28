from logging import Logger

from bento_etl.config import Config

__all__ = ["BaseLoader"]


class BaseLoader:
    """
    Base class for ETL loader implementation.

    Loaders are the final step of an ETL pipeline, they receive transformed data from their upstream
    and load it into the target destination.
    """

    def __init__(self, logger: Logger, config: Config):
        self.logger = logger
        self.config = config

    def load(self, data):
        pass


# TODO: implement loaders for phenopackets and experiments
