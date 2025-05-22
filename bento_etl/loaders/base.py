from logging import Logger

__all__ = ["BaseLoader"]


class BaseLoader:
    """
    Base class for ETL loader implementation.

    Loaders are the final step of an ETL pipeline, they receive transformed data from their upstream
    and load it into the target destination.
    """

    def __init__(self, logger: Logger):
        self.logger = logger

    def load(self, data):
        pass


# TODO: implement loaders for phenopackets and experiments
