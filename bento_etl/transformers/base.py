from logging import Logger
from typing import Any

__all__ = ["BaseTransformer"]


class BaseTransformer:
    """
    Base class for ETL transformer implementation.
    Transformers read the raw data from an Extractor, transform it to the desired format
    and then forward it to a Loader.
    """

    def __init__(self, logger: Logger):
        self.logger = logger

    def transform(self, raw: Any) -> Any:
        raise NotImplementedError


# TODO: implement Phenopacket and Experiment transformers that take in PCGL JSON data.

# TODO: implement plugin registration for custom transformers
