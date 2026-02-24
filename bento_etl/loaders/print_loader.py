from logging import Logger

from bento_etl.config import Config
from .base import BaseLoader


class PrintLoader(BaseLoader):
    # pragma: no cover
    def __init__(self, logger: Logger, config: Config):
        super().__init__(logger, config, "dummy_url", "katsu", 204, 0)

    # pragma: no cover
    async def load(self, data: list[dict]):
        for idx, item in enumerate(data):
            self.logger.debug(f"Item {idx} parsed: {item}")
