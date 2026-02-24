from logging import Logger

from bento_etl.config import Config
from .base import BaseLoader


class PrintLoader(BaseLoader):
    def __init__(self, logger: Logger, config: Config): # pragma: no cover
        super().__init__(logger, config, "dummy_url", "katsu", 204, 0)

    async def load(self, data: list[dict]): # pragma: no cover
        for idx, item in enumerate(data):
            self.logger.debug(f"Item {idx} parsed: {item}")
