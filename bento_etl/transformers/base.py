from logging import Logger

__all__ = ["BaseTransformer"]

class BaseTransformer:
    def __init__(self, logger: Logger, plugin: str):
        self.logger = logger
        self.plugin = plugin

    def transform(self, data: list[dict]) -> list[dict]:
        self.logger.info(f"Applying transformation with plugin: {self.plugin}")
        if not data:
            self.logger.warning("Received empty data for transformation")
            return []
        # For now, pass the input JSON unchanged
        # TODO: Implement specific transformations based on plugin
        self.logger.info(f"Transformed {len(data)} items")
        return data