import os
import json
import uuid
import httpx
import pytest
from unittest.mock import patch
from bento_etl.loaders.phenopackets_loader import PhenopacketsLoader

class TestPhenopacketsLoader:
    async_client_path = 'bento_etl.loaders.phenopackets_loader.httpx.AsyncClient.post'

    @pytest.mark.asyncio
    @patch(async_client_path, return_value = httpx.Response(204))
    async def test_valid_load(self, logger, config):
        loader = PhenopacketsLoader(logger, config, uuid.uuid4(), 4)
        await loader.load("{}")

    @pytest.mark.asyncio
    @patch(async_client_path, return_value = httpx.Response(400))
    async def test_invalid_dataset_id(self, logger, config):
        loader = PhenopacketsLoader(logger, config, "BAD_DATASET_ID", 4)

        with pytest.raises(Exception, match='400'):
            await loader.load("{}")

    @pytest.mark.asyncio
    @patch(async_client_path, return_value = httpx.Response(400))
    async def test_invalid_data(self, logger, config):
        loader = PhenopacketsLoader(logger, config, uuid.uuid4(), 4)

        with pytest.raises(Exception, match='400'):
            await loader.load("BAD_JSON")
