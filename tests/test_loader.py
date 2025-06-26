import asyncio
import uuid
import httpx
import pytest
import os
import json
from unittest.mock import patch
from bento_etl import authz
from bento_etl.loaders.base import BaseLoader
from bento_etl.loaders.phenopackets_loader import PhenopacketsLoader


@pytest.fixture(autouse=True)
def mock_bearer_token(monkeypatch):
    def mock_get_bearer_token(*args, **kwargs):
        return "MockedToken"
    monkeypatch.setattr(authz, "get_bearer_token", mock_get_bearer_token)

@pytest.fixture
def load_phenopacket_data():
    caller_path =os.path.dirname(__file__)
    file_path = os.path.join(caller_path, "data/synthetic_phenopackets_v2.json")
    with open(file_path) as f:
        file_content = json.load(f)
    return file_content

@pytest.fixture
def set_mock_for_valid_post(monkeypatch):
    async def mock_valid_post(*args, **kwargs):
                return httpx.Response(204)
    monkeypatch.setattr(httpx.AsyncClient, "post", mock_valid_post)

@pytest.fixture
def set_mock_for_invalid_post(monkeypatch):
    async def mock_invalid_post(*args, **kwargs):
                return httpx.Response(400)
    monkeypatch.setattr(httpx.AsyncClient, "post", mock_invalid_post)

async def mock_long_task():
    await asyncio.sleep(99999)
    return httpx.Response(204)

class TestBaseLoader:
     def test_create_data_batches_small_batch_size(self, logger, config, load_phenopacket_data):
          loader = BaseLoader(logger, config)

          batches = loader._create_data_batches(load_phenopacket_data, 5)
          assert len(batches) == 2
          assert len(batches[0]) == 5
          
          
     def test_create_data_batches_large_batch_size(self, logger, config, load_phenopacket_data):
          loader = BaseLoader(logger, config)

          batches = loader._create_data_batches(load_phenopacket_data, 100)
          assert len(batches) == 1
          assert len(batches[0]) == 6

     @pytest.mark.asyncio
     async def test_cancel_all_requests(self, logger, config):
        requests = [asyncio.ensure_future(mock_long_task()) for _ in range(5)]
        loader = BaseLoader(logger, config)

        assert all(request.done() == False for request in requests)
        loader._cancel_all_requests(requests)
        await asyncio.sleep(0.5)  # Sleep a bit to allow for the cancels to take affect
        assert all(request.done() for request in requests)

class TestPhenopacketsLoader:
    def test_invalid_batch_size(self, logger, config):
        with pytest.raises(Exception):
            PhenopacketsLoader(logger, config, uuid.uuid4(), -5)

    @pytest.mark.asyncio
    async def test_valid_load_no_batches(self, logger, config, load_phenopacket_data, set_mock_for_valid_post):
        loader = PhenopacketsLoader(logger, config, uuid.uuid4(), 0)
        await loader.load(load_phenopacket_data)
    
    @pytest.mark.asyncio
    async def test_valid_load_small_batch_size(self, logger, config, load_phenopacket_data, set_mock_for_valid_post):
        loader = PhenopacketsLoader(logger, config, uuid.uuid4(), 2)
        await loader.load(load_phenopacket_data)

    @pytest.mark.asyncio
    async def test_valid_load_large_batch_size(self, logger, config, load_phenopacket_data, set_mock_for_valid_post):
        loader = PhenopacketsLoader(logger, config, uuid.uuid4(), 9)
        await loader.load(load_phenopacket_data)

    @pytest.mark.asyncio
    async def test_invalid_dataset_id(self, logger, config, load_phenopacket_data, set_mock_for_invalid_post):
        loader = PhenopacketsLoader(logger, config, "BAD_DATASET_ID", 4)
        with pytest.raises(Exception, match="400"):
            await loader.load(load_phenopacket_data)

    @pytest.mark.asyncio
    async def test_invalid_data(self, logger, config, set_mock_for_invalid_post):
        loader = PhenopacketsLoader(logger, config, uuid.uuid4(), 4)
        with pytest.raises(Exception, match="400"):
            await loader.load("BAD_DATA")
