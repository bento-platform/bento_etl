import asyncio
import uuid
import httpx
import pytest
from bento_etl.loaders.base import BaseLoader
from bento_etl.loaders.phenopackets_loader import PhenopacketsLoader


async def mock_long_task():
    await asyncio.sleep(99999)
    return httpx.Response(204)


class TestBaseLoader:
    def test_create_data_batches_small_batch_size(
        self, logger, config, load_phenopacket_data
    ):
        loader = BaseLoader(logger, config)

        batches = loader._create_data_batches(load_phenopacket_data, 5)
        assert len(batches) == 2
        assert len(batches[0]) == 5

    def test_create_data_batches_large_batch_size(
        self, logger, config, load_phenopacket_data
    ):
        loader = BaseLoader(logger, config)

        batches = loader._create_data_batches(load_phenopacket_data, 100)
        assert len(batches) == 1
        assert len(batches[0]) == 6

    @pytest.mark.asyncio
    async def test_cancel_all_requests(self, logger, config):
        requests = [asyncio.ensure_future(mock_long_task()) for _ in range(5)]
        loader = BaseLoader(logger, config)

        assert all(not request.done() for request in requests)
        loader._cancel_all_requests(requests)
        await asyncio.sleep(0.5)  # Sleep a bit to allow for the cancels to take affect
        assert all(request.done() for request in requests)


class TestPhenopacketsLoader:
    def test_invalid_batch_size(self, logger, config):
        with pytest.raises(Exception):
            PhenopacketsLoader(logger, config, uuid.uuid4(), -5)

    @pytest.mark.asyncio
    async def test_valid_load_no_batches(
        self, logger, config, load_phenopacket_data, set_mock_for_valid_post
    ):
        loader = PhenopacketsLoader(logger, config, uuid.uuid4(), 0)
        await loader.load(load_phenopacket_data)

    @pytest.mark.asyncio
    async def test_valid_load_small_batch_size(
        self, logger, config, load_phenopacket_data, set_mock_for_valid_post
    ):
        loader = PhenopacketsLoader(logger, config, uuid.uuid4(), 2)
        await loader.load(load_phenopacket_data)

    @pytest.mark.asyncio
    async def test_valid_load_large_batch_size(
        self, logger, config, load_phenopacket_data, set_mock_for_valid_post
    ):
        loader = PhenopacketsLoader(logger, config, uuid.uuid4(), 9)
        await loader.load(load_phenopacket_data)

    @pytest.mark.asyncio
    async def test_invalid_dataset_id(
        self, logger, config, load_phenopacket_data, set_mock_for_invalid_post
    ):
        loader = PhenopacketsLoader(logger, config, "BAD_DATASET_ID", 0)
        with pytest.raises(Exception, match="400"):
            await loader.load(load_phenopacket_data)

    @pytest.mark.asyncio
    async def test_invalid_data(self, logger, config, set_mock_for_invalid_post):
        loader = PhenopacketsLoader(logger, config, uuid.uuid4(), 0)
        with pytest.raises(Exception, match="400"):
            await loader.load("BAD_DATA")
