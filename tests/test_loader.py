import asyncio
import uuid
import httpx
import pytest
from bento_etl.loaders.base import BaseLoader
from bento_etl.loaders.dependencies import get_loader
from bento_etl.loaders.experiments_loader import ExperimentsLoader
from bento_etl.loaders.phenopackets_loader import PhenopacketsLoader
from bento_etl.models import ExtractStep, Job, LoadStep, TransformStep


async def mock_long_task():
    await asyncio.sleep(99999)
    return httpx.Response(204)


def mock_job_with_data_type(data_type: str):
    return Job(
        extractor=ExtractStep(format="json", type="some_type"),
        transformer=TransformStep(),
        loader=LoadStep(
            dataset_id="some_dataset_id", batch_size=0, data_type=data_type
        ),
    )


class TestLoaderDependencies:
    def test_get_loader_phenopacket(self, logger, config):
        job = mock_job_with_data_type("phenopackets")
        loader = get_loader(job, logger, config)
        assert type(loader) is PhenopacketsLoader

    def test_get_loader_experiments(self, logger, config):
        job = mock_job_with_data_type("experiments")
        loader = get_loader(job, logger, config)
        assert type(loader) is ExperimentsLoader


class TestBaseLoader:
    def test_constructor_valid_no_batch_size(self, logger, config):
        BaseLoader(logger, config, "some_url", "some_service", 200)

    def test_constructor_valid_batch_size(self, logger, config):
        BaseLoader(logger, config, "some_url", "some_service", 200, 3)

    def test_constructor_invalid_load_url(self, logger, config):
        with pytest.raises(ValueError):
            BaseLoader(logger, config, "", "some_service", 200)

    def test_constructor_invalid_service_name(self, logger, config):
        with pytest.raises(ValueError):
            BaseLoader(logger, config, "some_url", "", 200)

    def test_constructor_invalid_batch_size(self, logger, config):
        with pytest.raises(ValueError):
            BaseLoader(logger, config, "some_url", "some_service", 200, -5)

    def test_create_data_batch_zero_batch_size(
        self, logger, config, load_phenopacket_data
    ):
        loader = BaseLoader(
            logger, config, "some_url", "some_service", 200, batch_size=0
        )

        batches = loader._create_data_batches(load_phenopacket_data)
        assert len(batches) == 1
        assert len(batches[0]) == len(load_phenopacket_data)

    def test_create_data_batches_small_batch_size(
        self, logger, config, load_phenopacket_data
    ):
        loader = BaseLoader(
            logger, config, "some_url", "some_service", 200, batch_size=5
        )

        batches = loader._create_data_batches(load_phenopacket_data)
        assert len(batches) == 2
        assert len(batches[0]) == 5

    def test_create_data_batches_large_batch_size(
        self, logger, config, load_phenopacket_data
    ):
        loader = BaseLoader(
            logger, config, "some_url", "some_service", 200, batch_size=100
        )

        batches = loader._create_data_batches(load_phenopacket_data)
        assert len(batches) == 1
        assert len(batches[0]) == 6

    @pytest.mark.asyncio
    async def test_cancel_all_requests(self, logger, config):
        requests = [asyncio.create_task(mock_long_task()) for _ in range(5)]
        loader = BaseLoader(logger, config, "some_url", "some_service", 200)

        assert all(not request.done() for request in requests)
        loader._cancel_all_requests(requests)
        await asyncio.sleep(0.5)  # Sleep a bit to allow for the cancels to take affect
        assert all(request.done() for request in requests)


class TestPhenopacketsLoader:
    def test_constructor_invalid_dataset_id(self, logger, config):
        with pytest.raises(ValueError):
            PhenopacketsLoader(logger, config, "")

    @pytest.mark.asyncio
    async def test_valid_load_no_batches(
        self, logger, config, load_phenopacket_data, set_mock_for_valid_post
    ):
        loader = PhenopacketsLoader(logger, config, uuid.uuid4())
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
        loader = PhenopacketsLoader(logger, config, uuid.uuid4(), 10)
        await loader.load(load_phenopacket_data)

    @pytest.mark.asyncio
    async def test_load_invalid_dataset_id(
        self, logger, config, load_phenopacket_data, set_mock_for_invalid_post
    ):
        loader = PhenopacketsLoader(logger, config, "BAD_DATASET_ID")
        with pytest.raises(Exception, match="400"):
            await loader.load(load_phenopacket_data)

    @pytest.mark.asyncio
    async def test_load_invalid_data(self, logger, config, set_mock_for_invalid_post):
        loader = PhenopacketsLoader(logger, config, uuid.uuid4())
        with pytest.raises(Exception, match="400"):
            await loader.load("BAD_DATA")


class TestExperimentsLoader:
    def test_constructor_invalid_dataset_id(self, logger, config):
        with pytest.raises(ValueError):
            ExperimentsLoader(logger, config, "")

    def test_slice_data_small_batch_size(self, logger, config, load_experiment_data):
        loader = ExperimentsLoader(logger, config, uuid.uuid4(), 2)

        batches = loader._slice_data(load_experiment_data)
        assert len(batches) == 5
        assert len(batches[0]["experiments"]) == 2
        assert all(batch["experiments"] for batch in batches)
        assert all(batch["resources"] for batch in batches)

    def test_slice_data_large_batch_size(self, logger, config, load_experiment_data):
        loader = ExperimentsLoader(logger, config, uuid.uuid4(), 200)
        batches = loader._slice_data(load_experiment_data)
        assert len(batches) == 1
        assert batches[0] == load_experiment_data

    @pytest.mark.asyncio
    async def test_valid_load_no_batches(
        self, logger, config, load_experiment_data, set_mock_for_valid_post
    ):
        loader = ExperimentsLoader(logger, config, uuid.uuid4())
        await loader.load(load_experiment_data)

    @pytest.mark.asyncio
    async def test_valid_load_small_batch_size(
        self, logger, config, load_experiment_data, set_mock_for_valid_post
    ):
        loader = ExperimentsLoader(logger, config, uuid.uuid4(), 2)
        await loader.load(load_experiment_data)

    @pytest.mark.asyncio
    async def test_valid_load_large_batch_size(
        self, logger, config, load_experiment_data, set_mock_for_valid_post
    ):
        loader = ExperimentsLoader(logger, config, uuid.uuid4(), 20)
        await loader.load(load_experiment_data)

    @pytest.mark.asyncio
    async def test_load_invalid_dataset_id(
        self, logger, config, load_experiment_data, set_mock_for_invalid_post
    ):
        loader = ExperimentsLoader(logger, config, "BAD_DATASET_ID")
        with pytest.raises(Exception, match="400"):
            await loader.load(load_experiment_data)

    @pytest.mark.asyncio
    async def test_load_invalid_data(self, logger, config, set_mock_for_invalid_post):
        loader = ExperimentsLoader(logger, config, uuid.uuid4())
        with pytest.raises(Exception, match="400"):
            await loader.load("BAD_DATA")
