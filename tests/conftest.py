from logging import Logger
from aioresponses import aioresponses
import httpx
import pytest
from fastapi.testclient import TestClient

import os
import json

from sqlmodel import Session, delete

from bento_etl.db import JobStatusDatabase, get_job_status_db
from bento_etl.logger import get_logger
from bento_etl.models import ExtractStep, Job, JobStatus, LoadStep, TransformStep

os.environ["BENTO_DEBUG"] = "true"
os.environ["BENTO_VALIDATE_SSL"] = "false"
os.environ["BENTO_JSON_LOGS"] = "false"  # use rich text logs in testing
os.environ["CORS_ORIGINS"] = "*"

os.environ["BENTO_AUTHZ_SERVICE_URL"] = "https://authz.local"
os.environ["AUTHZ_ENABLED"] = "False"

from bento_etl.config import Config, get_config
from bento_etl.main import app
from bento_etl import authz


@pytest.fixture
def config() -> Config:
    return get_config()


@pytest.fixture
def logger(config: Config) -> Logger:
    return get_logger(config)


@pytest.fixture
def job_status_database(
    logger: Logger, request: pytest.FixtureRequest
) -> JobStatusDatabase:
    db = get_job_status_db(logger)
    db.setup()

    def teardown():
        # Deletes the contents of the db after test
        with Session(db.engine) as session:
            session.exec(delete(JobStatus))
            session.commit()

    request.addfinalizer(teardown)
    return db


@pytest.fixture
def test_client():
    with TestClient(app) as client:
        yield client


@pytest.fixture
def mocked_job_dict():
    extractor = ExtractStep(extract_url="some_url", frequency_ms=0, type="api-fetch")
    tranformer = TransformStep()
    loader = LoadStep(
        dataset_id="some_dataset_id", batch_size=0, data_type="phenopackets"
    )
    yield Job(extractor=extractor, transformer=tranformer, loader=loader).model_dump()


@pytest.fixture(autouse=True)
def mock_bearer_token(monkeypatch):
    def mock_get_bearer_token(*args, **kwargs):
        return "MockedToken"

    monkeypatch.setattr(authz, "get_bearer_token", mock_get_bearer_token)


@pytest.fixture
def mock_authz():
    with aioresponses() as mock:
        yield mock.post(
            "https://authz.local/policy/evaluate", payload={"result": [[True]]}
        )


@pytest.fixture
def load_phenopacket_data():
    caller_path = os.path.dirname(__file__)
    file_path = os.path.join(caller_path, "data/synthetic_phenopackets_v2.json")
    with open(file_path) as f:
        file_content = json.load(f)
    return file_content


@pytest.fixture
def load_experiment_data():
    caller_path = os.path.dirname(__file__)
    file_path = os.path.join(caller_path, "data/synthetic_experiments.json")
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
