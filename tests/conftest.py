from aioresponses import aioresponses
import httpx
import pytest
from fastapi.testclient import TestClient

import os
import json

from sqlmodel import Session, delete

from bento_etl.db import JobStatusDatabase, get_job_status_db
from bento_etl.logger import get_logger, BoundLogger
from bento_etl.models import ExtractStep, Job, JobStatus, LoadStep, TransformStep

os.environ["BENTO_DEBUG"] = "true"
os.environ["BENTO_VALIDATE_SSL"] = "false"
os.environ["BENTO_JSON_LOGS"] = "false"  # use rich text logs in testing
os.environ["CORS_ORIGINS"] = "*"

# Use a temporary directory and SQLite file for testing
os.environ["BENTO_ETL_INTERNAL_DATA_DIR"] = os.getcwd()
os.environ["DB_NAME"] = "test_job_status.db"

os.environ["BENTO_AUTHZ_SERVICE_URL"] = "https://authz.local"
os.environ["AUTHZ_ENABLED"] = "False"

from bento_etl.config import Config, get_config
from bento_etl.main import app
from bento_etl import authz


@pytest.fixture(scope="session", autouse=True)
def db_teardown():
    yield
    db_path = os.path.join(
        os.environ["BENTO_ETL_INTERNAL_DATA_DIR"], os.environ["DB_NAME"]
    )
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def config() -> Config:
    return get_config()


@pytest.fixture
def logger(config) -> BoundLogger:
    return get_logger(config)


@pytest.fixture
def job_status_database(
    request: pytest.FixtureRequest, config, logger
) -> JobStatusDatabase:
    db = get_job_status_db(config, logger)
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
    extractor = ExtractStep(
        extract_url="some_url",
        type="api-fetch",
        http_verb="GET",
        expected_status_code=200,
    )
    tranformer = TransformStep(type="None")
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
def load_phenopacket_data() -> dict:
    caller_path = os.path.dirname(__file__)
    file_path = os.path.join(caller_path, "data/synthetic_phenopackets_v2.json")
    with open(file_path) as f:
        file_content = json.load(f)
    return file_content


@pytest.fixture
def load_experiment_data() -> dict:
    caller_path = os.path.dirname(__file__)
    file_path = os.path.join(caller_path, "data/synthetic_experiments.json")
    with open(file_path) as f:
        file_content = json.load(f)
    return file_content


#### EXTRACTOR MOCKS
EXTRACTOR_REQUEST_PATH = "bento_etl.extractors.api_fetch_extractor.httpx.request"


@pytest.fixture
def mock_extractor_success_call(monkeypatch, load_phenopacket_data):
    phenopacket_content = json.dumps(load_phenopacket_data).encode("utf-8")
    monkeypatch.setattr(
        EXTRACTOR_REQUEST_PATH,
        lambda *args: httpx.Response(200, content=phenopacket_content),
    )


@pytest.fixture
def mock_extractor_bad_status_code(monkeypatch):
    monkeypatch.setattr(EXTRACTOR_REQUEST_PATH, lambda *args,: httpx.Response(400))


@pytest.fixture
def mock_extractor_valid_empty_response(monkeypatch):
    monkeypatch.setattr(EXTRACTOR_REQUEST_PATH, lambda *args: httpx.Response(200))


#### LOADER MOCKS
LOADER_POST_REQUEST_PATH = "bento_etl.loaders.base.httpx.AsyncClient.post"


@pytest.fixture
def mock_loader_valid_post(monkeypatch):
    async def mock_valid_post(*args, **kwargs):
        return httpx.Response(204)

    monkeypatch.setattr(LOADER_POST_REQUEST_PATH, mock_valid_post)


@pytest.fixture
def mock_loader_invalid_post(monkeypatch):
    async def mock_invalid_post(*args, **kwargs):
        return httpx.Response(400)

    monkeypatch.setattr(LOADER_POST_REQUEST_PATH, mock_invalid_post)
