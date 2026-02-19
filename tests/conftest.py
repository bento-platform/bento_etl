import httpx
import pytest
import os
import json
import boto3

from moto import mock_aws
from aioresponses import aioresponses
from fastapi.testclient import TestClient
from sqlmodel import SQLModel, create_engine
from sqlmodel.pool import StaticPool


from bento_etl.db import JobStatusDatabase, get_job_status_db
from bento_etl.logger import get_logger, BoundLogger
from bento_etl.models import Job, LoadStep, TransformStep, ApiFetchExtractStep

os.environ["BENTO_DEBUG"] = "true"
os.environ["BENTO_VALIDATE_SSL"] = "false"
os.environ["BENTO_JSON_LOGS"] = "false"  # use rich text logs in testing
os.environ["CORS_ORIGINS"] = "*"
os.environ["TESTING"] = "True"

os.environ["BENTO_AUTHZ_SERVICE_URL"] = "https://authz.local"
os.environ["AUTHZ_ENABLED"] = "False"

from bento_etl.config import Config, get_config
from bento_etl.main import app
from bento_etl import authz


@pytest.fixture
def config() -> Config:
    return get_config()


@pytest.fixture
def logger(config: Config) -> BoundLogger:
    return get_logger(config)


@pytest.fixture()
def engine():
    eng = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def job_status_database(logger, config, engine) -> JobStatusDatabase:
    return JobStatusDatabase(logger, config, engine)


@pytest.fixture
def test_client(job_status_database):
    app.dependency_overrides[get_job_status_db] = lambda: job_status_database

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture
def mocked_job_dict():
    extractor = ApiFetchExtractStep(
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
        lambda *args, **kwargs: httpx.Response(200, content=phenopacket_content),
    )


@pytest.fixture
def mock_extractor_bad_status_code(monkeypatch):
    monkeypatch.setattr(
        EXTRACTOR_REQUEST_PATH, lambda *args, **kwargs: httpx.Response(400)
    )


@pytest.fixture
def mock_extractor_valid_empty_response(monkeypatch):
    monkeypatch.setattr(
        EXTRACTOR_REQUEST_PATH, lambda *args, **kwargs: httpx.Response(200)
    )


# S3
@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    # TODO: only use default boto/aws env vars
    os.environ["S3_BUCKET"] = "test"
    os.environ["S3_REGION"] = "us-east-1"
    os.environ["S3_ACCESS_KEY"] = "testing"
    os.environ["S3_SECRET_KEY"] = "testing"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    """
    Return a mocked S3 client
    """
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture(scope="function")
def mocked_s3(aws_credentials):
    """
    Return a mocked S3 client
    """
    with mock_aws():
        yield


@pytest.fixture
def mock_s3_extractor_pheno_json(mocked_s3):
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket="test")
    with open("tests/data/synthetic_phenopackets_v2.json", "rb") as f:
        s3.put_object(
            Bucket="test",
            Key="phenopackets.json",
            Body=f,
        )


#### TRANSFORMER MOCKS


@pytest.fixture
def mock_transformer(monkeypatch):
    """Mock transformer that returns data unchanged."""
    from bento_etl.transformers import dependencies

    def mock_get_transformer(job, logger):
        # Return a mock transformer for testing the transformer execution path
        from unittest.mock import MagicMock

        if job.transformer.type == "test-transformer":
            mock_trans = MagicMock()
            mock_trans.transform = lambda data: data  # passthrough
            return mock_trans
        return dependencies.get_transformer(job, logger)

    monkeypatch.setattr("bento_etl.routers.jobs.get_transformer", mock_get_transformer)


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
