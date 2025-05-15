import pytest
import os

from fastapi.testclient import TestClient

os.environ["CORS_ORIGINS"] = "*"
os.environ["AUTHZ_ENABLED"] = "False"

from bento_etl.config import Config, get_config
from bento_etl.main import app


@pytest.fixture
def config() -> Config:
    return get_config()


@pytest.fixture
def test_client():
    with TestClient(app) as client:
        yield client
