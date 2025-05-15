import pytest
import os

from fastapi.testclient import TestClient

os.environ["BENTO_DEBUG"] = "true"
os.environ["BENTO_VALIDATE_SSL"] = "false"
os.environ["BENTO_JSON_LOGS"] = "false"  # use rich text logs in testing
os.environ["CORS_ORIGINS"] = "*"

os.environ["BENTO_AUTHZ_SERVICE_URL"] = "https://authz.local"
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
