import pytest
from fastapi.testclient import TestClient
from bento_etl.main import app

AUTHZ_HEADER = {"Authorization": "Token bearer"}


def test_app_includes_job_router(test_client: TestClient, mock_authz):
    """Test that the app includes the job router."""
    response = test_client.get("/jobs", headers=AUTHZ_HEADER)
    # Should return 200 for valid request
    assert response.status_code == 200


def test_service_info_endpoint(test_client: TestClient):
    """Test the service info endpoint."""
    response = test_client.get("/service-info")
    assert response.status_code == 200
    data = response.json()
    assert "id" in data
    assert "name" in data
    assert "type" in data
    assert "bento" in data


def test_openapi_docs_available(test_client: TestClient):
    """Test that OpenAPI docs are available."""
    response = test_client.get("/docs")
    assert response.status_code == 200
