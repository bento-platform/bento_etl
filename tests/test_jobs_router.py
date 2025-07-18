import uuid
from fastapi.testclient import TestClient

from bento_etl.db import JobStatusDatabase


def test_get_status_valid(
    test_client: TestClient, job_status_database: JobStatusDatabase
):
    status = job_status_database.create_status()

    response = test_client.get(f"/jobs/{status.id}")
    assert response.status_code == 200
    assert response.json()["id"] == str(status.id)


def test_get_status_invalid(test_client: TestClient):
    response = test_client.get(f"/jobs/{uuid.uuid4()}")
    assert response.status_code == 404


def test_get_all_status(
    test_client: TestClient, job_status_database: JobStatusDatabase
):
    all_status_id = [str(job_status_database.create_status().id) for _ in range(5)]

    response = test_client.get("/jobs")
    response_body = response.json()

    assert response.status_code == 200
    assert len(all_status_id) == len(response_body)
    assert all([status["id"] in all_status_id for status in response_body])


def test_get_all_status_empty_db(test_client: TestClient):
    response = test_client.get("/jobs")
    assert response.status_code == 200
    assert response.json() == []
