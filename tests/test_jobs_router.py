import json
import time
from typing import Any
import uuid
from fastapi.testclient import TestClient

from bento_etl.db import JobStatusDatabase

AUTHZ_HEADER = {"Authorization": "Token bearer"}

DEFAULT_JOB_SCHEMA = {
    "extractor": {"extract_url": "some_url", "frequency_ms": 0, "type": "api-fetch"},
    "transformer": {},
    "loader": {
        "dataset_id": "some_dataset_id",
        "batch_size": 0,
        "data_type": "phenopackets",
    },
}


def test_post_submit_job_valid(
    test_client: TestClient,
    job_status_database: JobStatusDatabase,
    mock_authz,
    mock_extractor_success_call,
    mock_loader_valid_post,
):
    response = test_client.post(
        "/jobs", content=json.dumps(DEFAULT_JOB_SCHEMA), headers=AUTHZ_HEADER
    )

    assert response.status_code == 200
    assert response.json()["message"]
    assert len(test_client.get("/jobs").json()) == 1

    time.sleep(
        1
    )  # Time delay to let the job run and get updated in the db. might need to be longer

    db_response = test_client.get("/jobs")
    response_body = db_response.json()

    assert db_response.status_code == 200
    assert len(response_body) == 1
    assert response_body[0]["status"] == "success"


def test_post_submit_job_invalid_bad_extractor(
    test_client: TestClient,
    job_status_database: JobStatusDatabase,
    mock_authz,
    mock_extractor_bad_status_code,
    mock_loader_valid_post,
):
    response = test_client.post(
        "/jobs", content=json.dumps(DEFAULT_JOB_SCHEMA), headers=AUTHZ_HEADER
    )
    assert response.status_code == 200
    assert response.json()["message"]
    assert len(test_client.get("/jobs").json()) == 1

    time.sleep(
        1
    )  # Time delay to let the job run and get updated in the db. might need to be longer

    db_response = test_client.get("/jobs")
    response_body = db_response.json()

    assert db_response.status_code == 200
    assert len(response_body) == 1
    assert response_body[0]["status"] == "error"


def test_post_submit_job_invalid_bad_loader(
    test_client: TestClient,
    job_status_database: JobStatusDatabase,
    mock_authz,
    mock_extractor_success_call,
    mock_loader_invalid_post,
):
    response = test_client.post(
        "/jobs", content=json.dumps(DEFAULT_JOB_SCHEMA), headers=AUTHZ_HEADER
    )
    assert response.status_code == 200
    assert response.json()["message"]
    assert len(test_client.get("/jobs").json()) == 1

    time.sleep(
        1
    )  # Time delay to let the job run and get updated in the db. might need to be longer

    db_response = test_client.get("/jobs")
    response_body = db_response.json()

    assert db_response.status_code == 200
    assert len(response_body) == 1
    assert response_body[0]["status"] == "error"


def test_get_status_valid(
    test_client: TestClient,
    job_status_database: JobStatusDatabase,
    mocked_job_dict: dict[str, Any],
):
    status = job_status_database.create_status(mocked_job_dict)

    response = test_client.get(f"/jobs/{status.id}")
    assert response.status_code == 200
    assert response.json()["id"] == str(status.id)


def test_get_status_invalid(test_client: TestClient):
    response = test_client.get(f"/jobs/{uuid.uuid4()}")
    assert response.status_code == 404


def test_get_all_status(
    test_client: TestClient,
    job_status_database: JobStatusDatabase,
    mocked_job_dict: dict[str, Any],
):
    all_status_id = [
        str(job_status_database.create_status(mocked_job_dict).id) for _ in range(5)
    ]

    response = test_client.get("/jobs")
    response_body = response.json()

    assert response.status_code == 200
    assert len(all_status_id) == len(response_body)
    assert all([status["id"] in all_status_id for status in response_body])


def test_get_all_status_empty_db(test_client: TestClient):
    response = test_client.get("/jobs")
    assert response.status_code == 200
    assert response.json() == []


def test_delete_status_valid(
    test_client: TestClient,
    job_status_database: JobStatusDatabase,
    mocked_job_dict: dict[str, Any],
    mock_authz,
):
    status = job_status_database.create_status(mocked_job_dict)

    response = test_client.delete(f"/jobs/{status.id}", headers=AUTHZ_HEADER)

    assert response.status_code == 200
    assert response.json()["message"]
    assert (
        test_client.get(f"/jobs/{status.id}").status_code == 404
    )  # Get should return 404 because the status was deleted from db


def test_delete_status_invalid(test_client: TestClient, mock_authz):
    inexistant_status_id = uuid.uuid4()
    response = test_client.delete(f"/jobs/{inexistant_status_id}", headers=AUTHZ_HEADER)
    assert response.status_code == 404
