import json
import uuid
from fastapi.testclient import TestClient

from bento_etl.db import JobStatusDatabase
from bento_etl.models import Job


# TODO: Once Extractor and Transformer are integrated:
#   - Test for Error if
#       - Bad params are passed in extractor
#       - Correct extractor params but bad transformer params
#       - Correct extractor and transformer params but bad loader params
#   - Finish test for Success if the params to ETL are valid
def test_post_submit_job_valid(test_client: TestClient):
    job_schema = {
        "id": "some_id",
        "extractor": {"format": "json", "type": "string"},
        "transformer": {},
        "loader": {
            "dataset_id": "some_dataset_id",
            "batch_size": 0,
            "data_type": "phenopackets",
        },
    }
    response = test_client.post("/jobs", data=json.dumps(job_schema))
    assert response.status_code == 200
    assert response.json()["message"]
    assert len(test_client.get("/jobs").json()) == 1


def test_get_status_valid(
    test_client: TestClient,
    job_status_database: JobStatusDatabase,
    mocked_job_dict: Job,
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
    mocked_job_dict: Job,
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
    mocked_job_dict: Job,
):
    status = job_status_database.create_status(mocked_job_dict)
    response = test_client.delete(f"/jobs/{status.id}")

    assert response.status_code == 200
    assert response.json()["message"]
    assert (
        test_client.get(f"/jobs/{status.id}").status_code == 404
    )  # Get should return 404 because the status was deleted from db


def test_delete_status_invalid(test_client: TestClient):
    inexistant_status_id = uuid.uuid4()
    response = test_client.delete(f"/jobs/{inexistant_status_id}")
    assert response.status_code == 404
