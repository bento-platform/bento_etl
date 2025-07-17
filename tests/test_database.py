import uuid

import pytest
from bento_etl.models import JobStatusType


def test_create_status(job_status_database):
    status = job_status_database.create_status()
    assert status.id is not None
    assert status.created_at is not None


def test_update_status_to_success(job_status_database):
    status = job_status_database.create_status()
    new_status = job_status_database.update_status(status.id, JobStatusType.SUCCESS)

    assert status != new_status
    assert new_status.status == JobStatusType.SUCCESS
    assert new_status.completed_at is not None


def test_update_status_to_error(job_status_database):
    error_description = "Some error description"
    status = job_status_database.create_status()
    new_status = job_status_database.update_status(
        status.id, JobStatusType.ERROR, error_description
    )

    assert status != new_status
    assert new_status.status == JobStatusType.ERROR
    assert new_status.error_at is not None
    assert new_status.error_message == error_description


def test_update_status_invalid(job_status_database):
    job_status_database.create_status()
    inexistant_job_id = uuid.uuid4()
    with pytest.raises(Exception):
        job_status_database.update_status(inexistant_job_id, JobStatusType.LOADING)


def test_get_all_status(job_status_database):
    all_job_status = [job_status_database.create_status() for _ in range(5)]
    fetched_all_status = job_status_database.get_all_status()

    assert len(all_job_status) == len(fetched_all_status)
    assert all([status in fetched_all_status for status in all_job_status])


def test_get_all_status_empty_db(job_status_database):
    fetched_all_status = (
        job_status_database.get_all_status()
    )  # No job status in the database
    assert fetched_all_status == []


def test_get_status_valid(job_status_database):
    status = job_status_database.create_status()
    status_value = job_status_database.get_status(status.id)
    assert status_value.status == status.status


def test_get_status_invalid(job_status_database):
    inexistant_job_id = uuid.uuid4()
    with pytest.raises(Exception):
        job_status_database.get_status(inexistant_job_id)


def test_delete_status_valid(job_status_database):
    status = job_status_database.create_status()
    job_status_database.delete_status(status.id)

    with pytest.raises(Exception):
        job_status_database.get_status(status.id)


def test_delete_status_invalid(job_status_database):
    inexistant_job_id = uuid.uuid4()
    with pytest.raises(Exception):
        job_status_database.delete_status(inexistant_job_id)
