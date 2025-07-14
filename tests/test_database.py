import uuid

import pytest
from bento_etl.models import JobStatusType


def test_create_status(job_status_database):
    status = job_status_database.create_status()
    assert status.id is not None


def test_change_status_valid(job_status_database):
    status = job_status_database.create_status()
    new_status = job_status_database.change_status(status.id, JobStatusType.SUCCESS)

    assert status != new_status
    assert new_status.status == JobStatusType.SUCCESS


def test_change_status_invalid(job_status_database):
    with pytest.raises(Exception):
        job_status_database.change_status(
            uuid.uuid4(), JobStatusType.SUCCESS
        )  # Job status with uuid doesn't exist


def test_get_all_status_valid(job_status_database):
    all_job_status = [job_status_database.create_status() for _ in range(5)]
    fetched_all_status = job_status_database.get_all_status()

    assert len(all_job_status) == len(fetched_all_status)
    assert all([status in fetched_all_status for status in all_job_status])


def test_get_all_status_invalid(job_status_database):
    fetched_all_status = (
        job_status_database.get_all_status()
    )  # No job status in the database
    assert fetched_all_status == []


def test_get_status_valid(job_status_database):
    status = job_status_database.create_status()
    status_value = job_status_database.get_status(status.id)
    assert status_value.status == status.status


def test_get_status_invalid(job_status_database):
    status_value = job_status_database.get_status(
        uuid.uuid4()
    )  # Job status with uuid doesn't exist
    assert status_value is None
