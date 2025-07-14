

import uuid

import pytest
from bento_etl.models import JobStatusType


def test_create_status(database):
    status = database.create_job_status()
    assert status.id is not None

def test_change_status_valid(database):
    status = database.create_job_status()
    new_status = database.change_job_status(status.id, JobStatusType.SUCCESS)

    assert status != new_status
    assert new_status.status == JobStatusType.SUCCESS


def test_change_status_invalid(database):
    with pytest.raises(Exception):
        database.change_job_status(uuid.uuid4(), JobStatusType.SUCCESS)  # Job status with uuid doesn't exist


def test_get_all_status_valid(database):
    all_job_status = [database.create_job_status() for _ in range(5)]
    fetched_all_status = database.get_all_job_status()

    assert len(all_job_status) == len(fetched_all_status)
    assert all([status in fetched_all_status for status in all_job_status])


def test_get_all_status_invalid(database):
    fetched_all_status = database.get_all_job_status()  # No job status in the database
    assert fetched_all_status == []


def test_get_status_valid(database):
    status = database.create_job_status()
    status_value = database.get_job_status(status.id)
    assert status_value.status == status.status


def test_get_status_invalid(database):
    status_value = database.get_job_status(uuid.uuid4())  # Job status with uuid doesn't exist
    assert status_value is None

