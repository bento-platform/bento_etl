

from bento_etl.models import JobStatus, JobStatusType


def test_create_job_status(database):
    status = database.create_job_status()
    assert status.id is not None

def test_change_job_status(database):
    status = database.create_job_status()

    new_status = database.change_job_status(status.id, JobStatusType.SUCCESS)

    assert status != new_status
    assert new_status.status == JobStatusType.SUCCESS

def test_get_all_job_status_valid(database):
    all_job_status = [database.create_job_status() for _ in range(5)]

    fetched_all_status = database.get_all_job_status()

    assert all_job_status == fetched_all_status

