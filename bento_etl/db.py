from logging import Logger
from typing import Annotated
from functools import lru_cache

from fastapi import Depends, HTTPException
from sqlmodel import Session, SQLModel, create_engine, func, select

from bento_etl.models import JobStatus, JobStatusType

__all__ = [
    "JobStatusDatabase",
    "get_job_status_db",
    "JobStatusDatabaseDependency",
]


class JobStatusDatabase:
    def __init__(self, logger:Logger):
        self.engine = create_engine("sqlite:///jobstatus.db", echo=True)
        self.logger = logger

    def setup(self):
        SQLModel.metadata.create_all(self.engine)

    def create_status(self) -> JobStatus:
        with Session(self.engine) as session:
            job = JobStatus()
            job.status = JobStatusType.SUBMITTED

            session.add(job)
            session.commit()
            session.refresh(job)
            return job

    def update_status(
        self, job_id: str, status: JobStatusType, error_info: str | None = None) -> JobStatus:
        with Session(self.engine) as session:
            job = session.get(JobStatus, job_id)
            if not job:
                self.logger.error(f"Request job with id {job_id} is not found in database. Cannot update status")
                return

            job.status = status
            if job.status == JobStatusType.SUCCESS:
                job.completed_at = func.now()
            elif job.status == JobStatusType.ERROR:
                job.error_message = error_info
                job.error_at = func.now()

            session.add(job)
            session.commit()
            session.refresh(job)
            return job

    # TODO Error 404 if no jobs found?
    def get_all_status(self) -> list[JobStatus]:
        with Session(self.engine) as session:
            return session.exec(select(JobStatus)).all()

    def get_status(self, job_id: str) -> JobStatus:
        with Session(self.engine) as session:
            job_selection = select(JobStatus).where(JobStatus.id == job_id)
            result = session.exec(job_selection).first()
            if not result:
                raise HTTPException(
                    status_code=404, detail=f"Job {job_id} not found in database"
                )
            return result

    # TODO delete if not used
    def delete_job_status(self, job_id:str):
        with Session(self.engine) as session:
            job = session.get(JobStatus, job_id)
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")
            session.delete(job)
            session.commit()


@lru_cache
def get_job_status_db(logger:Logger):
    return JobStatusDatabase(logger)


JobStatusDatabaseDependency = Annotated[JobStatusDatabase, Depends(get_job_status_db)]
