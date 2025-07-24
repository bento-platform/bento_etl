from typing import Annotated
from functools import lru_cache

from fastapi import Depends, HTTPException
from sqlmodel import Session, SQLModel, create_engine, select

from bento_etl.models import JobStatus, JobStatusType

__all__ = [
    "JobStatusDatabase",
    "get_job_status_db",
    "JobStatusDatabaseDependency",
]


class JobStatusDatabase:
    def __init__(self):
        self.engine = create_engine("sqlite:///jobstatus.db", echo=True)

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

    def change_status(
        self, job_id: str, status: JobStatusType, information: str | None = None
    ) -> JobStatus:
        with Session(self.engine) as session:
            job = session.get(JobStatus, job_id)
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")
            job.status = status
            job.extra_information = information
            session.add(job)
            session.commit()
            session.refresh(job)
            return job

    def get_all_status(self) -> list[JobStatus]:
        with Session(self.engine) as session:
            return session.exec(select(JobStatus)).all()

    def get_status(self, job_id: str) -> JobStatus:
        with Session(self.engine) as session:
            job_selection = select(JobStatus).where(JobStatus.id == job_id)
            return session.exec(job_selection).first()

    def delete_job_status(self, job_id):
        with Session(self.engine) as session:
            job = session.get(JobStatus, job_id)
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")
            session.delete(job)
            session.commit()


@lru_cache
def get_job_status_db():
    return JobStatusDatabase()


JobStatusDatabaseDependency = Annotated[JobStatusDatabase, Depends(get_job_status_db)]
