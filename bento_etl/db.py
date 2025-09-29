from typing import Annotated, Any, Sequence
from functools import lru_cache
from datetime import datetime

from fastapi import Depends, HTTPException
from sqlmodel import Session, SQLModel, create_engine, select

from bento_etl.logger import BoundLogger, get_logger
from bento_etl.models import JobStatus, JobStatusType
from bento_etl.config import Config, get_config

__all__ = [
    "JobStatusDatabase",
    "get_job_status_db",
    "JobStatusDatabaseDependency",
]


class JobStatusDatabase:
    def __init__(self, logger: BoundLogger, config: Config):
        self.engine = create_engine(f"sqlite:////{str(config.database_path).lstrip('/')}", echo=True)
        self.logger = logger

    def setup(self):
        SQLModel.metadata.create_all(self.engine)

    def create_status(self, job_data: dict[str, Any]) -> JobStatus:
        with Session(self.engine) as session:
            job = JobStatus(status=JobStatusType.SUBMITTED, job_data=job_data)
            session.add(job)
            session.commit()
            session.refresh(job)
            return job

    def update_status(
        self, job_id: str, status: JobStatusType, error_info: str | None = None
    ) -> JobStatus:
        with Session(self.engine) as session:
            job = session.get(JobStatus, job_id)
            if not job:
                error_message = f"Requested job with id {job_id} is not found in database. Cannot update status"
                self.logger.error(error_message)
                raise ValueError(error_info)

            job.status = status
            if job.status == JobStatusType.SUCCESS:
                job.completed_at = datetime.now()
            elif job.status == JobStatusType.ERROR:
                job.error_message = error_info
                job.error_at = datetime.now()

            session.add(job)
            session.commit()
            session.refresh(job)
            return job

    def get_all_status(self) -> Sequence[JobStatus]:
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

    def delete_status(self, job_id: str):
        with Session(self.engine) as session:
            job = session.get(JobStatus, job_id)
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")
            session.delete(job)
            session.commit()


@lru_cache
def get_job_status_db():
    return JobStatusDatabase(get_logger(), get_config())


JobStatusDatabaseDependency = Annotated[JobStatusDatabase, Depends(get_job_status_db)]
