

from typing import Annotated
from functools import lru_cache

from fastapi import Depends, HTTPException
from sqlmodel import Session, SQLModel, create_engine, select, text

from bento_etl.config import Config, ConfigDependency, get_config
from bento_etl.models import JobStatus, JobStatusType

__all__ = [
    "Database",
    "get_db",
    "DatabaseDependency",
]


class Database():
    def __init__(self, config):
        self.engine = create_engine("sqlite:///jobstatus.db", echo=True)

    def setup(self):
        SQLModel.metadata.create_all(self.engine)

    def create_job_status(self):
        with Session(self.engine) as session:
            job = JobStatus()
            job.status = JobStatusType.SUBMITTED
            session.add(job)
            session.commit()
            session.refresh(job)
            return job

    def change_job_status(self, job_id:str, status:JobStatusType, information:str=""):
        with Session(self.engine) as session:
            job = session.get(JobStatus, job_id)
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")
            job.status = status
            job.extra_information=information
            session.add(job)
            session.commit()

    def get_all_job_status(self):
        with Session(self.engine) as session:
            return session.exec(select(JobStatus), execution_options={"prebuffer_rows": True})

    def get_job_status(self, job_id:str):
        with Session(self.engine) as session:
            job_selection = select(JobStatus).where(JobStatus.id == job_id)
            results = session.exec(job_selection)
            return results.one() # TODO or first()?
        
    
    def delete_job(self, job_id):
        with Session(self.engine) as session:
            job = session.get(JobStatus, job_id)
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")
            session.delete(job)
            session.commit()

@lru_cache
def get_db(config: ConfigDependency):
    return Database(config)


DatabaseDependency = Annotated[Database, Depends(get_db)]

