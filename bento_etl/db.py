

from typing import Annotated
from functools import lru_cache

from fastapi import Depends
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
        #TODO refactor to method
        try:
            create_db_engine = create_engine(f"{config.database_path}/postgres", echo=True)
            with create_db_engine.connect() as conn:
                conn.execute(text("commit"))
                conn.execute(text(f"create database {config.database_name}"))
        except:
            pass
        self.engine = create_engine(f"{config.database_path}/{config.database_name}", echo=True)

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
            job_selection = select(JobStatus).where(JobStatus.id == job_id)
            results = session.exec(job_selection)
            job = results.one()

            job.status = status
            job.extra_information = information

            session.add(job)
            session.commit()

            session.refresh(job) # TODO remove?
            print(job.status)


@lru_cache
def get_db(config: ConfigDependency):
    return Database(config)


DatabaseDependency = Annotated[Database, Depends(get_db)]

