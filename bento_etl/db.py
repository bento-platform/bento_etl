

from sqlmodel import Session, SQLModel, create_engine, select

from bento_etl.config import Config, get_config
from bento_etl.models import JobStatus, JobStatusType



class Database():
    def __init__(self):
        config = get_config()
        self.engine = create_engine(config.database_uri)
        SQLModel.metadata.create_all(self.engine)
    

    def change_status(self, job_id:str, status:JobStatusType, information:str=""):
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

