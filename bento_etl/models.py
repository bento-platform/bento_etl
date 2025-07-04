from enum import Enum
from typing import Literal
import uuid
from pydantic import BaseModel
from sqlmodel import Column, Enum as SQLModelEnum, Field, SQLModel

__all__ = ["Job"]


class ExtractStep(BaseModel):
    """
    Class to describe an Extractor step to run in a pipeline job.
    """

    format: Literal["json", "csv", "tsv", "vcf", "vcf.gz"]
    type: str
    # TODO: complete


class TransformStep(BaseModel):
    """
    Class to describe a Transformer step to run in a pipeline job.
    """

    # TODO: complete
    pass


class LoadStep(BaseModel):
    """
    Class to describe a Loader step to run in a pipeline job.
    """

    dataset_id: str
    batch_size: int
    data_type: Literal["phenopackets", "experiments"]

class JobStatusType(str, Enum):
    SUBMITTED = "Submitted"
    EXTRACTING = "Extracting"
    TRANSFORMING = "Transforming"
    LOADING = "Loading"
    SUCCESS = "Success"
    ERROR = "Error"

class JobStatus(SQLModel, table=True):
    """
    Describes the current status of a job
    """
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    status: JobStatusType = Field(sa_column=Column(SQLModelEnum(JobStatusType)))
    extra_information: str | None = Field(default=None)
    
    def __str__(self):
        return f"Job {self.id} | status {self.status} | extra information {self.extra_information}"


class Job(BaseModel):
    id: str  # TODO: auto gen uuid
    extractor: ExtractStep
    transformer: TransformStep
    loader: LoadStep
    status: JobStatus

    # TODO: add rest of fields
    # Should be able to describe an ETL pipeline to run
    # - Extractor to use and its config
    # - Transformer to use and its config
    # - Loader to use and its config
