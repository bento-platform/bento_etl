from datetime import datetime
from enum import Enum
from typing import Literal, Optional
import uuid
from pydantic import BaseModel
from sqlmodel import JSON, Column, Enum as SQLModelEnum, Field, SQLModel

__all__ = ["Job"]


class ExtractStep(BaseModel):
    """
    Class to describe an Extractor step to run in a pipeline job.
    """

    extract_url: str
    #
    type: Literal["api-fetch"]
    http_verb: str = "GET"
    expected_status_code: int = 200


class TransformStep(BaseModel):
    """
    Class to describe a Transformer step to run in a pipeline job.
    """

    type: Literal["None"]


class LoadStep(BaseModel):
    """
    Class to describe a Loader step to run in a pipeline job.
    """

    dataset_id: str
    batch_size: int
    data_type: Literal["phenopackets", "experiments"]


class Job(BaseModel):
    extractor: ExtractStep
    transformer: TransformStep
    loader: LoadStep

    # TODO: add rest of fields
    # Should be able to describe an ETL pipeline to run
    # - Extractor to use and its config
    # - Transformer to use and its config
    # - Loader to use and its config


class JobStatusType(str, Enum):
    SUBMITTED = "submitted"
    EXTRACTING = "extracting"
    TRANSFORMING = "transforming"
    LOADING = "loading"
    SUCCESS = "success"
    ERROR = "error"


class JobStatus(SQLModel, table=True):
    """
    Describes the current status of a job
    """

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    status: JobStatusType = Field(sa_column=Column(SQLModelEnum(JobStatusType)))
    job_data: dict = Field(sa_column=Column(JSON))
    created_at: datetime = datetime.now()
    completed_at: Optional[datetime] = None
    error_at: Optional[datetime] = None
    error_message: Optional[str] = None
