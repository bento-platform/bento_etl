from enum import Enum
from typing import Literal, Optional, List, Dict
import uuid
from pydantic import BaseModel, AnyUrl
from sqlmodel import Column, Enum as SQLModelEnum, Field, SQLModel

__all__ = ["Job", "JobStatus", "PipelineType"]

class PipelineType(str, Enum):
    PHENOPACKETS = "phenopackets"
    EXPERIMENTS = "experiments"

class ExtractStep(BaseModel):
    format: Literal["json", "csv", "tsv", "vcf", "vcf.gz"]
    type: str
    endpoint: AnyUrl
    interval: Optional[str] = None

class TransformStep(BaseModel):
    plugin: str

class LoadStep(BaseModel):
    dataset_id: str
    batch_size: int
    expected_status_code: int
    data_type: PipelineType

class JobStatusType(str, Enum):
    SUBMITTED = "Submitted"
    EXTRACTING = "Extracting"
    TRANSFORMING = "Transforming"
    LOADING = "Loading"
    SUCCESS = "Success"
    ERROR = "Error"

class JobStatus(SQLModel, table=True):
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    status: JobStatusType = Field(sa_column=Column(SQLModelEnum(JobStatusType)))
    extra_information: str | None = Field(default=None)

    def to_str(self):
        return f"Job {self.id} | {self.status} | {self.extra_information}"

class Job(BaseModel):
    id: str
    pipeline_type: PipelineType
    extractor: ExtractStep
    transformer: TransformStep
    loader: LoadStep
    status: JobStatusType = JobStatusType.SUBMITTED
    result: Optional[List[Dict]] = None