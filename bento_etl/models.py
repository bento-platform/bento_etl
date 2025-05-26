from typing import Literal
from pydantic import BaseModel

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
    # TODO: complete
    pass


class JobStatus(BaseModel):
    """
    Describes the current status of a job
    """
    # TODO: complete
    pass
    

class Job(BaseModel):
    id: str # TODO: auto gen uuid
    extractor: ExtractStep
    transformer: TransformStep
    loader: LoadStep
    status: JobStatus

    # TODO: add rest of fields
    # Should be able to describe an ETL pipeline to run
    # - Extractor to use and its config
    # - Transformer to use and its config
    # - Loader to use and its config
