from pydantic import BaseModel

__all__ = ["Job"]


class Job(BaseModel):
    id: str
    # TODO: add rest of fields
