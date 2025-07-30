import uuid
from fastapi import APIRouter, BackgroundTasks

from bento_etl.authz import authz_middleware
from bento_etl.db import JobStatusDatabaseDependency
from bento_etl.extractors.base import BaseExtractor
from bento_etl.extractors.dependencies import ExtractorDep
from bento_etl.loaders.base import BaseLoader
from bento_etl.loaders.dependencies import LoaderDep
from bento_etl.models import Job, JobStatus, JobStatusType
from bento_etl.pipeline_config import PipelineRegistryDep
from bento_etl.transformers.base import BaseTransformer
from bento_etl.transformers.dependencies import TransformerDep

__all__ = ["job_router"]

"""
Jobs router plan:
/jobs       [GET]       => list submitted jobs
/jobs       [POST]      => submit a job
/jobs/{ID}  [GET]       => get a specific job
/jobs/{ID}  [DELETE]    => kill a job if it is running
"""


async def run_pipeline(
    job_id: str,
    extractor: BaseExtractor,
    transformer: BaseTransformer,
    loader: BaseLoader,
    db: JobStatusDatabaseDependency,
    pipeline_registry: PipelineRegistryDep = None
):
    # TODO: completion POST callback if job includes a callback URL (success, errors, warnings)

    try:
        db.update_status(job_id, JobStatusType.EXTRACTING)
        extract_df = extractor.extract()

        db.update_status(job_id, JobStatusType.TRANSFORMING)
        transform_df = transformer.transform(extract_df)

        db.update_status(job_id, JobStatusType.LOADING)
        await loader.load(transform_df)

        db.update_status(job_id, JobStatusType.SUCCESS)

    except Exception as ex:
        db.update_status(job_id, JobStatusType.ERROR, str(ex))


# TODO: Use propper authorization checks instead of dep_public_endpoint before deploying.
# Should use authz_middleware.dep_require_permissions_on_resource and at the endpoint level instead of the router.
job_router = APIRouter(
    prefix="/jobs", dependencies=[authz_middleware.dep_public_endpoint()]
)


@job_router.post("", dependencies=[])
async def submit_job(
    job: Job,
    bt: BackgroundTasks,
    extractor: ExtractorDep,
    transformer: TransformerDep,
    loader: LoaderDep,
    db: JobStatusDatabaseDependency,
):
    import os
    import json
    batch_size = 4
    caller_path =os.path.dirname(__file__)
    file_path = os.path.join(caller_path, "phenopackets_result.json")
    with open(file_path) as f:
        json_content = json.load(f)
        response = await loader.load(json_content)
    job_id = db.create_status(job.model_dump()).id
    bt.add_task(run_pipeline, job_id, extractor, transformer, loader, db)
    return {"message": f"Running ETL job in the background {job_id}"}


@job_router.get("", response_model=list[JobStatus])
async def get_all_status(
    db: JobStatusDatabaseDependency,
):
    return db.get_all_status()


@job_router.get("/{job_id}", response_model=JobStatus)
async def get_status(
    job_id: uuid.UUID,
    db: JobStatusDatabaseDependency,
):
    return db.get_status(job_id)


@job_router.delete("/{job_id}")
async def delete_status(job_id: uuid.UUID, db: JobStatusDatabaseDependency):
    db.delete_status(job_id)
    return {"message": f"Job {job_id} has been deleted"}
