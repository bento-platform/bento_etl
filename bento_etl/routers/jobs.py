import uuid
from fastapi import APIRouter, BackgroundTasks, HTTPException, status

from bento_etl.authz import authz_middleware
from bento_etl.db import DatabaseDependency
from bento_etl.extractors.base import BaseExtractor
from bento_etl.extractors.dependencies import ExtractorDep
from bento_etl.loaders.base import BaseLoader
from bento_etl.loaders.dependencies import LoaderDep
from bento_etl.models import Job, JobStatus, JobStatusType
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
    db: DatabaseDependency,
):
    # TODO: Run pipelines as a background task, figure out dep injection for ETL components
    # TODO: Update job state in the DB after each step to reflect progression status
    # TODO: Pass error messages/more informative info to be persisted in case of error
    # TODO: completion POST callback if job includes a callback URL (success, errors, warnings)

    try:
        db.change_job_status(job_id, JobStatusType.EXTRACTING)
        extract_df = extractor.extract()

        db.change_job_status(job_id, JobStatusType.TRANSFORMING)
        transform_df = transformer.transform(extract_df)

        db.change_job_status(job_id, JobStatusType.LOADING)
        await loader.load(transform_df)

        db.change_job_status(job_id, JobStatusType.SUCCESS)

    except Exception as ex:
        db.change_job_status(job_id, JobStatusType.ERROR, str(ex))



# TODO: Use propper authorization checks instead of dep_public_endpoint before deploying.
# Should use authz_middleware.dep_require_permissions_on_resource and at the endpoint level instead of the router.
job_router = APIRouter(prefix="/jobs", dependencies=[authz_middleware.dep_public_endpoint()])

@job_router.post("", dependencies=[])
async def submit_job(
    job: Job,
    bt: BackgroundTasks,
    extractor: ExtractorDep,
    transformer: TransformerDep,
    loader: LoaderDep,
    db: DatabaseDependency,
):
    job.id = db.create_job_status().id
    bt.add_task(run_pipeline, job.id, extractor, transformer, loader, db)
    return {"message": f"Running ETL job in the background {job.id}"}


@job_router.get("", response_model=list[JobStatus])
async def get_all_job_status(
    db: DatabaseDependency,
):
    return db.get_all_job_status()


@job_router.get("/{job_id}", response_model=JobStatus)
async def get_job_status(
    job_id:uuid.UUID,
    db: DatabaseDependency,
):
    job = db.get_job_status(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found in database")
    return job


@job_router.delete("/{job_id}")
async def delete_job(
    job_id:uuid.UUID,
    db: DatabaseDependency
):
    # TODO kill the job if it is running
    db.delete_job(job_id)
    return {"message": f"Job {job_id} has been deleted"}

