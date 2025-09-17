import json
import os
import uuid
from fastapi import APIRouter, BackgroundTasks, HTTPException

from bento_lib.auth.permissions import P_DELETE_DATA, P_INGEST_DATA
from bento_lib.auth.resources import RESOURCE_EVERYTHING

from bento_etl.authz import authz_middleware
from bento_etl.config import ConfigDependency
from bento_etl.db import JobStatusDatabaseDependency
from bento_etl.extractors.base import BaseExtractor
from bento_etl.extractors.dependencies import ExtractorDep, get_extractor
from bento_etl.loaders.base import BaseLoader
from bento_etl.loaders.dependencies import LoaderDep, get_loader
from bento_etl.logger import LoggerDependency
from bento_etl.models import Job, JobStatus, JobStatusType
from bento_etl.transformers.base import BaseTransformer
from bento_etl.transformers.dependencies import TransformerDep, get_transformer

DEPENDENCY_INGEST_DATA = authz_middleware.dep_require_permissions_on_resource(
    frozenset({P_INGEST_DATA}), RESOURCE_EVERYTHING
)
DEPENDENCY_DELETE_DATA = authz_middleware.dep_require_permissions_on_resource(
    frozenset({P_DELETE_DATA}), RESOURCE_EVERYTHING
)

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
):
    # TODO: completion POST callback if job includes a callback URL (success, errors, warnings)

    try:
        db.update_status(job_id, JobStatusType.EXTRACTING)
        data = extractor.extract()

        if transformer:
            db.update_status(job_id, JobStatusType.TRANSFORMING)
            data = transformer.transform(data)

        db.update_status(job_id, JobStatusType.LOADING)
        await loader.load(data)

        db.update_status(job_id, JobStatusType.SUCCESS)

    except Exception as ex:
        db.update_status(job_id, JobStatusType.ERROR, str(ex))


job_router = APIRouter(prefix="/jobs")


# TODO replace
# @job_router.post("", dependencies=[DEPENDENCY_INGEST_DATA])
@job_router.post("", dependencies=[authz_middleware.dep_public_endpoint()])
async def submit_job(
    job: Job,
    bt: BackgroundTasks,
    extractor: ExtractorDep,
    transformer: TransformerDep,
    loader: LoaderDep,
    db: JobStatusDatabaseDependency,
):
    job_id = db.create_status(job.model_dump()).id
    bt.add_task(run_pipeline, job_id, extractor, transformer, loader, db)
    return {"message": f"Running ETL job in the background {job_id}"}


# TODO replace
# @job_router.post("", dependencies=[DEPENDENCY_INGEST_DATA])
@job_router.post(
    "/pipeline/{pipeline_file_name}",
    dependencies=[authz_middleware.dep_public_endpoint()],
)
async def run_from_pipeline_file(
    pipeline_file_name: str,
    bt: BackgroundTasks,
    db: JobStatusDatabaseDependency,
    logger: LoggerDependency,
    config: ConfigDependency,
):
    pipeline_file_path = os.path.join(
        os.getcwd(), f"pipelines/{pipeline_file_name}.json"
    )

    try:
        with open(pipeline_file_path) as file:
            file_content = json.load(file)
            job = Job.model_validate(file_content)
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Pipeline file not found or malformed: {e}"
        )

    extractor = get_extractor(job, logger)
    transformer = get_transformer(job, logger, config)
    loader = get_loader(job, logger, config)

    job_id = db.create_status(job.model_dump()).id
    bt.add_task(run_pipeline, job_id, extractor, transformer, loader, db)
    return {"message": f"Running ETL job in the background {job_id}"}


@job_router.get(
    "",
    response_model=list[JobStatus],
    dependencies=[authz_middleware.dep_public_endpoint()],
)
async def get_all_status(
    db: JobStatusDatabaseDependency,
):
    return db.get_all_status()


@job_router.get(
    "/{job_id}",
    response_model=JobStatus,
    dependencies=[authz_middleware.dep_public_endpoint()],
)
async def get_status(
    job_id: uuid.UUID,
    db: JobStatusDatabaseDependency,
):
    return db.get_status(job_id)


@job_router.delete("/{job_id}", dependencies=[DEPENDENCY_DELETE_DATA])
async def delete_status(job_id: uuid.UUID, db: JobStatusDatabaseDependency):
    db.delete_status(job_id)
    return {"message": f"Job {job_id} has been deleted"}
