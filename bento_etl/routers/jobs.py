from fastapi import APIRouter, BackgroundTasks, status

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

# TODO: Job model describing a submitable ETL pipeline job
# TODO: Add DB to keep track of jobs


def run_pipeline(
    job_id: str,
    extractor: BaseExtractor,
    transformer: BaseTransformer,
    loader: BaseLoader,
    db: DatabaseDependency,
):
    # TODO: Run pipelines as a background task, figure out dep injection for ETL components
    # TODO: Update job state in the DB after each step to reflect progression status
    # TODO: Pass error messages/more informative info to be persisted in case of error
    db.change_job_status(job_id, JobStatusType.EXTRACTING)
    extracted_df = extractor.extract()
    if extracted_df:
        db.change_job_status(job_id, JobStatusType.TRANSFORMING)
        transformed_df = transformer.transform(extracted_df)
        if transformed_df:
            db.change_job_status(job_id, JobStatusType.LOADING)
            loader.load(transformed_df)
            db.change_job_status(job_id, JobStatusType.SUCCESS)
        else:
            # TODO: log error and abort with callback
            db.change_job_status(job_id, JobStatusType.ERROR, "Issue with transforming step")
    else:
        # TODO: log error and abort with callback
        db.change_job_status(job_id, JobStatusType.ERROR, "Issue with extracting step")
    # TODO: completion POST callback if job includes a callback URL (success, errors, warnings)


job_router = APIRouter(prefix="/jobs")


@job_router.post("")
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
