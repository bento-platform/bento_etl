import uuid
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Body
from bento_etl.authz import authz_middleware
from bento_etl.config import ConfigDependency
from bento_etl.db import JobStatusDatabaseDependency
from bento_etl.logger import LoggerDependency
from bento_etl.pipeline_config import EtlPipelines
from bento_etl.models import (
    Job,
    PipelineType,
    ExtractStep,
    TransformStep,
    LoadStep,
    JobStatus,
    JobStatusType,
)
from bento_etl.extractors.dependencies import get_extractor
from bento_etl.transformers.dependencies import get_transformer
from bento_etl.loaders.dependencies import get_loader

__all__ = ["job_router", "set_pipeline_registry"]

job_router = APIRouter(
    prefix="/jobs",
    dependencies=[authz_middleware.dep_public_endpoint()],
)

_PIPELINE_REGISTRY: EtlPipelines | None = None

def set_pipeline_registry(registry: EtlPipelines):
    global _PIPELINE_REGISTRY
    _PIPELINE_REGISTRY = registry

def get_pipeline_registry() -> EtlPipelines:
    if _PIPELINE_REGISTRY is None:
        raise RuntimeError("Pipeline registry not initialized. Call set_pipeline_registry first.")
    return _PIPELINE_REGISTRY

PipelineRegistryDep = Depends(get_pipeline_registry)

def build_job_from_pipeline(
    pipeline_type: PipelineType,
    registry: EtlPipelines,
) -> Job:
    pipeline_name = pipeline_type.value
    pipeline_cfg = registry.pipelines.get(pipeline_name)

    if not pipeline_cfg:
        raise HTTPException(
            status_code=400,
            detail=f"Pipeline '{pipeline_name}' not found"
        )
    return Job(
        id=str(uuid.uuid4()),
        pipeline_type=pipeline_type,
        extractor=ExtractStep(
            format="json",
            type=pipeline_cfg.extractor.type,
            endpoint=pipeline_cfg.extractor.endpoint,
            interval=pipeline_cfg.extractor.interval,
        ),
        transformer=TransformStep(
            plugin=pipeline_cfg.transformer.plugin,
        ),
        loader=LoadStep(
            dataset_id=pipeline_name,
            data_type=pipeline_type,
            expected_status_code=pipeline_cfg.loader.expected_status_code,
            batch_size=pipeline_cfg.loader.batch_size,
        ),
    )

async def run_pipeline(
    job_id: uuid.UUID,
    extractor,
    transformer,
    loader,
    db: JobStatusDatabaseDependency,
):
    try:
        db.change_status(job_id, JobStatusType.EXTRACTING)
        data = extractor.extract()  # Returns JSON payload (list[dict])

        db.change_status(job_id, JobStatusType.TRANSFORMING)
        transformed_data = transformer.transform(data)  # Returns JSON payload (list[dict])

        db.change_status(job_id, JobStatusType.LOADING)
        await loader.load(transformed_data)  # Accepts JSON payload

        db.change_status(job_id, JobStatusType.SUCCESS)
    except Exception as ex:
        db.change_status(job_id, JobStatusType.ERROR, str(ex))

@job_router.post("", summary="Submit a new ETL job")
async def submit_job(
    bt: BackgroundTasks,
    logger: LoggerDependency = LoggerDependency,
    config: ConfigDependency = ConfigDependency,
    db: JobStatusDatabaseDependency = JobStatusDatabaseDependency,
    pipeline_type: PipelineType = Query(
        ..., description="Which pipeline to run (phenopackets|experiments)"
    ),
    loader_config: LoadStep = Body(
        ..., embed=True,
        description="Loader configuration (dataset_id, data_type, expected_status_code, batch_size)"
    ),
    registry: EtlPipelines = PipelineRegistryDep,
):
    job = build_job_from_pipeline(pipeline_type, registry)
    
    job.loader = LoadStep(
        dataset_id=loader_config.dataset_id or job.loader.dataset_id,
        data_type=loader_config.data_type or job.loader.data_type,
        expected_status_code=loader_config.expected_status_code or job.loader.expected_status_code,
        batch_size=loader_config.batch_size or job.loader.batch_size,
    )

    status = db.create_status()
    job.id = status.id

    extractor = get_extractor(job, logger)
    transformer = get_transformer(job, logger)
    loader = get_loader(job, logger, config, registry)

    bt.add_task(run_pipeline, job.id, extractor, transformer, loader, db)

    return {
        "id": job.id,
        "pipeline_type": pipeline_type,
        "loader": job.loader,
        "status": status.status,
        "message": "ETL job is running in the background",
    }

@job_router.get("", response_model=list[JobStatus])
async def list_jobs(
    db: JobStatusDatabaseDependency,
):
    return db.get_all_status()

@job_router.get("/{job_id}", response_model=JobStatus)
async def get_job_status(
    job_id: uuid.UUID,
    db: JobStatusDatabaseDependency,
):
    status = db.get_status(job_id)
    if not status:
        raise HTTPException(
            status_code=404,
            detail=f"Job {job_id} not found"
        )
    return status

@job_router.delete("/{job_id}")
async def delete_job(
    job_id: uuid.UUID,
    db: JobStatusDatabaseDependency,
):
    db.delete_job_status(job_id)
    return {"message": f"Job {job_id} has been deleted"}