import json
from fastapi import APIRouter
from fastapi.responses import JSONResponse

__all__ = ["test_data_source_router"]

PHENOPACKETS_FILE_PATH = "tests/data/synthetic_phenopackets_v2.json"
EXPERIMENTS_FILE_PATH = "tests/data/synthetic_experiments.json"

test_data_source_router = APIRouter(prefix="/test/data-sources")


@test_data_source_router.get(
    "/pheno-clean",
    description="Returns a JSON response with a couple of phenopackets",
    summary="Dummy API data source for valid phenopackets",
)
def get_phenopackets():
    data = {}
    with open(PHENOPACKETS_FILE_PATH, mode="rb") as f:
        data = json.load(f)
    return JSONResponse(data)


@test_data_source_router.get(
    "/exps-clean",
    description="Returns a JSON response with a couple of experiments",
    summary="Dummy API data source for valid experiments",
)
def get_experiments():
    data = {}
    with open(EXPERIMENTS_FILE_PATH, mode="rb") as f:
        data = json.load(f)
    return JSONResponse(data)
