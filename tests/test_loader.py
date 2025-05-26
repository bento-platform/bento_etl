from fastapi.testclient import TestClient
from bento_etl.loaders.phenopackets_loader import PhenopacketsLoader

def test_phenopackets_loader(test_client: TestClient, logger):
    # Placeholder test

    loader = PhenopacketsLoader(logger)

    assert loader.load("{}") == 200


    assert True