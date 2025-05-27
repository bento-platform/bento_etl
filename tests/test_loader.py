import os
import json
import inspect
from fastapi.testclient import TestClient
from bento_etl.loaders.phenopackets_loader import PhenopacketsLoader

def test_phenopackets_loader(test_client: TestClient, logger):
    # Placeholder test

    loader = PhenopacketsLoader(logger)
    
    
    caller_path =os.path.dirname(__file__)
    file_path = os.path.join(caller_path, "data/synthetic_phenopackets_v2.json")
    
    with open(file_path) as f:
        json_content = json.load(f)
        assert loader.load(json_content) == 200
