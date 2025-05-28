import os
import json
import inspect
from fastapi.testclient import TestClient
from bento_etl.loaders.phenopackets_loader import PhenopacketsLoader

def test_phenopackets_loader(test_client: TestClient, logger, config):
    # Placeholder test
    dataset_id = "16543a4a-cee9-4e3d-9826-fcb1e0e5a292" # From bento portal; moose project
    loader = PhenopacketsLoader(logger, config, dataset_id)

    
    caller_path =os.path.dirname(__file__)
    file_path = os.path.join(caller_path, "data/synthetic_phenopackets_v2.json")
    
    with open(file_path) as f:
        json_content = json.load(f)[0]
        assert loader.load(json_content) == 204
