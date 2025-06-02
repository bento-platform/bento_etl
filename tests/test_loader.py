import os
import json
import pytest
from bento_etl.loaders.phenopackets_loader import PhenopacketsLoader

@pytest.mark.anyio
async def test_phenopackets_loader(logger, config):
    # Placeholder test
    dataset_id = "16543a4a-cee9-4e3d-9826-fcb1e0e5a292" # From bento portal; moose project
    batch_size = 4
    loader = PhenopacketsLoader(logger, config, dataset_id, batch_size)

    
    caller_path =os.path.dirname(__file__)
    file_path = os.path.join(caller_path, "data/synthetic_phenopackets_v2.json")
    
    with open(file_path) as f:
        json_content = json.load(f)
        response = await loader.load(json_content)
        assert all(r.status_code == 204 for r in response)

