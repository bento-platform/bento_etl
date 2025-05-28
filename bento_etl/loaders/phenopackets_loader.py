import httpx
from fastapi import status
from .base import BaseLoader

class PhenopacketsLoader(BaseLoader):

    def __init__(self, logger):
        super().__init__(logger)

    def load(self, data):
        # ASSUME FOR NOW: data is json and VALID dataset with valid id
        dataset_id = "16543a4a-cee9-4e3d-9826-fcb1e0e5a292" # From bento portal; moose project
        
        # TODO UNHARDCODE later
        katsu_enpoint = f'http://localhost:8000/ingest/{dataset_id}/phenopackets_json'
        
        with httpx.Client() as client:
            r = client.request("POST", url=katsu_enpoint, json=data)
            return r.status_code
            #if r.status_code != status.HTTP_200_OK: