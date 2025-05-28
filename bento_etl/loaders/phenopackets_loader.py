import httpx
from fastapi import status
from .base import BaseLoader

class PhenopacketsLoader(BaseLoader):
    def __init__(self, logger, config, dataset_id):
        super().__init__(logger, config)
        self.dataset_id = dataset_id

    def load(self, data):
        katsu_ingest_url = f'http://{self.config.katsu_endpoint}/ingest/{self.dataset_id}/phenopackets_json'
        
        with httpx.Client() as client:
            r = client.request("POST", url=katsu_ingest_url, json=data)
            return r.status_code
            #if r.status_code != status.HTTP_200_OK: