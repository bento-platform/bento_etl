import requests
from .base import BaseLoader

class PhenopacketsLoader(BaseLoader):

    def __init__(self, logger):
        super().__init__(logger)

    def load(self, data):
        # POST request to Katsu
        # Must post to /api/datasets? to create a dataset (and an id) ~~~ manually get the datset uid for now from Data Manager

        dataset_id = "0440cdb8-6275-46e9-9515-34300d3353f5" #TODO get automatically

        # URL format: 'ingest/<str:dataset_id (uuid)>/<str:workflow_id>'  |  json
        #   ~{katsu_url}/ingest/${dataset_id}/phenopackets_json") 

        # Send a request to see if it works :)
        try:
            r = requests.get("https://google.com")
            #r = requests.post("katsu_server_url" + "/api/projects", json={})
            return r.status_code
        except requests.exceptions.ConnectionError:
            print(":(")
            #print(
            #    "Connection to the API server {} cannot be established.".format(
            #        katsu_server_url
            #    )
            #)
        #raise NotImplementedError()