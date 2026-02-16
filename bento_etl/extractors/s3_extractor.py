import boto3
import json

from logging import Logger
from bento_etl.extractors.base import BaseExtractor
from bento_etl.models import S3ExtractStep
from bento_etl.config import Config

class S3Extractor(BaseExtractor):

    def __init__(self, logger: Logger, config: Config, ext_config: S3ExtractStep):
        # S3 endpoint specific config
        self.endpoint = config.s3_endpoint
        self.region = config.s3_region
        self.access_key = config.s3_access_key
        self.secret_key = config.s3_secret_key
        self.validate_ssl = config.s3_validate_ssl
        self.use_https = config.s3_use_https

        # Extraction specific config
        self.bucket = ext_config.bucket_name
        self.object_key = ext_config.object_key
        self.parse_as = ext_config.parse_as

        super().__init__(logger)

    def _parse(self, body: bytes):
        if self.parse_as == "jsonl":
            array = []
            with open(body, 'r', encoding='utf-8') as f:
                for line in f:
                    json_line = json.loads(line.strip())
                    array.append(json_line)
            return array
        elif self.parse_as == "json":
            return json.loads(body)

            
    def extract(self):
        s3 = boto3.client('s3')

        response: dict = s3.get_object(
            Bucket=self.bucket,
            Key=self.object_key
        )

        body_bytes: bytes = response['Body'].read()

        return self._parse(body_bytes)
        





        
        
