import boto3
import json
from botocore.response import StreamingBody

from logging import Logger
from bento_etl.extractors.base import BaseExtractor
from bento_etl.models import S3ExtractStep
from bento_etl.config import Config


class S3Extractor(BaseExtractor):

    def __init__(self, logger: Logger, config: Config, ext_config: S3ExtractStep):
        s3_protocol = "https" if config.s3_use_https else "http"

        # S3 endpoint specific config
        self.endpoint = f"{s3_protocol}://{config.s3_endpoint}"
        self.region = config.s3_region
        self.access_key = config.s3_access_key
        self.secret_key = config.s3_secret_key
        self.validate_ssl = config.s3_validate_ssl
        self.use_https = config.s3_use_https

        allowed_buckets = config.s3_allowed_buckets

        # Extraction specific config
        self.bucket = ext_config.bucket_name
        self.object_key = ext_config.object_key

        # Raise if bucket is not in the allowed buckets list
        if allowed_buckets and ext_config.bucket_name not in allowed_buckets:
            raise Exception(f"Bucket {self.bucket} is not in the allowed buckets list")
        
        # Raise if allowed buckets list is undefined
        if not allowed_buckets:
            raise Exception("Config S3_ALLOWED_BUCKETS list is empty, allowed buckets must be explicitly configured.")

        super().__init__(logger)

    def _s3_client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            verify=self.validate_ssl,
        )

    def _parse_body(self, body: StreamingBody) -> dict:
        if self.object_key.endswith(".json"):
            return json.loads(body.read())
        elif self.object_key.endswith(".jsonl"):
            data = []
            for index, line_bytes in enumerate(body.iter_lines()):
                line_str = line_bytes.decode("utf-8")
                data[index] = line_str
            return data
        else:
            file_ext = self.object_key.split(".")[-1]
            raise Exception(f"No parsing method supports file extension: {file_ext}")

    def extract(self):
        s3 = self._s3_client()

        response: dict = s3.get_object(Bucket=self.bucket, Key=self.object_key)

        body: StreamingBody = response.get("Body")
        if not body:
            raise Exception(f"'Body' not found in response for key {self.object_key}")

        return self._parse_body(body)
