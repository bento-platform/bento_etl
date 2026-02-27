import boto3
import json
from botocore.response import StreamingBody

from logging import Logger
from bento_etl.extractors.base import BaseExtractor
from bento_etl.models import S3ExtractStep
from bento_etl.config import Config


class S3Extractor(BaseExtractor):
    def __init__(self, logger: Logger, config: Config, ext_config: S3ExtractStep):
        self.bucket = config.s3_bucket
        self.object_key = ext_config.object_key

        self.s3_client = boto3.client("s3")
        super().__init__(logger)

    def _parse_body(self, body: StreamingBody) -> list[dict]:
        if self.object_key.endswith(".json"):
            self.logger.info("Parsing object as JSON")
            return json.loads(body.read())
        elif self.object_key.endswith(".jsonl"):
            self.logger.info("Parsing object as new-line-delimited JSON (JSONL)")
            data = []
            for line_bytes in body.iter_lines():
                line_str = line_bytes.decode("utf-8")
                data.append(json.loads(line_str))
            return data
        else:
            file_ext = self.object_key.split(".")[-1]
            raise Exception(f"No parsing method supports file extension: {file_ext}")

    def extract(self):
        response: dict = self.s3_client.get_object(
            Bucket=self.bucket, Key=self.object_key
        )

        body: StreamingBody = response["Body"]
        return self._parse_body(body)
