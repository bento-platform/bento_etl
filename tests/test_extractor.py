import pytest
from unittest.mock import MagicMock

from bento_etl.extractors.api_fetch_extractor import ApiPollExtractor
from bento_etl.extractors.s3_extractor import S3Extractor
from bento_etl.extractors.base import BaseExtractor
from bento_etl.extractors.dependencies import get_extractor
from bento_etl.models import (
    Job,
    LoadStep,
    TransformStep,
    ApiFetchExtractStep,
    S3ExtractStep,
)
from bento_etl.config import Config


def mock_job_with_api_fetch_extractor():
    return Job(
        extractor=ApiFetchExtractStep(
            extract_url="some_url",
            type="api-fetch",
            http_verb="GET",
            expected_status_code=200,
        ),
        transformer=TransformStep(type="None"),
        loader=LoadStep(dataset_id="some_id", batch_size=0, data_type="experiments"),
    )


def mock_job_with_s3_extractor():
    return Job(
        extractor=S3ExtractStep(object_key="some-object.jsonl"),
        transformer=TransformStep(type="None"),
        loader=LoadStep(dataset_id="some_id", batch_size=0, data_type="experiments"),
    )


class TestExtractorDependencies:
    def test_get_extractor_api_fetch(self, logger, config: Config):
        job = mock_job_with_api_fetch_extractor()
        extractor = get_extractor(job, logger, config)
        assert isinstance(extractor, ApiPollExtractor)

    def test_get_extractor_s3(self, logger, config: Config):
        job = mock_job_with_s3_extractor()
        extractor = get_extractor(job, logger, config)
        assert isinstance(extractor, S3Extractor)

    def test_get_extractor_invalid_type(self, logger, config: Config):
        """Test that get_extractor raises NotImplementedError for invalid extractor type."""
        job = MagicMock()
        job.extractor = MagicMock()
        job.extractor.type = "invalid-type"

        with pytest.raises(NotImplementedError):
            get_extractor(job, logger, config)


class TestBaseExtractor:
    def test_extract_raises_not_implemented(self, logger):
        """Test that base extractor's extract method raises NotImplementedError."""
        extractor = BaseExtractor(logger)

        with pytest.raises(NotImplementedError):
            extractor.extract()


class TestApiFetchExtractor:
    def test_extract_valid(
        self, logger, load_phenopacket_data, mock_extractor_success_call
    ):
        extractor = ApiPollExtractor(logger, "http://valid_url")
        response = extractor.extract()
        assert response == load_phenopacket_data

    def test_extract_invalid_bad_status_code(
        self, logger, mock_extractor_bad_status_code
    ):
        extractor = ApiPollExtractor(logger, "http://invalid_url")
        with pytest.raises(Exception, match="400"):
            extractor.extract()

    def test_extract_invalid_empty_response(
        self, logger, mock_extractor_valid_empty_response
    ):
        extractor = ApiPollExtractor(logger, "http://empty_url")
        with pytest.raises(Exception):
            extractor.extract()


class TestS3Extractor:
    def test_extract_valid_json(
        self,
        logger,
        config,
        load_phenopacket_data,
        mock_s3_extractor_pheno_json,
        mocked_s3,
    ):
        # Pheno data in JSONL
        extractor = S3Extractor(
            logger, config, S3ExtractStep(object_key="phenopackets.json")
        )
        response = extractor.extract()
        assert response == load_phenopacket_data

    def test_extract_valid_jsonl(
        self,
        logger,
        config,
        load_phenopacket_data,
        mock_s3_extractor_pheno_jsonl,
        mocked_s3,
    ):
        # Pheno data in JSONL
        extractor = S3Extractor(
            logger, config, S3ExtractStep(object_key="phenopackets.jsonl")
        )
        response = extractor.extract()
        assert response == load_phenopacket_data
