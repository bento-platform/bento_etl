import pytest

from bento_etl.extractors.api_fetch_extractor import ApiPollExtractor
from bento_etl.extractors.dependencies import get_extractor
from bento_etl.models import ExtractStep, Job, LoadStep, TransformStep


def mock_job_with_api_fetch_extractor():
    return Job(
        extractor=ExtractStep(extract_url="some_url", frequency_ms=0, type="api-fetch"),
        transformer=TransformStep(type="None"),
        loader=LoadStep(dataset_id="some_id", batch_size=0, data_type="experiments"),
    )


class TestExtractorDependencies:
    def test_get_extractor_api_fetch(self, logger):
        job = mock_job_with_api_fetch_extractor()
        extractor = get_extractor(job, logger)
        assert type(extractor) is ApiPollExtractor


class TestApiFetchExtractor:
    def test_extract_valid(
        self, logger, load_phenopacket_data, mock_extractor_success_call
    ):
        extractor = ApiPollExtractor(logger, "http://valid_url", 0, "GET")
        response = extractor.extract()
        assert response == load_phenopacket_data

    def test_extract_invalid_bad_status_code(
        self, logger, mock_extractor_bad_status_code
    ):
        extractor = ApiPollExtractor(logger, "http://invalid_url", 0, "GET")
        with pytest.raises(Exception, match="400"):
            extractor.extract()

    def test_extract_invalid_empty_response(
        self, logger, mock_extractor_valid_empty_response
    ):
        extractor = ApiPollExtractor(logger, "http://empty_url", 0, "GET")
        with pytest.raises(Exception):
            extractor.extract()
