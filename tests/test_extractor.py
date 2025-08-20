import pytest

from bento_etl.extractors.api_fetch_extractor import ApiPollExtractor


class TestApiFetchExtractor:
    def test_extract_valid(
        self, logger, config, load_phenopacket_data, mock_extractor_success_call
    ):
        extractor = ApiPollExtractor(logger, config, "http://valid_url", 0, "GET")
        response = extractor.extract()
        assert response == load_phenopacket_data

    def test_extract_invalid_bad_status_code(
        self, logger, config, mock_extractor_bad_status_code
    ):
        extractor = ApiPollExtractor(logger, config, "http://invalid_url", 0, "GET")
        with pytest.raises(Exception, match="400"):
            extractor.extract()

    def test_extract_invalid_empty_response(
        self, logger, config, mock_extractor_valid_empty_response
    ):
        extractor = ApiPollExtractor(logger, config, "http://empty_url", 0, "GET")
        with pytest.raises(Exception):
            extractor.extract()
