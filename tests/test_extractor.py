
import httpx
import json

import pytest

from bento_etl.extractors.api_fetch_extractor import ApiPollExtractor

@pytest.fixture
def mock_success_call(monkeypatch, load_phenopacket_data):
    phenopacket_content = json.dumps(load_phenopacket_data).encode('utf-8')
    monkeypatch.setattr(httpx.Client, "request", lambda *args: httpx.Response(200, content=phenopacket_content))

@pytest.fixture
def mock_bad_status_code(monkeypatch):
    monkeypatch.setattr("httpx.Client.request", lambda *args,: httpx.Response(400))

@pytest.fixture
def mock_valid_empty_response(monkeypatch):
    monkeypatch.setattr("httpx.Client.request", lambda *args: httpx.Response(200, content=b''))


class TestApiFetchExtractor:
    def test_extract_valid(self, logger, config, load_phenopacket_data, mock_success_call):
        extractor = ApiPollExtractor(logger, config, "valid_url", 0, "GET")
        response = extractor.extract()
        assert response == load_phenopacket_data

    def test_extract_invalid_bad_status_code(self, logger, config, mock_bad_status_code):
        extractor = ApiPollExtractor(logger, config, "invalid_url", 0, "GET")
        with pytest.raises(Exception, match="400"):
            extractor.extract()

    def test_extract_invalid_empty_response(self, logger, config, mock_valid_empty_response):
        extractor = ApiPollExtractor(logger, config, "empty_url", 0, "GET")
        with pytest.raises(Exception):
            extractor.extract()
