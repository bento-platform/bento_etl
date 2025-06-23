import uuid
import httpx
import pytest
from unittest.mock import patch
from bento_etl import authz
from bento_etl.loaders.phenopackets_loader import PhenopacketsLoader


class TestPhenopacketsLoader:
    @pytest.fixture
    def mock_response(self, monkeypatch):
        def mock_get(*args, **kwargs):
            return ""
        monkeypatch.setattr(authz, "get_bearer_token", mock_get)
        
        async def mockreturn2(*args, **kwargs):
                    return httpx.Response(204)
        monkeypatch.setattr(httpx.AsyncClient, "post", mockreturn2)
    
    
    
    # TODO test for batch_size
    @pytest.mark.asyncio
    async def test_valid_load(self, logger, config, mock_response):
        loader = PhenopacketsLoader(logger, config, uuid.uuid4(), 4)
        await loader.load("{}")

    @pytest.mark.asyncio
    async def test_invalid_dataset_id(self, logger, config, mock_response):
        loader = PhenopacketsLoader(logger, config, "BAD_DATASET_ID", 4)

        with pytest.raises(Exception, match="400"):
            await loader.load("{}")

    @pytest.mark.asyncio
    async def test_invalid_data(self, logger, config, mock_response):
        loader = PhenopacketsLoader(logger, config, uuid.uuid4(), 4)

        with pytest.raises(Exception, match="400"):
            await loader.load("BAD_JSON")
