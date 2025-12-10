import pytest
from aioresponses import aioresponses
import httpx
from bento_etl.authz import get_bearer_token, get_bearer_token_from_config
from bento_etl.config import Config


def test_get_bearer_token(monkeypatch):
    """Test getting a bearer token from OpenID configuration."""
    # Mock the httpx.get and httpx.post calls
    def mock_get(url, verify=True):
        return httpx.Response(
            200,
            json={"token_endpoint": "https://auth.example.com/token"}
        )

    def mock_post(url, verify=True, data=None):
        return httpx.Response(
            200,
            json={"access_token": "test_access_token"}
        )

    # Remove the autouse mock temporarily for this test
    monkeypatch.setattr("httpx.get", mock_get)
    monkeypatch.setattr("httpx.post", mock_post)

    token = get_bearer_token(
        "https://auth.example.com/.well-known/openid-configuration",
        "test_client_id",
        "test_client_secret",
        True
    )

    assert token == "Bearer test_access_token"


def test_get_bearer_token_from_config(config: Config, monkeypatch):
    """Test getting a bearer token from config."""
    # Mock the get_bearer_token function
    def mock_get_bearer_token(openid_config_url, client_id, client_secret, validate_ssl):
        return f"Bearer {client_id}_{client_secret}"

    monkeypatch.setattr(
        "bento_etl.authz.get_bearer_token",
        mock_get_bearer_token
    )

    token = get_bearer_token_from_config(config)

    assert token.startswith("Bearer")
