from bento_lib.auth.middleware.fastapi import FastApiAuthMiddleware
import httpx
from .config import Config, get_config
from .logger import get_logger

__all__ = [
    "authz_middleware",
    "get_bearer_token_from_config",
    "get_bearer_token",
]

config = get_config()
authz_middleware = FastApiAuthMiddleware.build_from_fastapi_pydantic_config(
    config, get_logger()
)


def get_bearer_token_from_config(config: Config) -> str:
    return get_bearer_token(
        config.bento_openid_config_url,
        config.etl_client_id,
        config.etl_client_secret,
        config.bento_validate_ssl,
    )


def get_bearer_token(
    openid_config_url: str, client_id: str, client_secret: str, validate_ssl: bool
) -> str:
    openid_config = httpx.get(openid_config_url, verify=validate_ssl).json()

    token_res = httpx.post(
        openid_config["token_endpoint"],
        verify=validate_ssl,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )

    return f"Bearer {token_res.json()['access_token']}"
