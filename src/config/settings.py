"""
config.settings
~~~~~~~~~~~~~~~
Centralizes all project configuration: .env loading, database
credentials, marketplace API keys, and token persistence.

No business logic should exist in this module.
"""

import json
import logging
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths — resolved absolutely from the project root
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent
"""Absolute path to the project root directory."""

_ENV_PATH: Path = PROJECT_ROOT / ".env"
_TOKENS_PATH: Path = PROJECT_ROOT / "tokens.json"
_CUSTOS_PATH: Path = PROJECT_ROOT / "material" / "custo.xlsx"
_CUSTOS_JSON_PATH: Path = PROJECT_ROOT / "material" / "produtos_custo.json"

# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------
load_dotenv(dotenv_path=_ENV_PATH)

logger = logging.getLogger(__name__)

# --- Marketplace API ---
APP_ID: str = os.getenv("MELI_APP_ID", "")
CLIENT_SECRET: str = os.getenv("MELI_CLIENT_SECRET", "")
REDIRECT_URI: str = os.getenv("MELI_REDIRECT_URI", "")
AUTHORIZATION_CODE: str = os.getenv("MELI_AUTH_CODE", "")

# --- Database ---
DB_HOST: str = os.getenv("DB_HOST")
DB_PORT: str = os.getenv("DB_PORT")
DB_USER: str = os.getenv("DB_USER")
DB_PASS: str = os.getenv("DB_PASS")
DB_NAME: str = os.getenv("DB_NAME")

# ---------------------------------------------------------------------------
# API constants (centralized to avoid magic numbers in modules)
# ---------------------------------------------------------------------------
API_BASE_URL: str = "https://api.mercadolibre.com"
ADS_BASE_URL: str = f"{API_BASE_URL}/advertising"
BILLING_BASE_URL: str = f"{API_BASE_URL}/billing/integration"

# --- HTTP timeouts ---
REQUEST_TIMEOUT: int = 15
SHIPPING_TIMEOUT: int = 10

# --- Retry & throttle ---
MAX_TOKEN_RETRIES: int = 3
MAX_NETWORK_RETRIES: int = 5
RETRY_DELAY_SECONDS: int = 5
THROTTLE_DELAY_SECONDS: float = 0.5
RATE_LIMIT_BACKOFF_SECONDS: int = 12

# --- Billing ---
BILLING_GROUPS: list[str] = ["ML", "MP"]

# --- Ads ---
ADS_CHUNK_DAYS: int = 1
ADS_BATCH_SIZE: int = 30

# ---------------------------------------------------------------------------
# Public path helpers
# ---------------------------------------------------------------------------

def get_caminho_custos() -> Path:
    """Returns the absolute path to the cost spreadsheet."""
    return _CUSTOS_PATH


def get_caminho_json_custos() -> Path:
    """Returns the absolute path to the JSON cost file."""
    return _CUSTOS_JSON_PATH


# ---------------------------------------------------------------------------
# Token management
# ---------------------------------------------------------------------------

def carregar_tokens() -> Optional[dict]:
    """Reads persisted tokens from the local token file.

    Returns:
        Dictionary with access_token, refresh_token and user_id,
        or None if the file does not exist.
    """
    if _TOKENS_PATH.exists():
        try:
            with open(_TOKENS_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to read token file: %s", exc)
    return None


def salvar_tokens(access_token: str, refresh_token: str, user_id: int) -> None:
    """Persists refreshed tokens locally for the next execution.

    Args:
        access_token: Current API access token.
        refresh_token: Token used for automatic renewal.
        user_id: Seller ID from the marketplace.
    """
    dados = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "user_id": user_id,
    }
    try:
        with open(_TOKENS_PATH, "w", encoding="utf-8") as f:
            json.dump(dados, f)
        logger.debug("Tokens persisted to %s", _TOKENS_PATH)
    except OSError as exc:
        logger.error("Failed to persist tokens: %s", exc)
        raise
