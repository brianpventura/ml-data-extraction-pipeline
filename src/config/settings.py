"""
config.settings
~~~~~~~~~~~~~~~
Centralizes all project configuration: .env loading, database
credentials, marketplace API keys, and token persistence.

Supports multi-tenant architecture: the environment file is loaded
lazily via ``inicializar(env_file)`` so that the correct store
credentials are in ``os.environ`` before any setting is read.

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

# Token path is resolved dynamically via _get_tokens_path()
_CUSTOS_PATH: Path = PROJECT_ROOT / "material" / "custo.xlsx"
_CUSTOS_JSON_PATH: Path = PROJECT_ROOT / "material" / "produtos_custo.json"

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Multi-tenant initializer
# ---------------------------------------------------------------------------
_inicializado: bool = False


def inicializar(env_file: str) -> None:
    """Loads the tenant-specific .env file and populates module-level settings.

    Must be called exactly once before any other module reads the
    configuration constants (APP_ID, DB_HOST, etc.).

    Args:
        env_file: Absolute or relative path to the .env file
                  (e.g. '.env.prohair').
    """
    global _inicializado
    global APP_ID, CLIENT_SECRET, REDIRECT_URI, AUTHORIZATION_CODE
    global DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME

    load_dotenv(dotenv_path=env_file, override=True)

    # --- Marketplace API ---
    APP_ID = os.getenv("MELI_APP_ID", "")
    CLIENT_SECRET = os.getenv("MELI_CLIENT_SECRET", "")
    REDIRECT_URI = os.getenv("MELI_REDIRECT_URI", "")
    AUTHORIZATION_CODE = os.getenv("MELI_AUTH_CODE", "")

    # --- Database ---
    DB_HOST = os.getenv("DB_HOST", "")
    DB_PORT = os.getenv("DB_PORT", "")
    DB_USER = os.getenv("DB_USER", "")
    DB_PASS = os.getenv("DB_PASS", "")
    DB_NAME = os.getenv("DB_NAME", "")

    _inicializado = True
    logger.info("Settings initialized from: %s", env_file)


# ---------------------------------------------------------------------------
# Defaults (populated after inicializar() is called)
# ---------------------------------------------------------------------------
APP_ID: str = ""
CLIENT_SECRET: str = ""
REDIRECT_URI: str = ""
AUTHORIZATION_CODE: str = ""

DB_HOST: str = ""
DB_PORT: str = ""
DB_USER: str = ""
DB_PASS: str = ""
DB_NAME: str = ""

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
# Token management (per-tenant via _TOKENS_PATH)
# ---------------------------------------------------------------------------

def _get_tokens_path() -> Path:
    """Returns the token file path configured in the current tenant's environment.
    """
    token_file = os.getenv('MELI_TOKEN_FILE_PATH')
    
    if not token_file:
        raise ValueError("A variável 'MELI_TOKEN_FILE_PATH' não foi encontrada no ambiente carregado.")
        
    path = Path(token_file)
    return path if path.is_absolute() else PROJECT_ROOT / path


def carregar_tokens() -> Optional[dict]:
    """Reads persisted tokens from the local token file.

    Returns:
        Dictionary with access_token, refresh_token and user_id,
        or None if the file does not exist.
    """
    path = _get_tokens_path()
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
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
    path = _get_tokens_path()
    dados = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "user_id": user_id,
    }
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(dados, f)
        logger.debug("Tokens persisted to %s", path)
    except OSError as exc:
        logger.error("Failed to persist tokens: %s", exc)
        raise



