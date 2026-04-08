"""
config.settings
~~~~~~~~~~~~~~~
Centraliza toda a configuração do projeto: leitura do .env,
credenciais do banco de dados, chaves da API do Mercado Livre
e gerenciamento do arquivo tokens.json.

Nenhuma regra de negócio deve existir neste módulo.
"""

import json
import logging
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths — resolvidos de forma absoluta a partir da raiz do projeto
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent
"""Caminho absoluto até a raiz do projeto (data_mercadoLivre/)."""

_ENV_PATH: Path = PROJECT_ROOT / ".env"
_TOKENS_PATH: Path = PROJECT_ROOT / "tokens.json"
_CUSTOS_PATH: Path = PROJECT_ROOT / "material" / "custo.xlsx"
_CUSTOS_JSON_PATH: Path = PROJECT_ROOT / "material" / "produtos_custo.json"

# ---------------------------------------------------------------------------
# Carrega variáveis de ambiente
# ---------------------------------------------------------------------------
load_dotenv(dotenv_path=_ENV_PATH)

logger = logging.getLogger(__name__)

# --- Mercado Livre ---
APP_ID: str = os.getenv("MELI_APP_ID", "")
CLIENT_SECRET: str = os.getenv("MELI_CLIENT_SECRET", "")
REDIRECT_URI: str = os.getenv("MELI_REDIRECT_URI", "")
AUTHORIZATION_CODE: str = os.getenv("MELI_AUTH_CODE", "")

# --- Banco de Dados ---
DB_HOST: str = os.getenv("DB_HOST")
DB_PORT: str = os.getenv("DB_PORT")
DB_USER: str = os.getenv("DB_USER")
DB_PASS: str = os.getenv("DB_PASS")
DB_NAME: str = os.getenv("DB_NAME")

# ---------------------------------------------------------------------------
# helpers públicos para caminhos
# ---------------------------------------------------------------------------

def get_caminho_custos() -> Path:
    """Retorna o caminho absoluto da planilha de custos."""
    return _CUSTOS_PATH


def get_caminho_json_custos() -> Path:
    """Retorna o caminho absoluto do JSON de custos."""
    return _CUSTOS_JSON_PATH


# ---------------------------------------------------------------------------
# Gerenciamento de tokens
# ---------------------------------------------------------------------------

def carregar_tokens() -> Optional[dict]:
    """Lê o arquivo tokens.json salvo localmente com os últimos tokens.

    Returns:
        Dicionário com access_token, refresh_token e user_id, ou None
        caso o arquivo não exista.
    """
    if _TOKENS_PATH.exists():
        try:
            with open(_TOKENS_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Falha ao ler tokens.json: %s", exc)
    return None


def salvar_tokens(access_token: str, refresh_token: str, user_id: int) -> None:
    """Persiste os novos tokens em tokens.json para a próxima execução.

    Args:
        access_token: Token de acesso à API.
        refresh_token: Token de refresh para renovação automática.
        user_id: ID do vendedor no Mercado Livre.
    """
    dados = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "user_id": user_id,
    }
    try:
        with open(_TOKENS_PATH, "w", encoding="utf-8") as f:
            json.dump(dados, f)
        logger.debug("Tokens salvos com sucesso em %s", _TOKENS_PATH)
    except OSError as exc:
        logger.error("Falha ao salvar tokens.json: %s", exc)
        raise
