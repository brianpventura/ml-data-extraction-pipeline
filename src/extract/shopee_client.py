"""
extract.shopee_client
~~~~~~~~~~~~~~~~~~~~~
HTTP client for the Shopee Open Platform API (v2).
Handles HMAC-SHA256 authentication, OAuth shop authorization,
token management, and order retrieval.

Shopee API docs: https://open.shopee.com/developer-guide/4
"""

import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any, Optional
from urllib.parse import urlencode

import requests

from src.config.settings import REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# API constants
# ---------------------------------------------------------------------------
_API_HOST = "https://partner.shopeemobile.com"
_AUTH_PATH = "/api/v2/shop/auth_partner"
_TOKEN_PATH = "/api/v2/auth/token/get"
_REFRESH_TOKEN_PATH = "/api/v2/auth/access_token/get"
_ORDERS_PATH = "/api/v2/order/get_order_list"
_ORDER_DETAIL_PATH = "/api/v2/order/get_order_detail"


class ShopeeClient:
    """Client for the Shopee Open Platform REST API (v2).

    Authentication follows Shopee's HMAC-SHA256 signing scheme:
    every request must include a ``sign`` parameter computed from
    the partner key, path, timestamp, and (when applicable) the
    shop's access token and shop ID.

    Usage::

        client = ShopeeClient()
        auth_url = client.gerar_url_autenticacao()
        print(f"Authorize here: {auth_url}")
    """

    def __init__(self) -> None:
        """Initializes the client reading credentials from the environment.

        Required env vars:
            SHOPEE_PARTNER_ID: Numeric partner/app ID from Shopee.
            SHOPEE_PARTNER_KEY: Secret key for HMAC signing.

        Optional env vars:
            SHOPEE_REDIRECT_URI: OAuth callback URL.
            SHOPEE_TOKEN_FILE_PATH: Path to the JSON file for
                persisting shop tokens.

        Raises:
            ValueError: If SHOPEE_PARTNER_ID or SHOPEE_PARTNER_KEY
                        is missing from the environment.
        """
        partner_id_raw = os.getenv("SHOPEE_PARTNER_ID")
        partner_key_raw = os.getenv("SHOPEE_PARTNER_KEY")

        if not partner_id_raw:
            raise ValueError(
                "A variavel 'SHOPEE_PARTNER_ID' nao foi encontrada "
                "no ambiente carregado."
            )
        if not partner_key_raw:
            raise ValueError(
                "A variavel 'SHOPEE_PARTNER_KEY' nao foi encontrada "
                "no ambiente carregado."
            )

        self.partner_id: int = int(partner_id_raw)
        self.partner_key: bytes = partner_key_raw.encode("utf-8")
        self.redirect_uri: str = os.getenv("SHOPEE_REDIRECT_URI", "")
        self.token_file: str = os.getenv("SHOPEE_TOKEN_FILE_PATH", "")

        if not self.token_file:
            raise ValueError(
                "A variavel 'SHOPEE_TOKEN_FILE_PATH' nao foi encontrada "
                "no ambiente carregado."
            )

        # Runtime state (populated after authorization)
        self._access_token: str = ""
        self._refresh_token: str = ""
        self._shop_id: int = 0

        # Attempt to load persisted tokens
        self._carregar_tokens()

    # -------------------------------------------------------------------
    # Signature
    # -------------------------------------------------------------------
    def _gerar_assinatura(
        self,
        path: str,
        access_token: str = "",
        shop_id: int = 0,
    ) -> tuple[str, int]:
        """Generates the HMAC-SHA256 signature required by Shopee API v2.

        The base string varies depending on the call type:
        - **System-level** (auth, no token):
          ``{partner_id}{path}{timestamp}``
        - **Shop-level** (with token):
          ``{partner_id}{path}{timestamp}{access_token}{shop_id}``

        Args:
            path: API endpoint path (e.g. ``/api/v2/shop/auth_partner``).
            access_token: Shop access token (empty for system calls).
            shop_id: Numeric shop ID (0 for system calls).

        Returns:
            Tuple of ``(sign_hex, timestamp)``.
        """
        timestamp = int(time.time())

        base_string = f"{self.partner_id}{path}{timestamp}"
        if access_token and shop_id:
            base_string += f"{access_token}{shop_id}"

        sign = hmac.new(
            self.partner_key,
            base_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        return sign, timestamp

    # -------------------------------------------------------------------
    # OAuth — Authorization URL
    # -------------------------------------------------------------------
    def gerar_url_autenticacao(self) -> str:
        """Builds the full Shopee OAuth authorization URL.

        The seller must open this URL in a browser, log in, and
        authorize the partner app.  Shopee will then redirect to
        ``SHOPEE_REDIRECT_URI`` with ``code`` and ``shop_id`` query
        parameters.

        Returns:
            Complete authorization URL ready for the seller to visit.
        """
        sign, timestamp = self._gerar_assinatura(_AUTH_PATH)

        params = {
            "partner_id": self.partner_id,
            "timestamp": timestamp,
            "sign": sign,
            "redirect": self.redirect_uri,
        }

        url = f"{_API_HOST}{_AUTH_PATH}?{urlencode(params)}"
        logger.info("URL de autorizacao Shopee gerada com sucesso.")
        return url

    # -------------------------------------------------------------------
    # OAuth — Token exchange
    # -------------------------------------------------------------------
    def obter_token(self, code: str, shop_id: int) -> dict[str, Any]:
        """Exchanges the authorization code for access + refresh tokens.

        Args:
            code: Authorization code received via redirect callback.
            shop_id: Shop ID received via redirect callback.

        Returns:
            Dictionary with ``access_token``, ``refresh_token``,
            ``expire_in``, and ``shop_id``.

        Raises:
            requests.exceptions.RequestException: On network failure.
            ValueError: If the API returns an error payload.
        """
        sign, timestamp = self._gerar_assinatura(_TOKEN_PATH)

        params = {
            "partner_id": self.partner_id,
            "timestamp": timestamp,
            "sign": sign,
        }

        payload = {
            "code": code,
            "shop_id": shop_id,
            "partner_id": self.partner_id,
        }

        url = f"{_API_HOST}{_TOKEN_PATH}"
        response = requests.post(
            url, params=params, json=payload, timeout=REQUEST_TIMEOUT
        )

        dados = response.json()

        if dados.get("error"):
            raise ValueError(
                f"Shopee token error: {dados.get('error')} — "
                f"{dados.get('message', '')}"
            )

        self._access_token = dados.get("access_token", "")
        self._refresh_token = dados.get("refresh_token", "")
        self._shop_id = shop_id

        self._salvar_tokens()
        logger.info("Tokens Shopee obtidos com sucesso para shop_id=%d.", shop_id)
        return dados

    # -------------------------------------------------------------------
    # OAuth — Token refresh
    # -------------------------------------------------------------------
    def renovar_token(self) -> dict[str, Any]:
        """Refreshes an expired access token using the stored refresh token.

        Returns:
            Updated token dictionary from the API.

        Raises:
            ValueError: If no refresh token is available.
        """
        if not self._refresh_token or not self._shop_id:
            raise ValueError(
                "Nenhum refresh_token ou shop_id disponivel. "
                "Execute obter_token() primeiro."
            )

        sign, timestamp = self._gerar_assinatura(_REFRESH_TOKEN_PATH)

        params = {
            "partner_id": self.partner_id,
            "timestamp": timestamp,
            "sign": sign,
        }

        payload = {
            "refresh_token": self._refresh_token,
            "shop_id": self._shop_id,
            "partner_id": self.partner_id,
        }

        url = f"{_API_HOST}{_REFRESH_TOKEN_PATH}"
        response = requests.post(
            url, params=params, json=payload, timeout=REQUEST_TIMEOUT
        )

        dados = response.json()

        if dados.get("error"):
            raise ValueError(
                f"Shopee refresh error: {dados.get('error')} — "
                f"{dados.get('message', '')}"
            )

        self._access_token = dados.get("access_token", "")
        self._refresh_token = dados.get("refresh_token", "")

        self._salvar_tokens()
        logger.info("Token Shopee renovado com sucesso.")
        return dados

    # -------------------------------------------------------------------
    # Token persistence
    # -------------------------------------------------------------------
    def _carregar_tokens(self) -> None:
        """Loads persisted tokens from the local JSON file."""
        try:
            with open(self.token_file, "r", encoding="utf-8") as f:
                dados = json.load(f)

            self._access_token = dados.get("access_token", "")
            self._refresh_token = dados.get("refresh_token", "")
            self._shop_id = dados.get("shop_id", 0)

            if self._access_token:
                logger.debug(
                    "Tokens Shopee carregados de %s.", self.token_file
                )
        except (FileNotFoundError, json.JSONDecodeError):
            logger.debug(
                "Arquivo de tokens Shopee nao encontrado ou vazio: %s",
                self.token_file,
            )

    def _salvar_tokens(self) -> None:
        """Persists current tokens to the local JSON file."""
        dados = {
            "access_token": self._access_token,
            "refresh_token": self._refresh_token,
            "shop_id": self._shop_id,
        }
        try:
            with open(self.token_file, "w", encoding="utf-8") as f:
                json.dump(dados, f)
            logger.debug("Tokens Shopee salvos em %s.", self.token_file)
        except OSError as exc:
            logger.error("Falha ao salvar tokens Shopee: %s", exc)
            raise
