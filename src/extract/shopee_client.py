"""
extract.shopee_client
~~~~~~~~~~~~~~~~~~~~~
HTTP client for the Shopee Open Platform API (v2).
Handles HMAC-SHA256 authentication, OAuth shop authorization,
automatic token renewal, and signed request construction.

Authentication follows Shopee's signing scheme where every request
requires a ``sign`` parameter computed via HMAC-SHA256 from the
partner key, API path, timestamp, and (for shop-level calls) the
access token and shop ID.

Shopee API docs: https://open.shopee.com/developer-guide/4

Design patterns:
    - Mirrors ``meli_client.py`` conventions (Clean Architecture).
    - Uses ``settings`` for credential injection and token persistence.
    - Encapsulates all HTTP transport; callers receive clean dicts.
"""

import datetime
import hashlib
import hmac
import logging
import time
from typing import Any, Optional
from urllib.parse import urlencode

import requests
from tqdm import tqdm

from src.config.settings import (
    SHOPEE_PARTNER_ID,
    SHOPEE_PARTNER_KEY,
    SHOPEE_REDIRECT_URI,
    REQUEST_TIMEOUT,
    MAX_TOKEN_RETRIES,
    MAX_NETWORK_RETRIES,
    RETRY_DELAY_SECONDS,
    THROTTLE_DELAY_SECONDS,
    carregar_tokens_shopee,
    salvar_tokens_shopee,
)

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
_ESCROW_PATH = "/api/v2/payment/get_escrow_detail"

# Shopee allows max 100 per page on order list; we use 50 for safety
_PAGE_SIZE = 50
# Order detail endpoint accepts max 50 order_sn per call
_DETAIL_CHUNK_SIZE = 50
# Shopee limits time_from/time_to to a 15-day window
_MAX_DAYS_WINDOW = 15
# Fields requested in the order detail response
_DETAIL_OPTIONAL_FIELDS = (
    "buyer_user_id,buyer_username,item_list,"
    "escrow_amount,pay_time,recipient_address"
)


class ShopeeClient:
    """Client for the Shopee Open Platform REST API (v2).

    Encapsulates authentication, HMAC signing, and token lifecycle
    management for a specific Shopee shop (tenant).

    Usage::

        client = ShopeeClient()
        access_token, shop_id = client.obter_token_acesso()
        # Client is now authenticated and ready for API calls.
    """

    def __init__(self) -> None:
        """Initializes the client reading credentials from ``settings``.

        Credentials are injected via ``src.config.settings`` which reads
        them from the tenant-specific ``.env`` file loaded during
        ``inicializar()``.

        Required settings:
            ``SHOPEE_PARTNER_ID``: Numeric partner/app ID from Shopee.
            ``SHOPEE_PARTNER_KEY``: Secret key for HMAC signing.

        Raises:
            ValueError: If ``SHOPEE_PARTNER_ID`` or ``SHOPEE_PARTNER_KEY``
                        is missing from the loaded environment.
        """
        if not SHOPEE_PARTNER_ID:
            raise ValueError(
                "A variavel 'SHOPEE_PARTNER_ID' nao foi encontrada "
                "no ambiente carregado."
            )
        if not SHOPEE_PARTNER_KEY:
            raise ValueError(
                "A variavel 'SHOPEE_PARTNER_KEY' nao foi encontrada "
                "no ambiente carregado."
            )

        self._partner_id: int = int(SHOPEE_PARTNER_ID)
        self._partner_key: bytes = SHOPEE_PARTNER_KEY.encode("utf-8")
        self._redirect_uri: str = SHOPEE_REDIRECT_URI

        # Runtime state (populated after authorization)
        self._access_token: str = ""
        self._refresh_token: str = ""
        self._shop_id: int = 0

    # ------------------------------------------------------------------
    # Signature
    # ------------------------------------------------------------------

    def _gerar_assinatura(
        self,
        path: str,
        access_token: str = "",
        shop_id: int = 0,
    ) -> tuple[str, int]:
        """Generates the HMAC-SHA256 signature required by Shopee API v2.

        The base string varies depending on the call type:

        - **System-level** (auth endpoints, no token)::

              {partner_id}{path}{timestamp}

        - **Shop-level** (authenticated endpoints)::

              {partner_id}{path}{timestamp}{access_token}{shop_id}

        Args:
            path: API endpoint path (e.g. ``/api/v2/order/get_order_list``).
            access_token: Shop access token (empty for system calls).
            shop_id: Numeric shop ID (0 for system calls).

        Returns:
            Tuple of ``(sign_hex, timestamp)``.
        """
        timestamp = int(time.time())

        base_string = f"{self._partner_id}{path}{timestamp}"
        if access_token and shop_id:
            base_string += f"{access_token}{shop_id}"

        sign = hmac.new(
            self._partner_key,
            base_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        return sign, timestamp

    # ------------------------------------------------------------------
    # Authenticated request parameters
    # ------------------------------------------------------------------

    def _gerar_parametros_autenticados(
        self, api_path: str
    ) -> dict[str, Any]:
        """Builds the full URL query parameters for an authenticated API call.

        Generates a fresh HMAC signature using the current ``access_token``
        and ``shop_id``, then returns a dictionary ready to be passed as
        ``params`` to ``requests.get()`` or ``requests.post()``.

        Args:
            api_path: API endpoint path
                (e.g. ``/api/v2/order/get_order_list``).

        Returns:
            Dictionary with ``partner_id``, ``timestamp``, ``sign``,
            ``access_token``, and ``shop_id`` keys.

        Raises:
            RuntimeError: If the client has not been authenticated yet.
        """
        if not self._access_token or not self._shop_id:
            raise RuntimeError(
                "Token nao inicializado. Chame obter_token_acesso() primeiro."
            )

        sign, timestamp = self._gerar_assinatura(
            api_path,
            access_token=self._access_token,
            shop_id=self._shop_id,
        )

        return {
            "partner_id": self._partner_id,
            "timestamp": timestamp,
            "sign": sign,
            "access_token": self._access_token,
            "shop_id": self._shop_id,
        }

    # ------------------------------------------------------------------
    # Authentication — unified entry point
    # ------------------------------------------------------------------

    def obter_token_acesso(self) -> tuple[str, int]:
        """Obtains or refreshes the Shopee access token.

        Follows the same contract as ``MercadoLivreClient.obter_token_acesso()``:

        1. Attempts to load persisted tokens via ``carregar_tokens_shopee()``.
        2. If a ``refresh_token`` exists, calls the Shopee refresh endpoint
           to obtain a new ``access_token``.
        3. If successful, persists the updated tokens via
           ``salvar_tokens_shopee()``.

        Returns:
            Tuple of ``(access_token, shop_id)``.

        Raises:
            ValueError: If no persisted tokens exist (initial authorization
                        must be performed first via ``gerar_url_autenticacao()``
                        and ``obter_token_inicial()``).
            RuntimeError: If the Shopee API returns an error on refresh.
        """
        tokens_salvos = carregar_tokens_shopee()

        if not tokens_salvos or "refresh_token" not in tokens_salvos:
            raise ValueError(
                "Nenhum token Shopee persistido encontrado. "
                "Execute o fluxo de autorizacao inicial primeiro: "
                "gerar_url_autenticacao() -> obter_token_inicial()."
            )

        self._refresh_token = tokens_salvos["refresh_token"]
        self._shop_id = tokens_salvos.get("shop_id", 0)

        if not self._shop_id:
            raise ValueError(
                "Campo 'shop_id' ausente no arquivo de tokens Shopee. "
                "Refaca a autorizacao inicial."
            )

        # --- Refresh via Shopee API v2 ---
        sign, timestamp = self._gerar_assinatura(_REFRESH_TOKEN_PATH)

        params = {
            "partner_id": self._partner_id,
            "timestamp": timestamp,
            "sign": sign,
        }

        payload = {
            "refresh_token": self._refresh_token,
            "shop_id": self._shop_id,
            "partner_id": self._partner_id,
        }

        url = f"{_API_HOST}{_REFRESH_TOKEN_PATH}"
        response = requests.post(
            url, params=params, json=payload, timeout=REQUEST_TIMEOUT
        )

        dados = response.json()

        if dados.get("error"):
            raise RuntimeError(
                f"Erro ao renovar token Shopee: {dados.get('error')} — "
                f"{dados.get('message', 'Sem detalhes.')}"
            )

        self._access_token = dados["access_token"]
        self._refresh_token = dados["refresh_token"]

        salvar_tokens_shopee(
            self._access_token,
            self._refresh_token,
            self._shop_id,
        )
        logger.info("Token Shopee renovado com sucesso para shop_id=%d.", self._shop_id)
        return self._access_token, self._shop_id

    # ------------------------------------------------------------------
    # OAuth — Authorization URL
    # ------------------------------------------------------------------

    def gerar_url_autenticacao(self) -> str:
        """Builds the full Shopee OAuth authorization URL.

        The seller must open this URL in a browser, log in, and authorize
        the partner app. Shopee will then redirect to
        ``SHOPEE_REDIRECT_URI`` with ``code`` and ``shop_id`` query
        parameters.

        Returns:
            Complete authorization URL ready for the seller to visit.
        """
        sign, timestamp = self._gerar_assinatura(_AUTH_PATH)

        params = {
            "partner_id": self._partner_id,
            "timestamp": timestamp,
            "sign": sign,
            "redirect": self._redirect_uri,
        }

        url = f"{_API_HOST}{_AUTH_PATH}?{urlencode(params)}"
        logger.info("URL de autorizacao Shopee gerada com sucesso.")
        return url

    # ------------------------------------------------------------------
    # OAuth — Initial token exchange
    # ------------------------------------------------------------------

    def obter_token_inicial(self, code: str, shop_id: int) -> dict[str, Any]:
        """Exchanges the authorization code for access + refresh tokens.

        This is the **first-time** authorization flow. After this call
        succeeds, subsequent executions should use ``obter_token_acesso()``
        which performs automatic renewal.

        Args:
            code: Authorization code received via redirect callback.
            shop_id: Shop ID received via redirect callback.

        Returns:
            Dictionary with ``access_token``, ``refresh_token``,
            ``expire_in``, and ``shop_id``.

        Raises:
            RuntimeError: If the Shopee API returns an error payload.
            requests.exceptions.RequestException: On network failure.
        """
        sign, timestamp = self._gerar_assinatura(_TOKEN_PATH)

        params = {
            "partner_id": self._partner_id,
            "timestamp": timestamp,
            "sign": sign,
        }

        payload = {
            "code": code,
            "shop_id": shop_id,
            "partner_id": self._partner_id,
        }

        url = f"{_API_HOST}{_TOKEN_PATH}"
        response = requests.post(
            url, params=params, json=payload, timeout=REQUEST_TIMEOUT
        )

        dados = response.json()

        if dados.get("error"):
            raise RuntimeError(
                f"Erro na troca de code por token Shopee: "
                f"{dados.get('error')} — {dados.get('message', '')}"
            )

        self._access_token = dados["access_token"]
        self._refresh_token = dados["refresh_token"]
        self._shop_id = shop_id

        salvar_tokens_shopee(
            self._access_token,
            self._refresh_token,
            self._shop_id,
        )
        logger.info(
            "Tokens Shopee obtidos com sucesso para shop_id=%d.", shop_id
        )
        return dados

    # ------------------------------------------------------------------
    # Order extraction
    # ------------------------------------------------------------------

    def buscar_todos_pedidos(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Fetches order history with automatic pagination and detail enrichment.

        Implements Shopee's mandatory two-step extraction:

        1. **List** — ``/api/v2/order/get_order_list`` retrieves
           ``order_sn`` identifiers page by page using cursor-based
           pagination.
        2. **Detail** — ``/api/v2/order/get_order_detail`` fetches the
           full order payload in chunks of up to 50 SNs.

        Shopee restricts ``time_from``/``time_to`` to a maximum 15-day
        window. If the requested range exceeds that, this method
        automatically splits it into consecutive 15-day sub-windows.

        Args:
            date_from: Start date in ISO 8601
                (e.g. ``2026-03-01T00:00:00.000-00:00``) or ``None``
                for 15 days ago.
            date_to: End date in ISO 8601 or ``None`` for now.

        Returns:
            List of full order dictionaries (detail-level).

        Raises:
            RuntimeError: If the token was not initialized or the API
                returns an unrecoverable error.
        """
        if not self._access_token or not self._shop_id:
            raise RuntimeError(
                "Token nao inicializado. Chame obter_token_acesso() primeiro."
            )

        # --- Date resolution and Unix timestamp conversion ---
        agora = datetime.datetime.now(tz=datetime.timezone.utc)

        if date_to:
            dt_to = self._parse_iso_date(date_to)
        else:
            dt_to = agora

        if date_from:
            dt_from = self._parse_iso_date(date_from)
        else:
            dt_from = dt_to - datetime.timedelta(days=_MAX_DAYS_WINDOW)

        # --- Split into 15-day windows ---
        janelas = self._gerar_janelas_tempo(dt_from, dt_to)

        logger.info(
            "Shopee: Extraindo pedidos de %s ate %s (%d janela(s) de tempo).",
            dt_from.strftime("%Y-%m-%d"),
            dt_to.strftime("%Y-%m-%d"),
            len(janelas),
        )

        # --- Step 1: Collect all order_sn across all time windows ---
        todos_sn: list[str] = []
        tentativas_token = 0
        tentativas_rede = 0

        for idx_janela, (ts_from, ts_to) in enumerate(janelas, start=1):
            cursor = ""
            has_more = True

            tqdm.write(
                f"   [{idx_janela}/{len(janelas)}] Janela: "
                f"{datetime.datetime.fromtimestamp(ts_from, tz=datetime.timezone.utc).strftime('%Y-%m-%d')} → "
                f"{datetime.datetime.fromtimestamp(ts_to, tz=datetime.timezone.utc).strftime('%Y-%m-%d')}"
            )

            while has_more:
                params = self._gerar_parametros_autenticados(_ORDERS_PATH)
                params.update({
                    "time_range_field": "create_time",
                    "time_from": ts_from,
                    "time_to": ts_to,
                    "page_size": _PAGE_SIZE,
                    "cursor": cursor,
                })

                url = f"{_API_HOST}{_ORDERS_PATH}"

                try:
                    resp = requests.get(
                        url, params=params, timeout=REQUEST_TIMEOUT
                    )
                    dados = resp.json()

                    # --- Token expiry handling ---
                    erro = dados.get("error", "")
                    if erro in ("error_auth", "error_permission") or resp.status_code == 401:
                        tentativas_token += 1
                        if tentativas_token > MAX_TOKEN_RETRIES:
                            raise RuntimeError(
                                f"Falha ao renovar token Shopee apos "
                                f"{MAX_TOKEN_RETRIES} tentativas."
                            )
                        tqdm.write(
                            f"   [!] Token expirado. Renovando "
                            f"({tentativas_token}/{MAX_TOKEN_RETRIES})..."
                        )
                        self.obter_token_acesso()
                        continue

                    tentativas_token = 0

                    if erro:
                        raise RuntimeError(
                            f"Erro Shopee get_order_list: {erro} — "
                            f"{dados.get('message', '')}"
                        )

                    tentativas_rede = 0
                    response_data = dados.get("response", {})
                    order_list = response_data.get("order_list", [])

                    for order in order_list:
                        sn = order.get("order_sn")
                        if sn:
                            todos_sn.append(sn)

                    has_more = response_data.get("more", False)
                    cursor = response_data.get("next_cursor", "")

                except requests.exceptions.RequestException as exc:
                    tentativas_rede += 1
                    if tentativas_rede > MAX_NETWORK_RETRIES:
                        raise RuntimeError(
                            f"Rede instavel apos {MAX_NETWORK_RETRIES} "
                            f"tentativas. Ultima falha: {exc}"
                        ) from exc
                    tqdm.write(
                        f"   -> Instabilidade de rede ({tentativas_rede}/"
                        f"{MAX_NETWORK_RETRIES}). Aguardando "
                        f"{RETRY_DELAY_SECONDS}s... ({exc})"
                    )
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue

                time.sleep(THROTTLE_DELAY_SECONDS)

        if not todos_sn:
            logger.info("Shopee: Nenhum pedido encontrado no periodo.")
            return []

        logger.info("Shopee: %d order_sn coletados. Buscando detalhes...", len(todos_sn))

        # --- Step 2: Fetch order details in chunks ---
        chunks = [
            todos_sn[i : i + _DETAIL_CHUNK_SIZE]
            for i in range(0, len(todos_sn), _DETAIL_CHUNK_SIZE)
        ]

        todos_pedidos: list[dict[str, Any]] = []
        tentativas_token = 0
        tentativas_rede = 0

        pbar = tqdm(
            total=len(todos_sn),
            desc="Shopee Order Details",
            unit="ped",
            colour="red",
        )

        try:
            for chunk_idx, chunk_sns in enumerate(chunks, start=1):
                params = self._gerar_parametros_autenticados(_ORDER_DETAIL_PATH)
                params["order_sn_list"] = ",".join(chunk_sns)
                params["response_optional_fields"] = _DETAIL_OPTIONAL_FIELDS

                url = f"{_API_HOST}{_ORDER_DETAIL_PATH}"

                try:
                    resp = requests.get(
                        url, params=params, timeout=REQUEST_TIMEOUT
                    )
                    dados = resp.json()

                    erro = dados.get("error", "")
                    if erro in ("error_auth", "error_permission") or resp.status_code == 401:
                        tentativas_token += 1
                        if tentativas_token > MAX_TOKEN_RETRIES:
                            raise RuntimeError(
                                f"Falha ao renovar token Shopee apos "
                                f"{MAX_TOKEN_RETRIES} tentativas (detail)."
                            )
                        tqdm.write(
                            f"   [!] Token expirado no detail. Renovando "
                            f"({tentativas_token}/{MAX_TOKEN_RETRIES})..."
                        )
                        self.obter_token_acesso()
                        # Retry same chunk
                        params = self._gerar_parametros_autenticados(_ORDER_DETAIL_PATH)
                        params["order_sn_list"] = ",".join(chunk_sns)
                        params["response_optional_fields"] = _DETAIL_OPTIONAL_FIELDS
                        resp = requests.get(
                            url, params=params, timeout=REQUEST_TIMEOUT
                        )
                        dados = resp.json()

                    tentativas_token = 0

                    if dados.get("error"):
                        tqdm.write(
                            f"   -> Aviso no chunk {chunk_idx}: "
                            f"{dados.get('error')} — {dados.get('message', '')}"
                        )
                        pbar.update(len(chunk_sns))
                        continue

                    tentativas_rede = 0
                    order_list = dados.get("response", {}).get("order_list", [])
                    todos_pedidos.extend(order_list)
                    pbar.update(len(chunk_sns))

                except requests.exceptions.RequestException as exc:
                    tentativas_rede += 1
                    if tentativas_rede > MAX_NETWORK_RETRIES:
                        raise RuntimeError(
                            f"Rede instavel apos {MAX_NETWORK_RETRIES} "
                            f"tentativas (detail). Ultima falha: {exc}"
                        ) from exc
                    tqdm.write(
                        f"   -> Instabilidade de rede ({tentativas_rede}/"
                        f"{MAX_NETWORK_RETRIES}). Aguardando "
                        f"{RETRY_DELAY_SECONDS}s..."
                    )
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue

                time.sleep(THROTTLE_DELAY_SECONDS)
        finally:
            pbar.close()

        # --- Step 3: Financial enrichment via Escrow ---
        logger.info(
            "Shopee: Enriquecendo %d pedidos com dados financeiros (escrow)...",
            len(todos_pedidos),
        )

        pbar_escrow = tqdm(
            total=len(todos_pedidos),
            desc="Shopee Escrow Detail",
            unit="ped",
            colour="magenta",
        )

        try:
            for pedido in todos_pedidos:
                sn = pedido.get("order_sn", "")
                if sn:
                    pedido["escrow_detail"] = self._buscar_detalhes_escrow(sn)
                else:
                    pedido["escrow_detail"] = {}
                pbar_escrow.update(1)
                time.sleep(THROTTLE_DELAY_SECONDS)
        finally:
            pbar_escrow.close()

        logger.info(
            "Shopee: Extracao completa. %d pedidos com detalhes e escrow obtidos.",
            len(todos_pedidos),
        )
        return todos_pedidos

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _buscar_detalhes_escrow(self, order_sn: str) -> dict:
        """Fetches the financial escrow breakdown for a single order.

        Calls ``/api/v2/payment/get_escrow_detail`` to obtain fee
        breakdowns (commission, transaction, service, shipping).

        Args:
            order_sn: Shopee order serial number.

        Returns:
            The ``response`` dict from the API, or an empty dict
            if the call fails or escrow is not yet released.
        """
        tentativas_rede = 0

        while True:
            try:
                params = self._gerar_parametros_autenticados(_ESCROW_PATH)
                params["order_sn"] = order_sn

                url = f"{_API_HOST}{_ESCROW_PATH}"
                resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
                dados = resp.json()

                erro = dados.get("error", "")

                # Token refresh on auth error
                if erro in ("error_auth", "error_permission") or resp.status_code == 401:
                    self.obter_token_acesso()
                    params = self._gerar_parametros_autenticados(_ESCROW_PATH)
                    params["order_sn"] = order_sn
                    resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
                    dados = resp.json()
                    erro = dados.get("error", "")

                if erro:
                    # Escrow may not be available for unpaid/cancelled orders
                    logger.debug(
                        "Escrow indisponivel para %s: %s", order_sn, erro
                    )
                    return {}

                return dados.get("response", {})

            except requests.exceptions.RequestException as exc:
                tentativas_rede += 1
                if tentativas_rede > MAX_NETWORK_RETRIES:
                    logger.warning(
                        "Falha ao obter escrow de %s apos %d tentativas: %s",
                        order_sn, MAX_NETWORK_RETRIES, exc,
                    )
                    return {}
                time.sleep(RETRY_DELAY_SECONDS)

    @staticmethod
    def _parse_iso_date(date_str: str) -> datetime.datetime:
        """Parses an ISO 8601 date string into a timezone-aware datetime.

        Handles common formats:
            - ``2026-03-01T00:00:00.000-00:00``
            - ``2026-03-01T00:00:00``
            - ``2026-03-01``

        Returns:
            Timezone-aware ``datetime`` (UTC if no timezone provided).
        """
        date_str = date_str.strip()

        # Try full ISO with timezone
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                dt = datetime.datetime.strptime(date_str, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                return dt
            except ValueError:
                continue

        raise ValueError(f"Formato de data nao reconhecido: {date_str}")

    @staticmethod
    def _gerar_janelas_tempo(
        dt_from: datetime.datetime,
        dt_to: datetime.datetime,
    ) -> list[tuple[int, int]]:
        """Splits a date range into consecutive windows of up to 15 days.

        Shopee's ``get_order_list`` requires ``time_from`` and ``time_to``
        to be within a 15-day span. This method generates the necessary
        sub-windows as Unix timestamp pairs.

        Returns:
            List of ``(time_from_unix, time_to_unix)`` tuples.
        """
        janelas: list[tuple[int, int]] = []
        cursor = dt_from
        delta = datetime.timedelta(days=_MAX_DAYS_WINDOW)

        while cursor < dt_to:
            window_end = min(cursor + delta, dt_to)
            janelas.append((
                int(cursor.timestamp()),
                int(window_end.timestamp()),
            ))
            cursor = window_end

        return janelas if janelas else [(int(dt_from.timestamp()), int(dt_to.timestamp()))]
