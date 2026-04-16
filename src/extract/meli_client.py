"""
extract.meli_client
~~~~~~~~~~~~~~~~~~~~~~~~~~
HTTP client for the marketplace REST API.
Handles authentication (OAuth 2.0), paginated order retrieval,
and shipping cost enrichment. Returns raw JSON data only.
"""

import logging
import time
from typing import Any, Optional

import requests
from tqdm import tqdm

from src.config.settings import (
    APP_ID,
    AUTHORIZATION_CODE,
    CLIENT_SECRET,
    REDIRECT_URI,
    API_BASE_URL,
    REQUEST_TIMEOUT,
    SHIPPING_TIMEOUT,
    RETRY_DELAY_SECONDS,
    THROTTLE_DELAY_SECONDS,
    MAX_TOKEN_RETRIES,
    MAX_NETWORK_RETRIES,
    carregar_tokens,
    salvar_tokens,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# API constants
# ---------------------------------------------------------------------------
_TOKEN_URL = f"{API_BASE_URL}/oauth/token"
_ORDERS_URL = f"{API_BASE_URL}/orders/search"
_SHIPMENTS_URL = f"{API_BASE_URL}/shipments"

_PAGE_LIMIT = 50
_OFFSET_CEILING = 9950


class MercadoLivreClient:
    """Encapsulates authentication and requests to the marketplace API."""

    def __init__(self) -> None:
        self._access_token: Optional[str] = None
        self._user_id: Optional[int] = None
        self._headers: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def obter_token_acesso(self) -> tuple[str, int]:
        """Obtains or refreshes the access token via OAuth 2.0.

        Attempts renewal via refresh_token if persisted locally.
        Falls back to the initial authorization_code grant otherwise.

        Returns:
            Tuple of (access_token, user_id).

        Raises:
            RuntimeError: If the API returns a non-200 status.
            requests.exceptions.RequestException: On network failure.
        """
        headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
        }

        tokens_salvos = carregar_tokens()

        if tokens_salvos and "refresh_token" in tokens_salvos:
            payload = {
                "grant_type": "refresh_token",
                "client_id": APP_ID,
                "client_secret": CLIENT_SECRET,
                "refresh_token": tokens_salvos["refresh_token"],
            }
        else:
            payload = {
                "grant_type": "authorization_code",
                "client_id": APP_ID,
                "client_secret": CLIENT_SECRET,
                "code": AUTHORIZATION_CODE,
                "redirect_uri": REDIRECT_URI,
            }

        response = requests.post(
            _TOKEN_URL, headers=headers, data=payload, timeout=REQUEST_TIMEOUT
        )

        if response.status_code == 200:
            dados: dict[str, Any] = response.json()
            self._access_token = dados["access_token"]
            self._user_id = dados["user_id"]
            self._headers = {"Authorization": f"Bearer {self._access_token}"}

            salvar_tokens(
                dados["access_token"],
                dados["refresh_token"],
                dados["user_id"],
            )
            logger.info("Access token obtained/renewed successfully.")
            return self._access_token, self._user_id
        else:
            raise RuntimeError(f"Erro ao obter token (HTTP {response.status_code}): {response.text}")

    # ------------------------------------------------------------------
    # Order extraction
    # ------------------------------------------------------------------

    def buscar_todos_pedidos(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Fetches order history with automatic pagination and enriches
        each order with the actual shipping cost.

        Args:
            date_from: Start date in the API's ISO 8601 format.
            date_to: End date in the API's ISO 8601 format.

        Returns:
            List of raw order dictionaries with an extra
            ``custo_frete_real`` field added.

        Raises:
            RuntimeError: If the token was not initialized or the API
                returns an unrecoverable error.
        """
        if not self._access_token or not self._user_id:
            raise RuntimeError(
                "Token não inicializado. Chame obter_token_acesso() primeiro."
            )

        offset = 0
        current_date_to: Optional[str] = date_to
        todos_pedidos: list[dict[str, Any]] = []
        tentativas_token = 0
        tentativas_rede = 0

        logger.info("Starting order extraction with shipping cost enrichment...")

        # Initial probe to determine expected total for progress bar
        total_esperado = self._obter_total_pedidos(date_from)
        pbar = tqdm(
            total=total_esperado,
            desc="Extracting Orders",
            unit="ped",
            colour="yellow",
        )

        try:
            while True:
                params: dict[str, Any] = {
                    "seller": self._user_id,
                    "limit": _PAGE_LIMIT,
                    "offset": offset,
                    "sort": "date_desc",
                }
                if date_from:
                    params["order.date_created.from"] = date_from
                if current_date_to:
                    params["order.date_created.to"] = current_date_to

                try:
                    response = requests.get(
                        _ORDERS_URL,
                        headers=self._headers,
                        params=params,
                        timeout=REQUEST_TIMEOUT,
                    )

                    # HTTP 401: automatic token renewal with retry cap
                    if response.status_code == 401:
                        tentativas_token += 1
                        if tentativas_token > MAX_TOKEN_RETRIES:
                            raise RuntimeError(
                                f"Falha ao renovar token após {MAX_TOKEN_RETRIES} "
                                f"tentativas. Verifique suas credenciais."
                            )
                        tqdm.write(
                            f"[!] Token expirado! Renovando acesso "
                            f"(tentativa {tentativas_token}/{MAX_TOKEN_RETRIES})..."
                        )
                        self.obter_token_acesso()
                        continue

                    # Reset token retry counter on success
                    tentativas_token = 0

                    if response.status_code == 200:
                        # Reset network retry counter on success
                        tentativas_rede = 0
                        resultados: list[dict] = response.json().get("results", [])

                        if not resultados:
                            break

                        for pedido in resultados:
                            custo_frete = self._buscar_custo_frete(pedido)
                            pedido["custo_frete_real"] = custo_frete
                            pbar.update(1)

                        todos_pedidos.extend(resultados)

                        if len(resultados) < _PAGE_LIMIT:
                            break

                        # Dynamic date_to pivot to mitigate the API's hard offset limit of 10,000
                        if offset + _PAGE_LIMIT >= _OFFSET_CEILING:
                            data_ultimo = resultados[-1].get("date_created")
                            tqdm.write(
                                f"-> Marco atingido. Deslocando busca para "
                                f"vendas antes de {data_ultimo}..."
                            )
                            current_date_to = data_ultimo
                            offset = 0
                        else:
                            offset += _PAGE_LIMIT
                    else:
                        raise RuntimeError(
                            f"Erro na API (offset {offset}, "
                            f"HTTP {response.status_code}): {response.text}"
                        )

                except requests.exceptions.RequestException as exc:
                    tentativas_rede += 1
                    if tentativas_rede > MAX_NETWORK_RETRIES:
                        raise RuntimeError(
                            f"Rede instável após {MAX_NETWORK_RETRIES} "
                            f"tentativas consecutivas. Última falha: {exc}"
                        ) from exc
                    tqdm.write(
                        f"-> Instabilidade na rede ({tentativas_rede}/"
                        f"{MAX_NETWORK_RETRIES}). "
                        f"Aguardando {RETRY_DELAY_SECONDS}s... ({exc})"
                    )
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue

                time.sleep(THROTTLE_DELAY_SECONDS)
        finally:
            pbar.close()

        logger.info("Extraction complete: %d orders retrieved.", len(todos_pedidos))
        return todos_pedidos

    # ------------------------------------------------------------------
    # Private methods
    # ------------------------------------------------------------------

    def _obter_total_pedidos(self, date_from: Optional[str]) -> int:
        """Probes the API (limit=1) to discover the expected total order count."""
        try:
            params: dict[str, Any] = {"seller": self._user_id, "limit": 1}
            if date_from:
                params["order.date_created.from"] = date_from

            resp = requests.get(
                _ORDERS_URL,
                headers=self._headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            return resp.json().get("paging", {}).get("total", 1000)
        except Exception:
            return 1000

    def _buscar_custo_frete(self, pedido: dict[str, Any]) -> float:
        """Retrieves the actual shipping cost via the Shipments API.

        Args:
            pedido: Raw order dictionary.

        Returns:
            Shipping cost as a float. Returns 0.0 if unavailable.
        """
        shipping_id = pedido.get("shipping", {}).get("id")
        if not shipping_id:
            return 0.0

        try:
            url = f"{_SHIPMENTS_URL}/{shipping_id}"
            resp = requests.get(
                url, headers=self._headers, timeout=SHIPPING_TIMEOUT
            )

            if resp.status_code == 200:
                corpo = resp.json()
                base_cost = float(corpo.get("base_cost") or 0.0)
                opcoes = corpo.get("shipping_option") or {}
                list_cost = float(opcoes.get("list_cost") or 0.0)
                return list_cost if list_cost > 0 else base_cost
        except Exception:
            logger.debug(
                "Falha ao obter frete do envio %s", shipping_id, exc_info=True
            )

        return 0.0
