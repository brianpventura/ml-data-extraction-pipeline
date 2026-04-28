"""
extract.meli_client
~~~~~~~~~~~~~~~~~~~~~~~~~~
HTTP client for the Mercado Livre REST API.
Handles authentication (OAuth 2.0), paginated order retrieval,
and shipping cost enrichment. Returns raw JSON data only.

Performance notes:
    - Uses a shared ``requests.Session`` (keep-alive + connection pool)
      to avoid TCP/TLS handshake overhead on every call.
    - Shipping cost enrichment is parallelized via ``ThreadPoolExecutor``
      so that N orders are enriched in roughly N/max_workers time
      instead of N sequential round-trips.
    - HTTP 429/5xx are retried automatically by the underlying adapter
      with exponential backoff (urllib3 ``Retry``).
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    RATE_LIMIT_BACKOFF_SECONDS,
    MAX_TOKEN_RETRIES,
    MAX_NETWORK_RETRIES,
    carregar_tokens,
    salvar_tokens,
)
from src.extract.http_session import build_session

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# API constants
# ---------------------------------------------------------------------------
_TOKEN_URL = f"{API_BASE_URL}/oauth/token"
_ORDERS_URL = f"{API_BASE_URL}/orders/search"
_SHIPMENTS_URL = f"{API_BASE_URL}/shipments"

_PAGE_LIMIT = 50
_OFFSET_CEILING = 9950
_SHIPPING_PARALLEL_WORKERS = 8


class MercadoLivreClient:
    """Encapsulates authentication and requests to the Mercado Livre API."""

    def __init__(self) -> None:
        self._access_token: Optional[str] = None
        self._user_id: Optional[int] = None
        self._headers: dict[str, str] = {}
        self._session: requests.Session = build_session()

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

        response = self._session.post(
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

        raise RuntimeError(
            f"Erro ao obter token (HTTP {response.status_code}): {response.text}"
        )

    # ------------------------------------------------------------------
    # Order extraction
    # ------------------------------------------------------------------

    def buscar_todos_pedidos(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Fetches the order history with pagination + shipping enrichment.

        The extraction happens in two steps:

        1. **Paginated list** — pulls orders 50 at a time. Uses dynamic
           ``date_to`` pivot to bypass the 10k offset hard limit.
        2. **Shipping enrichment (parallel)** — fetches actual shipping
           cost from ``/shipments/{id}`` for every order using a thread
           pool. This is *the* dominant cost of extraction; parallelism
           here drives most of the wall-clock improvement.

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
                "Token nao inicializado. Chame obter_token_acesso() primeiro."
            )

        # --- Step 1: paginated list (sequential — pagination is stateful) ---
        todos_pedidos = self._listar_pedidos_paginado(date_from, date_to)

        if not todos_pedidos:
            logger.info("Mercado Livre: nenhum pedido novo encontrado.")
            return []

        # --- Step 2: parallel shipping enrichment ---
        logger.info(
            "Mercado Livre: enriquecendo %d pedidos com frete (paralelo, %d workers)...",
            len(todos_pedidos), _SHIPPING_PARALLEL_WORKERS,
        )
        self._enriquecer_frete_paralelo(
            todos_pedidos, max_workers=_SHIPPING_PARALLEL_WORKERS
        )

        logger.info(
            "Mercado Livre: extracao completa (%d pedidos).", len(todos_pedidos)
        )
        return todos_pedidos

    # ------------------------------------------------------------------
    # Private — Pagination
    # ------------------------------------------------------------------

    def _listar_pedidos_paginado(
        self,
        date_from: Optional[str],
        date_to: Optional[str],
    ) -> list[dict[str, Any]]:
        """Walks the paginated /orders/search endpoint and returns all hits.

        Stateful (cursor-like via offset + dynamic date_to pivot), so
        kept sequential. Network glitches are absorbed by the Session's
        retry adapter; only token expiry needs explicit handling here.
        """
        offset = 0
        current_date_to: Optional[str] = date_to
        todos_pedidos: list[dict[str, Any]] = []
        tentativas_token = 0
        tentativas_rede = 0

        total_esperado = self._obter_total_pedidos(date_from)
        pbar = tqdm(
            total=total_esperado,
            desc="ML Order List",
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
                    response = self._session.get(
                        _ORDERS_URL,
                        headers=self._headers,
                        params=params,
                        timeout=REQUEST_TIMEOUT,
                    )

                    # Token renewal — adapter cannot handle 401s
                    if response.status_code == 401:
                        tentativas_token += 1
                        if tentativas_token > MAX_TOKEN_RETRIES:
                            raise RuntimeError(
                                f"Falha ao renovar token apos {MAX_TOKEN_RETRIES} "
                                f"tentativas. Verifique suas credenciais."
                            )
                        tqdm.write(
                            f"[!] Token expirado. Renovando "
                            f"(tentativa {tentativas_token}/{MAX_TOKEN_RETRIES})..."
                        )
                        self.obter_token_acesso()
                        continue

                    tentativas_token = 0

                    if response.status_code == 200:
                        tentativas_rede = 0
                        resultados: list[dict] = response.json().get("results", [])

                        if not resultados:
                            break

                        todos_pedidos.extend(resultados)
                        pbar.update(len(resultados))

                        if len(resultados) < _PAGE_LIMIT:
                            break

                        # Dynamic date_to pivot for the 10k offset hard limit
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

                    elif response.status_code == 429:
                        # Adapter retry already exhausted — wait extra and continue
                        retry_after = int(
                            response.headers.get("Retry-After", RATE_LIMIT_BACKOFF_SECONDS)
                        )
                        tqdm.write(
                            f"[!] Rate limit (HTTP 429). Aguardando {retry_after}s..."
                        )
                        time.sleep(retry_after)
                        continue
                    else:
                        raise RuntimeError(
                            f"Erro na API (offset {offset}, "
                            f"HTTP {response.status_code}): {response.text}"
                        )

                except requests.exceptions.RequestException as exc:
                    tentativas_rede += 1
                    if tentativas_rede > MAX_NETWORK_RETRIES:
                        raise RuntimeError(
                            f"Rede instavel apos {MAX_NETWORK_RETRIES} "
                            f"tentativas. Ultima falha: {exc}"
                        ) from exc
                    tqdm.write(
                        f"-> Instabilidade de rede ({tentativas_rede}/"
                        f"{MAX_NETWORK_RETRIES}). "
                        f"Aguardando {RETRY_DELAY_SECONDS}s... ({exc})"
                    )
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue

                time.sleep(THROTTLE_DELAY_SECONDS)
        finally:
            pbar.close()

        return todos_pedidos

    # ------------------------------------------------------------------
    # Private — Parallel shipping enrichment
    # ------------------------------------------------------------------

    def _enriquecer_frete_paralelo(
        self, pedidos: list[dict[str, Any]], max_workers: int = 8
    ) -> None:
        """Fetches shipping cost for each order in parallel (in-place).

        Uses a Semaphore to cap concurrent requests so the API rate
        limit is respected.

        Args:
            pedidos: List of raw order dicts. Each dict gets a
                ``custo_frete_real`` field populated in place.
            max_workers: Max simultaneous shipping API requests.
        """
        semaphore = threading.Semaphore(max_workers)

        def _worker(pedido: dict[str, Any]) -> None:
            with semaphore:
                pedido["custo_frete_real"] = self._buscar_custo_frete(pedido)

        pbar = tqdm(
            total=len(pedidos),
            desc="ML Shipping Cost",
            unit="ped",
            colour="cyan",
        )
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(_worker, p) for p in pedidos]
                for future in as_completed(futures):
                    # Surface any worker exception immediately
                    future.result()
                    pbar.update(1)
        finally:
            pbar.close()

    # ------------------------------------------------------------------
    # Private — Single-call helpers
    # ------------------------------------------------------------------

    def _obter_total_pedidos(self, date_from: Optional[str]) -> int:
        """Probes the API (limit=1) to discover the expected total order count."""
        try:
            params: dict[str, Any] = {"seller": self._user_id, "limit": 1}
            if date_from:
                params["order.date_created.from"] = date_from

            resp = self._session.get(
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
            resp = self._session.get(
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
