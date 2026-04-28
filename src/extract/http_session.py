"""
extract.http_session
~~~~~~~~~~~~~~~~~~~~
Shared HTTP session factory with retry/backoff for marketplace APIs.

Why a Session?
    - Reuses TCP/TLS connections (keep-alive) -> 20-40% less latency
      per request when calling the same host repeatedly.
    - Centralizes retry logic for transient failures (429, 5xx).
    - Removes duplicated try/except/sleep loops from each client.

Usage::

    from src.extract.http_session import build_session

    session = build_session()
    resp = session.get(url, params=params, timeout=10)
"""

from __future__ import annotations

import logging
from typing import Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Defaults — tuned for marketplace APIs
# ---------------------------------------------------------------------------
_DEFAULT_TOTAL_RETRIES: int = 5
_DEFAULT_BACKOFF_FACTOR: float = 1.0  # 1s, 2s, 4s, 8s, 16s
_DEFAULT_STATUS_FORCELIST: tuple[int, ...] = (429, 500, 502, 503, 504)
_DEFAULT_ALLOWED_METHODS: frozenset[str] = frozenset(
    {"HEAD", "GET", "OPTIONS", "POST"}
)
_DEFAULT_POOL_CONNECTIONS: int = 20
_DEFAULT_POOL_MAXSIZE: int = 50


def build_session(
    total_retries: int = _DEFAULT_TOTAL_RETRIES,
    backoff_factor: float = _DEFAULT_BACKOFF_FACTOR,
    status_forcelist: Iterable[int] = _DEFAULT_STATUS_FORCELIST,
    pool_connections: int = _DEFAULT_POOL_CONNECTIONS,
    pool_maxsize: int = _DEFAULT_POOL_MAXSIZE,
) -> requests.Session:
    """Builds a ``requests.Session`` with retry + connection pooling.

    The retry adapter handles 429 (rate limit) and 5xx (server) errors
    transparently with exponential backoff. The connection pool is
    sized to support parallel calls (used by ThreadPoolExecutor).

    Args:
        total_retries: Maximum number of retries per request.
        backoff_factor: Exponential backoff base (delay = backoff * 2^n).
        status_forcelist: HTTP status codes that trigger a retry.
        pool_connections: Number of distinct host connection pools.
        pool_maxsize: Max simultaneous connections per pool.

    Returns:
        Configured ``requests.Session`` ready for reuse.
    """
    session = requests.Session()

    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=tuple(status_forcelist),
        allowed_methods=_DEFAULT_ALLOWED_METHODS,
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    logger.debug(
        "HTTP session ready (retries=%d, backoff=%.1f, pool=%d).",
        total_retries, backoff_factor, pool_maxsize,
    )
    return session
