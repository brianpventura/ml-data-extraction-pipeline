"""
config.channels
~~~~~~~~~~~~~~~
Single source of truth for the marketplaces (channels) supported
by the pipeline. Adding a new marketplace becomes a one-line change
here plus a Client + Adapter — no edits to ``main.py`` or
``data_processor.py``.

Pattern:
    Each ``ChannelSpec`` binds an extractor (Client class) to a
    transform adapter (Adapter class) and pins the canonical
    ``id_canal`` used by ``dim_canal_venda`` rows.

Adding Amazon (example):
    1. Implement ``src/extract/amazon_client.py`` with
       ``obter_token_acesso()`` and ``buscar_todos_pedidos(date_from, date_to)``.
    2. Implement ``src/transform/adapters/amazon_adapter.py`` extending
       ``BaseMarketplaceAdapter``.
    3. Append a ``ChannelSpec(...)`` entry to ``CHANNELS`` below.
    4. ``main.py`` will pick it up automatically.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional, Type

from src.transform.adapters.base_adapter import BaseMarketplaceAdapter


# A factory because the Client classes need lazy import — they read
# tenant settings populated at runtime by ``configurar_ambiente()``.
ClientFactory = Callable[[], object]


@dataclass(frozen=True)
class ChannelSpec:
    """Static description of one supported marketplace.

    Attributes:
        nome: Human-readable name (used in logs).
        id_canal: Canonical channel ID written to ``dim_canal_venda``
            and to every fact row.
        client_factory: Zero-arg callable that returns an authenticated
            extractor instance. Lazy so settings are loaded first.
        adapter_class: Concrete ``BaseMarketplaceAdapter`` subclass
            applied to the raw payload.
        opcional: When True, the orchestrator silently skips this channel
            if the tenant is not configured for it (e.g. Shopee on a
            Mercado-Livre-only account).
    """

    nome: str
    id_canal: int
    client_factory: ClientFactory
    adapter_class: Type[BaseMarketplaceAdapter]
    opcional: bool = False


# ---------------------------------------------------------------------------
# Lazy factories — keep settings imports lazy to respect the multi-tenant
# initialization order (see ``src.config.tenant.configurar_ambiente``).
# ---------------------------------------------------------------------------

def _meli_factory() -> object:
    from src.extract.meli_client import MercadoLivreClient
    return MercadoLivreClient()


def _shopee_factory() -> object:
    from src.extract.shopee_client import ShopeeClient
    return ShopeeClient()


def _meli_adapter() -> Type[BaseMarketplaceAdapter]:
    from src.transform.adapters.mercado_livre_adapter import MercadoLivreAdapter
    return MercadoLivreAdapter


def _shopee_adapter() -> Type[BaseMarketplaceAdapter]:
    from src.transform.adapters.shopee_adapter import ShopeeAdapter
    return ShopeeAdapter


# ---------------------------------------------------------------------------
# Registry — the only place that needs to change to add a marketplace.
# ---------------------------------------------------------------------------
# NOTE: ``adapter_class`` is wrapped in a callable so the import stays
# lazy (avoids forcing ``src.transform`` at config-load time).

CHANNELS: tuple[ChannelSpec, ...] = (
    ChannelSpec(
        nome="Mercado Livre",
        id_canal=1,
        client_factory=_meli_factory,
        adapter_class=_meli_adapter(),  # safe: import fast, no env reads
        opcional=False,
    ),
    ChannelSpec(
        nome="Shopee",
        id_canal=2,
        client_factory=_shopee_factory,
        adapter_class=_shopee_adapter(),
        opcional=True,
    ),
)


def encontrar_canal(nome: str) -> Optional[ChannelSpec]:
    """Returns the ChannelSpec whose ``nome`` matches (case-insensitive)."""
    nome_lower = nome.lower()
    for canal in CHANNELS:
        if canal.nome.lower() == nome_lower:
            return canal
    return None
