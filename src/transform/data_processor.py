"""
transform.data_processor
~~~~~~~~~~~~~~~~~~~~~~~~~
Transform layer of the ETL pipeline.

Receives raw API responses and runs them through the matching
``BaseMarketplaceAdapter`` to produce Star Schema DataFrames ready
for the load layer.

Design notes:
    - One generic ``processar_pedidos`` function does the work for
      every marketplace. The adapter chosen is what changes — picked
      from ``src.config.channels.CHANNELS``.
    - The legacy per-marketplace functions
      (``processar_pedidos_mercado_livre_v2`` / ``..._shopee_v2``) are
      kept as thin wrappers for backwards compatibility with code
      paths still importing them.
"""

import datetime
import logging
from typing import Type

import pandas as pd

from src.config.channels import CHANNELS, ChannelSpec
from src.config.utils import normalizar_sku
from src.transform.adapters.base_adapter import BaseMarketplaceAdapter

logger = logging.getLogger(__name__)


_EMPTY_RESULT: dict = {
    "df_fato_pedido": pd.DataFrame(),
    "df_fato_itens": pd.DataFrame(),
    "df_fato_transacoes": pd.DataFrame(),
    "df_dim_anuncios": pd.DataFrame(),
    "df_dim_cliente": pd.DataFrame(),
}


# ---------------------------------------------------------------------------
# Generic processor — one entry point for every marketplace
# ---------------------------------------------------------------------------

def processar_pedidos(
    dados_brutos: list,
    adapter_class: Type[BaseMarketplaceAdapter],
    id_canal: int,
    nome_canal: str = "",
) -> dict:
    """Runs ``dados_brutos`` through ``adapter_class`` and returns DataFrames.

    Args:
        dados_brutos: Raw list of order dicts from the marketplace API.
        adapter_class: Concrete adapter to apply (subclass of
            ``BaseMarketplaceAdapter``).
        id_canal: Canonical channel ID propagated into every row.
        nome_canal: Optional channel label used in log messages.

    Returns:
        Dict with five DataFrames keyed by their target table:
            - ``df_fato_pedido``
            - ``df_fato_itens``
            - ``df_fato_transacoes``
            - ``df_dim_anuncios``
            - ``df_dim_cliente``
    """
    if not dados_brutos:
        return {**_EMPTY_RESULT}

    adapter = adapter_class(raw_data=dados_brutos, id_canal=id_canal)

    df_fato_pedido = pd.DataFrame(adapter.padronizar_pedidos())

    df_fato_itens = pd.DataFrame(adapter.padronizar_itens())
    if not df_fato_itens.empty:
        df_fato_itens = df_fato_itens.groupby(
            ["id_pedido", "id_anuncio"], as_index=False
        ).agg({"quantidade": "sum", "preco_unitario": "max"})

    df_fato_transacoes = pd.DataFrame(adapter.padronizar_transacoes())

    df_dim_anuncios = pd.DataFrame(adapter.padronizar_anuncios())
    if not df_dim_anuncios.empty:
        df_dim_anuncios = df_dim_anuncios.drop_duplicates(
            subset=["id_anuncio"], keep="last"
        )

    df_dim_cliente = pd.DataFrame(adapter.padronizar_clientes())
    if not df_dim_cliente.empty:
        df_dim_cliente = df_dim_cliente.drop_duplicates(
            subset=["id_cliente"], keep="last"
        )

    logger.info(
        "Processamento %s concluido — Pedidos: %d | Itens: %d | "
        "Transacoes Fin: %d | Anuncios: %d | Clientes: %d",
        nome_canal or adapter_class.__name__,
        len(df_fato_pedido),
        len(df_fato_itens),
        len(df_fato_transacoes),
        len(df_dim_anuncios),
        len(df_dim_cliente),
    )

    return {
        "df_fato_pedido": df_fato_pedido,
        "df_fato_itens": df_fato_itens,
        "df_fato_transacoes": df_fato_transacoes,
        "df_dim_anuncios": df_dim_anuncios,
        "df_dim_cliente": df_dim_cliente,
    }


def processar_pelo_canal(spec: ChannelSpec, dados_brutos: list) -> dict:
    """Convenience: processes raw data for a given ``ChannelSpec``."""
    return processar_pedidos(
        dados_brutos=dados_brutos,
        adapter_class=spec.adapter_class,
        id_canal=spec.id_canal,
        nome_canal=spec.nome,
    )


# ---------------------------------------------------------------------------
# Backwards-compatible wrappers (delegated to the generic function)
# ---------------------------------------------------------------------------

def processar_pedidos_mercado_livre_v2(dados_brutos: list) -> dict:
    """Legacy wrapper. Prefer ``processar_pedidos`` + ``CHANNELS``."""
    spec = next(c for c in CHANNELS if c.nome == "Mercado Livre")
    return processar_pelo_canal(spec, dados_brutos)


def processar_pedidos_shopee_v2(dados_brutos: list) -> dict:
    """Legacy wrapper. Prefer ``processar_pedidos`` + ``CHANNELS``."""
    spec = next(c for c in CHANNELS if c.nome == "Shopee")
    return processar_pelo_canal(spec, dados_brutos)


# ---------------------------------------------------------------------------
# Cost enrichment via spreadsheet
# ---------------------------------------------------------------------------

def enriquecer_produtos_com_custos(
    df_dim_produto: pd.DataFrame,
    df_custos: pd.DataFrame,
) -> pd.DataFrame:
    """Joins the product dimension with the cost DataFrame by SKU,
    populating the ``custo_unitario`` column.

    Uses ``pd.merge`` (left join) to associate each product with its
    cost, without losing products that have no match.

    Args:
        df_dim_produto: Product DataFrame.
        df_custos: Cost DataFrame (output of ``carregar_planilha_custos``).

    Returns:
        Product DataFrame with ``custo_unitario`` populated where
        a SKU match was found.
    """
    if df_dim_produto.empty:
        logger.warning("DataFrame de produtos esta vazio. Merge ignorado.")
        return df_dim_produto

    if df_custos.empty:
        logger.warning("DataFrame de custos esta vazio. Merge ignorado.")
        return df_dim_produto

    df_dim_produto = df_dim_produto.copy()
    df_dim_produto["sku_normalizado"] = normalizar_sku(df_dim_produto["sku"])

    df_custos_merge = df_custos[["sku", "custo"]].copy()
    df_custos_merge = df_custos_merge.rename(columns={"custo": "custo_planilha"})

    df_merged = df_dim_produto.merge(
        df_custos_merge,
        left_on="sku_normalizado",
        right_on="sku",
        how="left",
        suffixes=("", "_custo"),
    )

    mask = df_merged["custo_planilha"].notna()
    df_merged.loc[mask, "custo_unitario"] = df_merged.loc[mask, "custo_planilha"]

    drop_cols = [c for c in ("sku_normalizado", "sku_custo", "custo_planilha")
                 if c in df_merged.columns]
    df_merged = df_merged.drop(columns=drop_cols)

    atualizados = int(mask.sum())
    logger.info(
        "Enriquecimento de custos: %d de %d produtos com custo atualizado.",
        atualizados, len(df_merged),
    )

    return df_merged


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _unix_para_datetime(ts: int) -> str:
    """Converts a Unix timestamp to a formatted datetime string."""
    if not ts:
        return ""
    try:
        return datetime.datetime.fromtimestamp(
            ts, tz=datetime.timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OSError):
        return ""
