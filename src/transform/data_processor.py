"""
transform.data_processor
~~~~~~~~~~~~~~~~~~~~~~~~~
Transform layer of the ETL pipeline.
Receives raw API responses and local cost data, applies cleaning,
type casting, dimensional modeling (Star Schema), and cost
enrichment via SKU-based joins.

Returns structures ready for database insertion.
No API access or database operations belong here.
"""

import logging
import datetime
from typing import Any

import pandas as pd

from src.config.utils import normalizar_sku
from src.transform.adapters.mercado_livre_adapter import MercadoLivreAdapter
from src.transform.adapters.shopee_adapter import ShopeeAdapter

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main transformation — Star Schema
# ---------------------------------------------------------------------------

def processar_pedidos_mercado_livre_v2(dados_brutos: list) -> dict:
    """Parses Mercado Livre API orders into Star Schema DataFrames via Adapter.

    Args:
        dados_brutos: List of dictionary records.

    Returns:
        Dict mapped to 4 DataFrames.
    """
    if not dados_brutos:
        return {
            "df_fato_pedido": pd.DataFrame(),
            "df_fato_itens": pd.DataFrame(),
            "df_fato_transacoes": pd.DataFrame(),
            "df_dim_anuncios": pd.DataFrame(),
            "df_dim_cliente": pd.DataFrame()
        }

    adapter = MercadoLivreAdapter(raw_data=dados_brutos, id_canal=1)

    df_fato_pedido = pd.DataFrame(adapter.padronizar_pedidos())
    
    df_fato_itens = pd.DataFrame(adapter.padronizar_itens())
    if not df_fato_itens.empty:
        df_fato_itens = df_fato_itens.groupby(['id_pedido', 'id_anuncio'], as_index=False).agg({
            'quantidade': 'sum',
            'preco_unitario': 'max'
        })
        
    df_fato_transacoes = pd.DataFrame(adapter.padronizar_transacoes())
    
    df_dim_anuncios = pd.DataFrame(adapter.padronizar_anuncios())
    if not df_dim_anuncios.empty:
        df_dim_anuncios = df_dim_anuncios.drop_duplicates(subset=["id_anuncio"], keep="last")

    df_dim_cliente = pd.DataFrame(adapter.padronizar_clientes())
    if not df_dim_cliente.empty:
        df_dim_cliente = df_dim_cliente.drop_duplicates(subset=["id_cliente"], keep="last")

    logger.info(
        "Processamento ML (V2/Adapter) concluído — Pedidos: %d | Itens: %d | "
        "Transações Fin: %d | Anúncios: %d | Clientes: %d",
        len(df_fato_pedido),
        len(df_fato_itens),
        len(df_fato_transacoes),
        len(df_dim_anuncios),
        len(df_dim_cliente)
    )

    return {
        "df_fato_pedido": df_fato_pedido,
        "df_fato_itens": df_fato_itens,
        "df_fato_transacoes": df_fato_transacoes,
        "df_dim_anuncios": df_dim_anuncios,
        "df_dim_cliente": df_dim_cliente
    }


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
        df_dim_produto: Product DataFrame (output of ``processar_pedidos``).
        df_custos: Cost DataFrame (output of ``carregar_planilha_custos``).

    Returns:
        Product DataFrame with ``custo_unitario`` populated where
        a SKU match was found.
    """
    if df_dim_produto.empty:
        logger.warning("DataFrame de produtos está vazio. Merge ignorado.")
        return df_dim_produto

    if df_custos.empty:
        logger.warning("DataFrame de custos está vazio. Merge ignorado.")
        return df_dim_produto

    # Work on a copy to avoid mutating the caller's DataFrame
    df_dim_produto = df_dim_produto.copy()

    # Normalize SKU in products to ensure match
    df_dim_produto["sku_normalizado"] = normalizar_sku(df_dim_produto["sku"])

    # Prepare cost DataFrame for the merge
    df_custos_merge = df_custos[["sku", "custo"]].copy()
    df_custos_merge = df_custos_merge.rename(
        columns={"custo": "custo_planilha"}
    )

    # Left join — preserves all products
    df_merged = df_dim_produto.merge(
        df_custos_merge,
        left_on="sku_normalizado",
        right_on="sku",
        how="left",
        suffixes=("", "_custo"),
    )

    # Populate custo_unitario where a match was found
    mask = df_merged["custo_planilha"].notna()
    df_merged.loc[mask, "custo_unitario"] = df_merged.loc[
        mask, "custo_planilha"
    ]

    # Drop auxiliary columns
    colunas_drop = ["sku_normalizado", "sku_custo", "custo_planilha"]
    colunas_existentes = [c for c in colunas_drop if c in df_merged.columns]
    df_merged = df_merged.drop(columns=colunas_existentes)

    atualizados = int(mask.sum())
    logger.info(
        "Enriquecimento de custos: %d de %d produtos com custo atualizado.",
        atualizados,
        len(df_merged),
    )

    return df_merged


# ---------------------------------------------------------------------------
# Shopee transformation — Star Schema
# ---------------------------------------------------------------------------

def processar_pedidos_shopee_v2(dados_brutos: list) -> dict:
    """Parses Shopee API order payload into Star Schema DataFrames via Adapter.

    Args:
        dados_brutos: List of order dictionaries.

    Returns:
        Dict mapped to 4 DataFrames.
    """
    if not dados_brutos:
        return {
            "df_fato_pedido": pd.DataFrame(),
            "df_fato_itens": pd.DataFrame(),
            "df_fato_transacoes": pd.DataFrame(),
            "df_dim_anuncios": pd.DataFrame(),
            "df_dim_cliente": pd.DataFrame()
        }

    adapter = ShopeeAdapter(raw_data=dados_brutos, id_canal=2)

    df_fato_pedido = pd.DataFrame(adapter.padronizar_pedidos())
    
    df_fato_itens = pd.DataFrame(adapter.padronizar_itens())
    if not df_fato_itens.empty:
        df_fato_itens = df_fato_itens.groupby(['id_pedido', 'id_anuncio'], as_index=False).agg({
            'quantidade': 'sum',
            'preco_unitario': 'max'
        })
        
    df_fato_transacoes = pd.DataFrame(adapter.padronizar_transacoes())
    
    # Processa dim_anuncios e dropa duplicatas (garante id único)
    df_dim_anuncios = pd.DataFrame(adapter.padronizar_anuncios())
    if not df_dim_anuncios.empty:
        df_dim_anuncios = df_dim_anuncios.drop_duplicates(subset=["id_anuncio"], keep="last")

    df_dim_cliente = pd.DataFrame(adapter.padronizar_clientes())
    if not df_dim_cliente.empty:
        df_dim_cliente = df_dim_cliente.drop_duplicates(subset=["id_cliente"], keep="last")

    logger.info(
        "Processamento Shopee (V2/Adapter) concluído — Pedidos: %d | Itens: %d | "
        "Transações Fin: %d | Anúncios: %d | Clientes: %d",
        len(df_fato_pedido),
        len(df_fato_itens),
        len(df_fato_transacoes),
        len(df_dim_anuncios),
        len(df_dim_cliente)
    )

    return {
        "df_fato_pedido": df_fato_pedido,
        "df_fato_itens": df_fato_itens,
        "df_fato_transacoes": df_fato_transacoes,
        "df_dim_anuncios": df_dim_anuncios,
        "df_dim_cliente": df_dim_cliente
    }


# ---------------------------------------------------------------------------
# Private helper functions
# ---------------------------------------------------------------------------

def _unix_para_datetime(ts: int) -> str:
    """Converts a Unix timestamp to a formatted datetime string.

    Args:
        ts: Unix timestamp (seconds since epoch).

    Returns:
        Date string in ``%Y-%m-%d %H:%M:%S`` format, or empty string
        if the timestamp is zero/invalid.
    """
    if not ts:
        return ""
    try:
        return datetime.datetime.fromtimestamp(
            ts, tz=datetime.timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OSError):
        return ""
