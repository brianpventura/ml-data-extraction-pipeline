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

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main transformation — Star Schema
# ---------------------------------------------------------------------------

def processar_pedidos(
    dados_brutos: list[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Transforms raw API orders into 4 DataFrames following the
    Star Schema model (Customers, Products, Orders, Items).

    Args:
        dados_brutos: List of dictionaries from the marketplace API,
            already enriched with the ``custo_frete_real`` field.

        Returns:
        Tuple of 4 DataFrames:
        ``(df_dim_cliente, df_dim_produto, df_fato_pedido, df_fato_itens_pedido)``
    """
    clientes: list[dict] = []
    produtos: list[dict] = []
    pedidos: list[dict] = []
    itens_pedido: list[dict] = []

    for pedido in dados_brutos:
        id_pedido = pedido.get("id")
        data_criacao = pedido.get("date_created")
        status = pedido.get("status")
        total_pago_comprador = pedido.get("paid_amount", 0.0)
        total_produtos = pedido.get("total_amount", 0.0)

        # --- 1. Customer (Dimension) ---
        comprador = pedido.get("buyer", {})
        id_cliente = comprador.get("id")
        nickname = comprador.get("nickname", "")

        clientes.append(
            {
                "id_cliente": id_cliente,
                "nickname": nickname,
                "nome_completo": "",  # Default placeholder (omitted by source).
            }
        )

        # --- 2. Shipping cost (higher of financial extract vs. shipment) ---
        frete_financeiro = _extrair_frete_financeiro(pedido)
        frete_multiget = pedido.get("custo_frete_real", 0.0)
        frete_final = max(frete_financeiro, frete_multiget)

        # --- 3. Order (Fact Header) ---
        pedidos.append(
            {
                "id_pedido": id_pedido,
                "id_cliente": id_cliente,
                "data_criacao": data_criacao,
                "status": status,
                "valor_produtos": total_produtos,
                "custo_frete": frete_final,
                "total_pago_comprador": total_pago_comprador,
                "origem_venda": "MERCADO_LIVRE",
                "taxa_comissao": 0.0,
                "taxa_transacao": 0.0,
                "taxa_servico": 0.0,
            }
        )

        # --- 4. Items and Products (Dimension + Fact Line) ---
        for item in pedido.get("order_items", []):
            produto = item.get("item", {})
            id_produto = produto.get("id")

            produtos.append(
                {
                    "id_produto": id_produto,
                    "sku": produto.get("seller_sku", ""),
                    "descricao": produto.get("title", ""),
                    "custo_unitario": 0.00,  # Populated via cost merge
                }
            )

            itens_pedido.append(
                {
                    "id_pedido": id_pedido,
                    "id_produto": id_produto,
                    "quantidade": item.get("quantity", 1),
                    "preco_unitario": item.get("unit_price", 0.0),
                    "taxa_venda": item.get("sale_fee", 0.0),
                    "origem_venda": "MERCADO_LIVRE",
                }
            )

    # Convert to DataFrames
    df_dim_cliente = pd.DataFrame(clientes)
    df_dim_produto = pd.DataFrame(produtos)
    df_fato_pedido = pd.DataFrame(pedidos)
    df_fato_itens_pedido = pd.DataFrame(itens_pedido)

    # Deduplicate dimensions (keep most recent record)
    if not df_dim_cliente.empty:
        df_dim_cliente = df_dim_cliente.drop_duplicates(
            subset=["id_cliente"], keep="last"
        )

    if not df_dim_produto.empty:
        df_dim_produto = df_dim_produto.drop_duplicates(
            subset=["id_produto"], keep="last"
        )

    logger.info(
        "Processamento concluído — Clientes: %d | Produtos: %d | "
        "Pedidos: %d | Itens: %d",
        len(df_dim_cliente),
        len(df_dim_produto),
        len(df_fato_pedido),
        len(df_fato_itens_pedido),
    )

    return df_dim_cliente, df_dim_produto, df_fato_pedido, df_fato_itens_pedido


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

def processar_pedidos_shopee(
    dados_brutos: list[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Transforms raw Shopee API orders into 4 DataFrames following
    the same Star Schema model used for the Mercado Livre pipeline.

    Mapping:
        - ``order_sn`` → ``id_pedido`` (VARCHAR — alphanumeric)
        - ``buyer_user_id`` → ``id_cliente``
        - ``buyer_username`` → ``nickname``
        - ``create_time`` (Unix) → ``data_criacao`` (``%Y-%m-%d %H:%M:%S``)
        - ``item_list[].item_id`` → ``id_produto``
        - ``item_list[].item_sku`` → ``sku``
        - ``item_list[].model_quantity_purchased`` → ``quantidade``
        - ``item_list[].model_discounted_price`` → ``preco_unitario``
        - ``valor_produtos`` = sum(quantidade × preço) from ``item_list``
        - ``escrow_amount`` → ``total_pago_comprador``
        - ``origem_venda`` = ``'SHOPEE'``

    Args:
        dados_brutos: List of order dictionaries returned by
            ``ShopeeClient.buscar_todos_pedidos()``.

    Returns:
        Tuple of 4 DataFrames:
        ``(df_dim_cliente, df_dim_produto, df_fato_pedido, df_fato_itens_pedido)``
    """
    clientes: list[dict] = []
    produtos: list[dict] = []
    pedidos: list[dict] = []
    itens_pedido: list[dict] = []

    for pedido in dados_brutos:
        order_sn = str(pedido.get("order_sn", ""))
        id_cliente = pedido.get("buyer_user_id", 0)
        nickname = pedido.get("buyer_username", "")
        status = pedido.get("order_status", "")
        escrow_amount = float(pedido.get("escrow_amount", 0.0))

        # Convert Unix timestamp to datetime string
        create_time_unix = pedido.get("create_time", 0)
        data_criacao = _unix_para_datetime(create_time_unix)

        # --- 1. Customer (Dimension) ---
        clientes.append({
            "id_cliente": id_cliente,
            "nickname": nickname,
            "nome_completo": "",
        })

        # --- 2. Items and Products ---
        item_list = pedido.get("item_list", [])
        valor_produtos = 0.0

        for item in item_list:
            item_id = str(item.get("item_id", ""))
            item_sku = item.get("item_sku", "")
            item_name = item.get("item_name", "")
            quantidade = int(item.get("model_quantity_purchased", 1))
            preco_unitario = float(item.get("model_discounted_price", 0.0))

            valor_produtos += quantidade * preco_unitario

            produtos.append({
                "id_produto": item_id,
                "sku": item_sku,
                "descricao": item_name,
                "custo_unitario": 0.00,
            })

            itens_pedido.append({
                "id_pedido": order_sn,
                "id_produto": item_id,
                "quantidade": quantidade,
                "preco_unitario": preco_unitario,
                "taxa_venda": 0.0,
                "origem_venda": "SHOPEE",
            })

        # --- 3. Financial data from Escrow (if available) ---
        escrow = pedido.get("escrow_detail", {})
        order_income = escrow.get("order_income", {})

        taxa_comissao = float(order_income.get("commission_fee", 0.0) or 0.0)
        taxa_transacao = float(order_income.get("seller_transaction_fee", 0.0) or 0.0)
        taxa_servico = float(order_income.get("service_fee", 0.0) or 0.0)
        frete_escrow = float(order_income.get("actual_shipping_fee", 0.0) or 0.0)

        # --- 4. Order (Fact Header) ---
        pedidos.append({
            "id_pedido": order_sn,
            "id_cliente": id_cliente,
            "data_criacao": data_criacao,
            "status": status,
            "valor_produtos": valor_produtos,
            "custo_frete": frete_escrow,
            "total_pago_comprador": escrow_amount,
            "origem_venda": "SHOPEE",
            "taxa_comissao": taxa_comissao,
            "taxa_transacao": taxa_transacao,
            "taxa_servico": taxa_servico,
        })

    # Convert to DataFrames
    df_dim_cliente = pd.DataFrame(clientes)
    df_dim_produto = pd.DataFrame(produtos)
    df_fato_pedido = pd.DataFrame(pedidos)
    df_fato_itens_pedido = pd.DataFrame(itens_pedido)

    # Deduplicate dimensions
    if not df_dim_cliente.empty:
        df_dim_cliente = df_dim_cliente.drop_duplicates(
            subset=["id_cliente"], keep="last"
        )

    if not df_dim_produto.empty:
        df_dim_produto = df_dim_produto.drop_duplicates(
            subset=["id_produto"], keep="last"
        )

    logger.info(
        "Processamento Shopee concluido — Clientes: %d | Produtos: %d | "
        "Pedidos: %d | Itens: %d",
        len(df_dim_cliente),
        len(df_dim_produto),
        len(df_fato_pedido),
        len(df_fato_itens_pedido),
    )

    return df_dim_cliente, df_dim_produto, df_fato_pedido, df_fato_itens_pedido


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

def _extrair_frete_financeiro(pedido: dict[str, Any]) -> float:
    """Extracts shipping cost embedded in the order's financial fees.

    The marketplace may report shipping cost under the
    ``shipping_fee`` or ``shipping_cost`` types within ``fee_details``.

    Args:
        pedido: Raw order dictionary from the API.

    Returns:
        Financial shipping cost as a float.
    """
    tarifas = pedido.get("fee_details", [])
    frete = 0.0

    for tarifa in tarifas:
        if tarifa.get("type") in ("shipping_fee", "shipping_cost"):
            frete += tarifa.get("amount", 0.0)

    return frete
