"""
transform.data_processor
~~~~~~~~~~~~~~~~~~~~~~~~~
Camada de transformação do pipeline ETL.
Recebe dados brutos (API + planilha) e aplica limpeza, tipagem,
cruzamento (merge) e regras de negócio.

Retorna estruturas prontas para inserção no banco de dados.
Não faz acesso a API nem a banco de dados.
"""

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Transformação principal — Star Schema
# ---------------------------------------------------------------------------

def processar_pedidos(
    dados_brutos: list[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Transforma a lista de pedidos brutos da API em 4 DataFrames
    seguindo o Star Schema (Clientes, Produtos, Pedidos, Itens).

    Args:
        dados_brutos: Lista de dicionários retornados pela API do ML,
            já enriquecidos com o campo ``custo_frete_real``.

    Returns:
        Tupla de 4 DataFrames:
        ``(df_clientes, df_produtos, df_pedidos, df_itens_pedido)``
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

        # --- 1. Cliente (Dimensão) ---
        comprador = pedido.get("buyer", {})
        id_cliente = comprador.get("id")
        nickname = comprador.get("nickname", "")

        clientes.append(
            {
                "id_cliente": id_cliente,
                "nickname": nickname,
                "nome_completo": "",  # Preenchimento padrão (dados omitidos pela origem).
            }
        )

        # --- 2. Custo de frete (maior entre extrato financeiro e envio) ---
        frete_financeiro = _extrair_frete_financeiro(pedido)
        frete_multiget = pedido.get("custo_frete_real", 0.0)
        frete_final = max(frete_financeiro, frete_multiget)

        # --- 3. Pedido (Fato Cabeçalho) ---
        pedidos.append(
            {
                "id_pedido": id_pedido,
                "id_cliente": id_cliente,
                "data_criacao": data_criacao,
                "status": status,
                "valor_produtos": total_produtos,
                "custo_frete": frete_final,
                "total_pago_comprador": total_pago_comprador,
            }
        )

        # --- 4. Itens e Produtos (Dimensão + Fato Linha) ---
        for item in pedido.get("order_items", []):
            produto = item.get("item", {})
            id_produto = produto.get("id")

            produtos.append(
                {
                    "id_produto": id_produto,
                    "sku": produto.get("seller_sku", ""),
                    "descricao": produto.get("title", ""),
                    "custo_unitario": 0.00,  # preenchido via merge de custos
                }
            )

            itens_pedido.append(
                {
                    "id_pedido": id_pedido,
                    "id_produto": id_produto,
                    "quantidade": item.get("quantity", 1),
                    "preco_unitario": item.get("unit_price", 0.0),
                    "taxa_venda": item.get("sale_fee", 0.0),
                }
            )

    # Converte para DataFrames
    df_clientes = pd.DataFrame(clientes)
    df_produtos = pd.DataFrame(produtos)
    df_pedidos = pd.DataFrame(pedidos)
    df_itens_pedido = pd.DataFrame(itens_pedido)

    # Remove duplicatas das dimensões (mantém o registro mais recente)
    if not df_clientes.empty:
        df_clientes = df_clientes.drop_duplicates(
            subset=["id_cliente"], keep="last"
        )

    if not df_produtos.empty:
        df_produtos = df_produtos.drop_duplicates(
            subset=["id_produto"], keep="last"
        )

    logger.info(
        "Processamento concluído — Clientes: %d | Produtos: %d | "
        "Pedidos: %d | Itens: %d",
        len(df_clientes),
        len(df_produtos),
        len(df_pedidos),
        len(df_itens_pedido),
    )

    return df_clientes, df_produtos, df_pedidos, df_itens_pedido


# ---------------------------------------------------------------------------
# Enriquecimento de custos via planilha
# ---------------------------------------------------------------------------

def enriquecer_produtos_com_custos(
    df_produtos: pd.DataFrame,
    df_custos: pd.DataFrame,
) -> pd.DataFrame:
    """Cruza a dimensão de produtos com a planilha de custos pelo SKU,
    preenchendo a coluna ``custo_unitario``.

    Utiliza ``pd.merge`` (left join) para associar cada produto ao seu
    custo, sem perder produtos que não tenham correspondência na planilha.

    Args:
        df_produtos: DataFrame de produtos (saída de ``processar_pedidos``).
        df_custos: DataFrame de custos (saída de ``carregar_planilha_custos``).

    Returns:
        DataFrame de produtos com ``custo_unitario`` preenchido onde
        houver correspondência de SKU.
    """
    if df_produtos.empty:
        logger.warning("DataFrame de produtos está vazio. Merge ignorado.")
        return df_produtos

    if df_custos.empty:
        logger.warning("DataFrame de custos está vazio. Merge ignorado.")
        return df_produtos

    # Trabalha em cópia para não modificar o DataFrame original do chamador
    df_produtos = df_produtos.copy()

    # Normaliza SKU nos produtos para garantir match
    df_produtos["sku_normalizado"] = (
        df_produtos["sku"]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )

    # Prepara a planilha de custos para o merge
    df_custos_merge = df_custos[["sku", "custo"]].copy()
    df_custos_merge = df_custos_merge.rename(
        columns={"custo": "custo_planilha"}
    )

    # Left join — preserva todos os produtos
    df_merged = df_produtos.merge(
        df_custos_merge,
        left_on="sku_normalizado",
        right_on="sku",
        how="left",
        suffixes=("", "_custo"),
    )

    # Preenche custo_unitario onde houver correspondência
    mask = df_merged["custo_planilha"].notna()
    df_merged.loc[mask, "custo_unitario"] = df_merged.loc[
        mask, "custo_planilha"
    ]

    # Remove colunas auxiliares
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
# Funções auxiliares privadas
# ---------------------------------------------------------------------------

def _extrair_frete_financeiro(pedido: dict[str, Any]) -> float:
    """Extrai o custo de frete embutido nas tarifas financeiras do pedido.

    O Mercado Livre pode reportar o custo de frete sob os tipos
    ``shipping_fee`` ou ``shipping_cost`` dentro de ``fee_details``.

    Args:
        pedido: Dicionário bruto de um pedido da API.

    Returns:
        Custo de frete financeiro em reais (float).
    """
    tarifas = pedido.get("fee_details", [])
    frete = 0.0

    for tarifa in tarifas:
        if tarifa.get("type") in ("shipping_fee", "shipping_cost"):
            frete += tarifa.get("amount", 0.0)

    return frete
