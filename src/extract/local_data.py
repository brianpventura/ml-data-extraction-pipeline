"""
extract.local_data
~~~~~~~~~~~~~~~~~~
Responsável por ler fontes de dados locais (planilhas, arquivos CSV, etc.).
Retorna DataFrames limpos e padronizados, sem aplicar regras de negócio.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def carregar_planilha_custos(caminho: Path) -> pd.DataFrame:
    """Lê a planilha de custos (material/custo.xlsx) e retorna um DataFrame
    com as colunas padronizadas.

    O SKU é tratado como texto para evitar problemas de conversão numérica
    do Excel (ex: ``1010003`` lido como ``1010003.0``).

    Args:
        caminho: Caminho absoluto ou relativo para o arquivo .xlsx.

    Returns:
        DataFrame com pelo menos as colunas ``sku`` (str) e ``custo`` (float).

    Raises:
        FileNotFoundError: Se o arquivo não existir no caminho informado.
        ValueError: Se as colunas obrigatórias ``sku`` e ``custo`` não
            forem encontradas na planilha.
    """
    caminho = Path(caminho)

    if not caminho.exists():
        raise FileNotFoundError(
            f"Planilha de custos não encontrada: {caminho}"
        )

    logger.info("Lendo planilha de custos: %s", caminho)

    # Lê tudo como string para preservar integridade dos SKUs
    df = pd.read_excel(caminho, dtype=str)

    # Padroniza nomes de colunas
    df.columns = df.columns.str.lower().str.strip()

    # Valida colunas obrigatórias
    colunas_obrigatorias = {"sku", "custo"}
    colunas_encontradas = set(df.columns)
    faltando = colunas_obrigatorias - colunas_encontradas

    if faltando:
        raise ValueError(
            f"Colunas obrigatórias não encontradas na planilha: {faltando}. "
            f"Colunas disponíveis: {list(df.columns)}"
        )

    # Limpa SKU — remove .0 residual do Excel e espaços
    df["sku"] = (
        df["sku"]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )

    # Converte custo para float (aceita vírgula como separador decimal)
    df["custo"] = (
        df["custo"]
        .astype(str)
        .str.strip()
        .str.replace(",", ".", regex=False)
    )
    df["custo"] = pd.to_numeric(df["custo"], errors="coerce")

    # Remove linhas sem SKU válido ou sem custo numérico
    df = df.dropna(subset=["sku", "custo"])
    df = df[df["sku"] != "nan"]

    logger.info(
        "Planilha carregada: %d registros de custos válidos.", len(df)
    )

    return df


def carregar_json_custos(caminho: Path) -> pd.DataFrame:
    caminho = Path(caminho)
    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo JSON não encontrado: {caminho}")

    logger.info("Lendo JSON de custos: %s", caminho)
    df = pd.read_json(caminho)
    df.columns = df.columns.str.lower().str.strip()

    if "sku" not in df.columns or "preco_custo" not in df.columns:
        raise ValueError("O JSON deve conter as chaves 'sku' e 'preco_custo'.")

    # Padroniza a coluna para o restante do pipeline
    df = df.rename(columns={"preco_custo": "custo"})

    df["sku"] = df["sku"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    df["custo"] = pd.to_numeric(df["custo"], errors="coerce")

    df = df.dropna(subset=["sku", "custo"])
    df = df[df["sku"] != "nan"]
    logger.info("JSON carregado: %d registros de custos válidos.", len(df))
    return df
