"""
extract.local_data
~~~~~~~~~~~~~~~~~~
Local data source readers.
Loads cost data from Excel and JSON files into standardized
DataFrames with validated schemas. No business logic applied.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def carregar_planilha_custos(caminho: Path) -> pd.DataFrame:
    """Reads a cost spreadsheet (.xlsx) and returns a standardized DataFrame.

    SKU values are read as text to prevent automatic numeric inference
    (e.g., ``1010003`` being read as ``1010003.0``).

    Args:
        caminho: Absolute or relative path to the .xlsx file.

    Returns:
        DataFrame with at least ``sku`` (str) and ``custo`` (float) columns.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If required columns ``sku`` and ``custo`` are missing.
    """
    caminho = Path(caminho)

    if not caminho.exists():
        raise FileNotFoundError(
            f"Planilha de custos não encontrada: {caminho}"
        )

    logger.info("Reading cost spreadsheet: %s", caminho)

    # Read all columns as string to preserve SKU integrity
    df = pd.read_excel(caminho, dtype=str)

    # Standardize column names
    df.columns = df.columns.str.lower().str.strip()

    # Validate required columns
    colunas_obrigatorias = {"sku", "custo"}
    colunas_encontradas = set(df.columns)
    faltando = colunas_obrigatorias - colunas_encontradas

    if faltando:
        raise ValueError(
            f"Colunas obrigatórias não encontradas na planilha: {faltando}. "
            f"Colunas disponíveis: {list(df.columns)}"
        )

    # Clean SKU — remove residual .0 suffix from Excel and whitespace
    df["sku"] = (
        df["sku"]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )

    # Convert cost to float (accepts comma as decimal separator)
    df["custo"] = (
        df["custo"]
        .astype(str)
        .str.strip()
        .str.replace(",", ".", regex=False)
    )
    df["custo"] = pd.to_numeric(df["custo"], errors="coerce")

    # Remove rows without a valid SKU or numeric cost
    df = df.dropna(subset=["sku", "custo"])
    df = df[df["sku"] != "nan"]

    logger.info(
        "Spreadsheet loaded: %d valid cost records.", len(df)
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

    # Standardize column name to match pipeline convention
    df = df.rename(columns={"preco_custo": "custo"})

    df["sku"] = df["sku"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    df["custo"] = pd.to_numeric(df["custo"], errors="coerce")

    df = df.dropna(subset=["sku", "custo"])
    df = df[df["sku"] != "nan"]
    logger.info("JSON loaded: %d valid cost records.", len(df))
    return df
