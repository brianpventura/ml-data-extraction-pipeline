"""
Standalone Script: Batch Cost Price Update
Reads a cost spreadsheet and updates the dim_produto table in MySQL.
"""

import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import text
import logging
import argparse
import os

# Ensures project root is in sys.path when running as standalone script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))



logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger(__name__)

def configurar_ambiente() -> str:
    """Handles store selection and injects multi-tenant environment variables."""
    parser = argparse.ArgumentParser(description="Standalone Script: Batch Cost Price Update")
    parser.add_argument("--loja", type=str, required=False, default=None,
                        help="Nome da loja (ex: prohair, progrowth). Se omitido, será solicitado.")
    args = parser.parse_args()

    if args.loja:
        nome_loja = args.loja
    else:
        print("=====================================================")
        print("  Atualização em Lote de Custos — Multi-Tenant       ")
        print("=====================================================\n")
        nome_loja = input("Digite o nome da loja (ex: prohair, progrowth): ")

    nome_loja = nome_loja.strip().lower()
    if not nome_loja:
        logger.error("Nenhum nome de loja foi informado.")
        sys.exit(1)

    env_file = f".env.{nome_loja}"
    if not os.path.exists(env_file):
        logger.error(f"Arquivo '{env_file}' nao encontrado. A loja '{nome_loja}' nao esta cadastrada.")
        sys.exit(1)

    # Initialize environment settings natively
    from src.config.settings import inicializar
    inicializar(env_file)
    
    logger.info("Ambiente configurado para a loja: '%s'", nome_loja)
    return nome_loja

def executar_upsert_custos():
    loja = configurar_ambiente()
    
    from src.load.database import conectar_mysql
    
    caminho_arquivo = Path("material/produtos_custo.xlsx") # Ajuste a pasta se necessário
    
    if not caminho_arquivo.exists():
        logger.error(f"Arquivo nao encontrado: {caminho_arquivo}")
        return

    logger.info(f"Lendo arquivo: {caminho_arquivo}")
    
    # 1. Read as string to prevent automatic type inference and SKU truncation
    df = pd.read_excel(caminho_arquivo, dtype=str)
    df.columns = df.columns.str.lower().str.strip()

    # Validate columns
    if "sku" not in df.columns or "preco_custo" not in df.columns:
        logger.error("O arquivo deve conter as colunas exatas 'sku' e 'preco_custo'.")
        return

    # 2. Cleaning and Transformation
    # Remove residual decimal suffixes from spreadsheet export
    df["sku"] = df["sku"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    
    # Standardize decimal separator to dot and convert to float
    df["preco_custo"] = df["preco_custo"].astype(str).str.strip().str.replace(",", ".", regex=False)
    df["preco_custo"] = pd.to_numeric(df["preco_custo"], errors="coerce")

    # Remove empty or invalid rows
    df = df.dropna(subset=["sku", "preco_custo"])
    df = df[df["sku"] != "nan"]
    
    # Remove duplicate SKUs within the spreadsheet, keeping the last price
    df = df.drop_duplicates(subset=["sku"], keep="last")

    if df.empty:
        logger.warning("Nenhum dado válido para atualizar após a limpeza.")
        return

    logger.info(f"{len(df)} SKUs prontos para atualização no banco de dados.")

    # 3. Database Injection (Batch Upsert / Update)
    engine = conectar_mysql()
    try:
        with engine.begin() as conn:
            # Create staging table
            df[["sku", "preco_custo"]].to_sql("stg_upsert_custos", con=conn, if_exists="replace", index=False)

            # Cross-join staging table with dim_produto for batch UPDATE
            resultado = conn.execute(text("""
                UPDATE dim_produto p
                INNER JOIN stg_upsert_custos c ON p.sku = c.sku
                SET p.custo_unitario = c.preco_custo;
            """))
            
            # Staging table teardown
            conn.execute(text("DROP TABLE IF EXISTS stg_upsert_custos;"))
            
            logger.info(f"[SUCESSO] {resultado.rowcount} produtos tiveram seus custos atualizados na loja {loja.upper()}!")

    except Exception as exc:
        logger.error(f"Erro ao salvar no banco: {exc}")

if __name__ == "__main__":
    executar_upsert_custos()