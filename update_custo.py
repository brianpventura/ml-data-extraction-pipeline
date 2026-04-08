"""
Script Avulso: Atualização em Lote de Preços de Custo
Lê o arquivo 'produtos_custo.xlsx' e atualiza a tabela tb_produto no MySQL.
"""

import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import text
import logging

# Garante que a raiz do projeto esteja no path para importar o database.py
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.load.database import conectar_mysql

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger(__name__)

def executar_upsert_custos():
    caminho_arquivo = Path("material/produtos_custo.xlsx") # Ajuste a pasta se necessário
    
    if not caminho_arquivo.exists():
        logger.error(f"Arquivo não encontrado: {caminho_arquivo}")
        return

    logger.info(f"Lendo arquivo: {caminho_arquivo}")
    
    # 1. Injeta dtype=str para evitar inferência automática e truncamento indesejado nos SKUs originais.
    df = pd.read_excel(caminho_arquivo, dtype=str)
    df.columns = df.columns.str.lower().str.strip()

    # Valida as colunas
    if "sku" not in df.columns or "preco_custo" not in df.columns:
        logger.error("O arquivo deve conter as colunas exatas 'sku' e 'preco_custo'.")
        return

    # 2. Limpeza e Transformação
    # Remove os rastros de sufixos decimais típicos na exportação direta de planilhas.
    df["sku"] = df["sku"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    
    # Padroniza o separador decimal para ponto e converte para float.
    df["preco_custo"] = df["preco_custo"].astype(str).str.strip().str.replace(",", ".", regex=False)
    df["preco_custo"] = pd.to_numeric(df["preco_custo"], errors="coerce")

    # Remove linhas vazias ou inválidas
    df = df.dropna(subset=["sku", "preco_custo"])
    df = df[df["sku"] != "nan"]
    
    # Remove SKUs duplicados dentro da própria planilha, mantendo o último preço lido
    df = df.drop_duplicates(subset=["sku"], keep="last")

    if df.empty:
        logger.warning("Nenhum dado válido para atualizar após a limpeza.")
        return

    logger.info(f"{len(df)} SKUs prontos para atualização no banco de dados.")

    # 3. Injeção no Banco de Dados (Upsert / Update em lote)
    engine = conectar_mysql()
    try:
        with engine.begin() as conn:
            # Cria a tabela temporária
            df[["sku", "preco_custo"]].to_sql("stg_upsert_custos", con=conn, if_exists="replace", index=False)

            # Executa o UPDATE cruzando a tabela temporária com a tb_produto
            resultado = conn.execute(text("""
                UPDATE tb_produto p
                INNER JOIN stg_upsert_custos c ON p.sku = c.sku
                SET p.custo_unitario = c.preco_custo;
            """))
            
            # Exclusão via banco da tabela de staging ('teardown' do lote).
            conn.execute(text("DROP TABLE IF EXISTS stg_upsert_custos;"))
            
            logger.info(f"✅ SUCESSO: {resultado.rowcount} produtos tiveram seus custos atualizados no banco de dados!")

    except Exception as exc:
        logger.error(f"Erro ao salvar no banco: {exc}")

if __name__ == "__main__":
    executar_upsert_custos()