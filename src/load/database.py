"""
load.database
~~~~~~~~~~~~~
Load layer of the ETL pipeline.
Manages database connections, DDL schema creation, and DML
upsert operations via staging tables. Receives pre-processed
DataFrames only — no API or business logic belongs here.
"""

import logging
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config.settings import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection (singleton — reuses the same connection pool)
# ---------------------------------------------------------------------------
_engine: Optional[Engine] = None


def conectar_mysql() -> Engine:
    """Returns a SQLAlchemy Engine for MySQL.

    Uses the singleton pattern: the Engine (and its connection pool)
    is created only once and reused across all subsequent calls.

    Returns:
        Engine configured with credentials from .env.
    """
    global _engine
    if _engine is None:
        url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        _engine = create_engine(url, pool_pre_ping=True)
        logger.debug("Engine MySQL criada: %s:%s/%s", DB_HOST, DB_PORT, DB_NAME)
    return _engine


# ---------------------------------------------------------------------------
# DDL — Table creation
# ---------------------------------------------------------------------------

def criar_tabelas(engine: Engine) -> None:
    """Creates the Star Schema tables if they do not already exist.

    Tables created:
        - ``dim_cliente`` (Dimension)
        - ``dim_produto`` (Dimension)
        - ``fato_pedido`` (Fact — header)
        - ``fato_itens_pedido`` (Fact — line item)
        - ``fato_custos_ads`` (Fact — daily advertising costs)
        - ``fato_custos_operacionais`` (Fact — operational costs)

    Args:
        engine: SQLAlchemy Engine connected to MySQL.
    """
    with engine.begin() as conn:
        conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS dim_cliente (
                    id_cliente BIGINT PRIMARY KEY,
                    nickname VARCHAR(100) NOT NULL,
                    nome_completo VARCHAR(255)
                );
            """)
        )

        conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS dim_produto (
                    id_produto VARCHAR(50) PRIMARY KEY,
                    sku VARCHAR(100),
                    descricao VARCHAR(255) NOT NULL,
                    custo_unitario DECIMAL(10,2) DEFAULT 0.00
                );
            """)
        )

        conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS fato_pedido (
                    id_pedido BIGINT PRIMARY KEY,
                    id_cliente BIGINT,
                    data_criacao DATETIME NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    valor_produtos DECIMAL(10,2) NOT NULL,
                    custo_frete DECIMAL(10,2) DEFAULT 0.00,
                    total_pago_comprador DECIMAL(10,2) NOT NULL,
                    FOREIGN KEY (id_cliente) REFERENCES dim_cliente(id_cliente)
                );
            """)
        )

        conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS fato_itens_pedido (
                    id_registro INT AUTO_INCREMENT PRIMARY KEY,
                    id_pedido BIGINT NOT NULL,
                    id_produto VARCHAR(50) NOT NULL,
                    quantidade INT NOT NULL,
                    preco_unitario DECIMAL(10,2) NOT NULL,
                    taxa_venda DECIMAL(10,2) DEFAULT 0.00,
                    FOREIGN KEY (id_pedido) REFERENCES fato_pedido(id_pedido),
                    FOREIGN KEY (id_produto) REFERENCES dim_produto(id_produto),
                    UNIQUE KEY unique_item (id_pedido, id_produto)
                );
            """)
        )

        # Ads cost fact table with composite PK for upsert idempotency
        conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS fato_custos_ads (
                    data_metrica DATE,
                    id_campanha VARCHAR(50),
                    nome_campanha VARCHAR(255),
                    impressoes INT,
                    cliques INT,
                    custo DECIMAL(10,2) DEFAULT 0.00,
                    receita DECIMAL(10,2) DEFAULT 0.00,
                    PRIMARY KEY (data_metrica, id_campanha)
                );
            """)
        )
        conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS fato_custos_operacionais (
                    data_metrica DATE,
                    tipo_custo VARCHAR(100),
                    valor DECIMAL(10,2) DEFAULT 0.00,
                    PRIMARY KEY (data_metrica, tipo_custo)
                );
            """)
        )

    logger.info("Star Schema tables verified/created successfully.")


# ---------------------------------------------------------------------------
# DML — Upsert via staging tables
# ---------------------------------------------------------------------------

def salvar_no_banco(
    df_dim_cliente: pd.DataFrame,
    df_dim_produto: pd.DataFrame,
    df_fato_pedido: pd.DataFrame,
    df_fato_itens_pedido: pd.DataFrame,
) -> None:
    """Inserts transformed data into MySQL using staging tables
    and upsert logic (ON DUPLICATE KEY UPDATE).

    Staging tables are created within the same transaction to
    ensure atomicity — if the upsert fails, rollback undoes
    everything, including the staging tables.

    Args:
        df_dim_cliente: Customer dimension.
        df_dim_produto: Product dimension (with custo_unitario populated).
        df_fato_pedido: Order fact header.
        df_fato_itens_pedido: Order item fact lines.
    """
    engine = conectar_mysql()
    criar_tabelas(engine)

    try:
        with engine.begin() as conn:
            # --- 1. UPSERT CLIENTES ---
            if not df_dim_cliente.empty:
                df_dim_cliente.to_sql(
                    "stg_clientes", con=conn, if_exists="replace", index=False
                )
                conn.execute(
                    text("""
                        INSERT INTO dim_cliente (id_cliente, nickname, nome_completo)
                        SELECT id_cliente, nickname, nome_completo FROM stg_clientes
                        ON DUPLICATE KEY UPDATE
                            nickname = VALUES(nickname),
                            nome_completo = VALUES(nome_completo);
                    """)
                )
                conn.execute(text("DROP TABLE IF EXISTS stg_clientes;"))
                logger.info(
                    "Upsert Clientes: %d registros processados.",
                    len(df_dim_cliente),
                )

            # --- 2. UPSERT PRODUTOS ---
            if not df_dim_produto.empty:
                df_dim_produto.to_sql(
                    "stg_produtos", con=conn, if_exists="replace", index=False
                )
                conn.execute(
                    text("""
                        INSERT INTO dim_produto (id_produto, sku, descricao, custo_unitario)
                        SELECT id_produto, sku, descricao, custo_unitario FROM stg_produtos
                        ON DUPLICATE KEY UPDATE
                            sku = VALUES(sku),
                            descricao = VALUES(descricao);
                    """)
                )
                conn.execute(text("DROP TABLE IF EXISTS stg_produtos;"))
                logger.info(
                    "Upsert Produtos: %d registros processados.",
                    len(df_dim_produto),
                )

            # --- 3. UPSERT PEDIDOS ---
            if not df_fato_pedido.empty:
                df_fato_pedido.to_sql(
                    "stg_pedidos", con=conn, if_exists="replace", index=False
                )
                conn.execute(
                    text("""
                        INSERT INTO fato_pedido (id_pedido, id_cliente, data_criacao,
                                               status, valor_produtos, custo_frete,
                                               total_pago_comprador)
                        SELECT id_pedido, id_cliente, data_criacao, status,
                               valor_produtos, custo_frete, total_pago_comprador
                        FROM stg_pedidos
                        ON DUPLICATE KEY UPDATE
                            status = VALUES(status),
                            total_pago_comprador = VALUES(total_pago_comprador),
                            custo_frete = VALUES(custo_frete);
                    """)
                )
                conn.execute(text("DROP TABLE IF EXISTS stg_pedidos;"))
                logger.info(
                    "Upsert Pedidos: %d registros processados.",
                    len(df_fato_pedido),
                )

            # --- 4. INSERT ITENS (IGNORE duplicados) ---
            if not df_fato_itens_pedido.empty:
                df_fato_itens_pedido.to_sql(
                    "stg_itens", con=conn, if_exists="replace", index=False
                )
                conn.execute(
                    text("""
                        INSERT IGNORE INTO fato_itens_pedido
                            (id_pedido, id_produto, quantidade,
                             preco_unitario, taxa_venda)
                        SELECT id_pedido, id_produto, quantity,
                               unit_price, sale_fee
                        FROM stg_itens;
                    """)
                )
                conn.execute(text("DROP TABLE IF EXISTS stg_itens;"))
                logger.info(
                    "Upsert Itens: %d registros processados.", len(df_fato_itens_pedido)
                )

    except Exception as exc:
        logger.error("Critical database error: %s", exc, exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Cost update in database
# ---------------------------------------------------------------------------

def atualizar_custos_no_banco(df_custos: pd.DataFrame) -> int:
    """Updates the ``custo_unitario`` column in ``dim_produto``
    using the pre-processed cost DataFrame.

    Receives a DataFrame (output of
    ``extract.local_data.carregar_planilha_custos``) and executes
    batch UPDATEs via a staging table.

    Args:
        df_custos: DataFrame with ``sku`` (str) and ``custo`` (float) columns.

    Returns:
        Number of products updated in the database.
    """
    if df_custos.empty:
        logger.warning("DataFrame de custos vazio. Nenhum UPDATE executado.")
        return 0

    engine = conectar_mysql()
    produtos_atualizados = 0

    try:
        with engine.begin() as conn:
            # Staging with costs — within the same transaction
            df_custos[["sku", "custo"]].to_sql(
                "stg_custos", con=conn, if_exists="replace", index=False
            )

            resultado = conn.execute(
                text("""
                    UPDATE dim_produto p
                    INNER JOIN stg_custos c ON p.sku = c.sku
                    SET p.custo_unitario = c.custo;
                """)
            )
            produtos_atualizados = resultado.rowcount

            conn.execute(text("DROP TABLE IF EXISTS stg_custos;"))

        logger.info(
            "Custos atualizados no banco: %d produtos.", produtos_atualizados
        )

    except Exception as exc:
        logger.error(
            "Erro ao atualizar custos no banco: %s", exc, exc_info=True
        )
        raise

    return produtos_atualizados


# ---------------------------------------------------------------------------
# Query — incremental load
# ---------------------------------------------------------------------------

def obter_ultima_data_pedido() -> Optional[str]:
    """Fetches the most recent order date from the database to
    enable incremental loading.

    Returns:
        Date formatted in the ISO 8601 pattern required by the API,
        or None if the database is empty or the table does not exist.
    """
    engine = conectar_mysql()
    try:
        with engine.connect() as conn:
            resultado = conn.execute(
                text("SELECT MAX(data_criacao) FROM fato_pedido")
            ).scalar()
            if resultado:
                return resultado.strftime("%Y-%m-%dT%H:%M:%S.000-00:00")
    except Exception as exc:
        logger.warning("Aviso ao buscar última data no banco: %s", exc)

    return None


def salvar_custos_operacionais(df_fato_custos_operacionais: pd.DataFrame) -> int:
    """Inserts/updates operational costs via staging table.

    Args:
        df_fato_custos_operacionais: DataFrame with columns: data_metrica, tipo_custo, valor.

    Returns:
        Number of records processed.
    """
    if df_fato_custos_operacionais.empty:
        return 0

    engine = conectar_mysql()

    with engine.begin() as conn:
        df_fato_custos_operacionais.to_sql("stg_custos_op", con=conn, if_exists="replace", index=False)

        conn.execute(text("""
            INSERT INTO fato_custos_operacionais (data_metrica, tipo_custo, valor)
            SELECT data_metrica, tipo_custo, valor
            FROM stg_custos_op
            ON DUPLICATE KEY UPDATE
                valor = VALUES(valor);
        """))
        conn.execute(text("DROP TABLE IF EXISTS stg_custos_op;"))

    registros = len(df_fato_custos_operacionais)
    logger.info("Custos operacionais atualizados: %d registros.", registros)
    return registros