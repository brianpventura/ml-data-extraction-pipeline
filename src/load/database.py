"""
load.database
~~~~~~~~~~~~~
Camada de carga (Load) do pipeline ETL.
Conecta ao MySQL, cria tabelas e executa operações de INSERT / UPDATE.
Recebe dados já limpos e transformados — nenhuma lógica de negócio
ou de API deve existir neste módulo.
"""

import logging
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config.settings import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Conexão (singleton — reutiliza o mesmo pool de conexões)
# ---------------------------------------------------------------------------
_engine: Optional[Engine] = None


def conectar_mysql() -> Engine:
    """Retorna uma Engine do SQLAlchemy para o MySQL.

    Usa o padrão singleton: a Engine (e seu pool de conexões) é criada
    apenas uma vez e reutilizada em todas as chamadas subsequentes.

    Returns:
        Engine configurada com as credenciais do .env.
    """
    global _engine
    if _engine is None:
        url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        _engine = create_engine(url, pool_pre_ping=True)
        logger.debug("Engine MySQL criada: %s:%s/%s", DB_HOST, DB_PORT, DB_NAME)
    return _engine


# ---------------------------------------------------------------------------
# DDL — Criação de tabelas
# ---------------------------------------------------------------------------

def criar_tabelas(engine: Engine) -> None:
    """Cria as tabelas do Star Schema caso ainda não existam.

    Tabelas criadas:
        - ``tb_cliente`` (Dimensão)
        - ``tb_produto`` (Dimensão)
        - ``tb_pedido`` (Fato — cabeçalho)
        - ``tb_itens_pedido`` (Fato — linha)
        - ``tb_custos_ads`` (Fato — custos diários de publicidade)

    Args:
        engine: Engine do SQLAlchemy conectada ao MySQL.
    """
    with engine.begin() as conn:
        conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS tb_cliente (
                    id_cliente BIGINT PRIMARY KEY,
                    nickname VARCHAR(100) NOT NULL,
                    nome_completo VARCHAR(255)
                );
            """)
        )

        conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS tb_produto (
                    id_produto VARCHAR(50) PRIMARY KEY,
                    sku VARCHAR(100),
                    descricao VARCHAR(255) NOT NULL,
                    custo_unitario DECIMAL(10,2) DEFAULT 0.00
                );
            """)
        )

        conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS tb_pedido (
                    id_pedido BIGINT PRIMARY KEY,
                    id_cliente BIGINT,
                    data_criacao DATETIME NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    valor_produtos DECIMAL(10,2) NOT NULL,
                    custo_frete DECIMAL(10,2) DEFAULT 0.00,
                    total_pago_comprador DECIMAL(10,2) NOT NULL,
                    FOREIGN KEY (id_cliente) REFERENCES tb_cliente(id_cliente)
                );
            """)
        )

        conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS tb_itens_pedido (
                    id_registro INT AUTO_INCREMENT PRIMARY KEY,
                    id_pedido BIGINT NOT NULL,
                    id_produto VARCHAR(50) NOT NULL,
                    quantidade INT NOT NULL,
                    preco_unitario DECIMAL(10,2) NOT NULL,
                    taxa_venda DECIMAL(10,2) DEFAULT 0.00,
                    FOREIGN KEY (id_pedido) REFERENCES tb_pedido(id_pedido),
                    FOREIGN KEY (id_produto) REFERENCES tb_produto(id_produto),
                    UNIQUE KEY unique_item (id_pedido, id_produto)
                );
            """)
        )

        # Tabela Fato de Custos de Ads configurada com Chave Composta visando garantir idempotência durante Upserts.
        conn.execute(
            text("""
                CREATE TABLE IF NOT EXISTS tb_custos_ads (
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
                CREATE TABLE IF NOT EXISTS tb_custos_operacionais (
                    data_metrica DATE,
                    tipo_custo VARCHAR(100),
                    valor DECIMAL(10,2) DEFAULT 0.00,
                    PRIMARY KEY (data_metrica, tipo_custo)
                );
            """)
        )

    logger.info("Tabelas do Star Schema verificadas/criadas com sucesso.")


# ---------------------------------------------------------------------------
# DML — Upsert via staging tables
# ---------------------------------------------------------------------------

def salvar_no_banco(
    df_clientes: pd.DataFrame,
    df_produtos: pd.DataFrame,
    df_pedidos: pd.DataFrame,
    df_itens: pd.DataFrame,
) -> None:
    """Insere os dados transformados no MySQL usando tabelas de staging
    e lógica de upsert (ON DUPLICATE KEY UPDATE).

    As staging tables são criadas dentro da mesma transação (via ``conn``)
    para garantir atomicidade — se o upsert falhar, o rollback desfaz
    tudo, incluindo as staging tables.

    Args:
        df_clientes: Dimensão clientes.
        df_produtos: Dimensão produtos (com custo_unitario preenchido).
        df_pedidos: Fato cabeçalho de pedidos.
        df_itens: Fato linha de itens do pedido.
    """
    engine = conectar_mysql()
    criar_tabelas(engine)

    try:
        with engine.begin() as conn:
            # --- 1. UPSERT CLIENTES ---
            if not df_clientes.empty:
                df_clientes.to_sql(
                    "stg_clientes", con=conn, if_exists="replace", index=False
                )
                conn.execute(
                    text("""
                        INSERT INTO tb_cliente (id_cliente, nickname, nome_completo)
                        SELECT id_cliente, nickname, nome_completo FROM stg_clientes
                        ON DUPLICATE KEY UPDATE
                            nickname = VALUES(nickname),
                            nome_completo = VALUES(nome_completo);
                    """)
                )
                conn.execute(text("DROP TABLE IF EXISTS stg_clientes;"))
                logger.info(
                    "Upsert Clientes: %d registros processados.",
                    len(df_clientes),
                )

            # --- 2. UPSERT PRODUTOS ---
            if not df_produtos.empty:
                df_produtos.to_sql(
                    "stg_produtos", con=conn, if_exists="replace", index=False
                )
                conn.execute(
                    text("""
                        INSERT INTO tb_produto (id_produto, sku, descricao, custo_unitario)
                        SELECT id_produto, sku, descricao, custo_unitario FROM stg_produtos
                        ON DUPLICATE KEY UPDATE
                            sku = VALUES(sku),
                            descricao = VALUES(descricao);
                    """)
                )
                conn.execute(text("DROP TABLE IF EXISTS stg_produtos;"))
                logger.info(
                    "Upsert Produtos: %d registros processados.",
                    len(df_produtos),
                )

            # --- 3. UPSERT PEDIDOS ---
            if not df_pedidos.empty:
                df_pedidos.to_sql(
                    "stg_pedidos", con=conn, if_exists="replace", index=False
                )
                conn.execute(
                    text("""
                        INSERT INTO tb_pedido (id_pedido, id_cliente, data_criacao,
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
                    len(df_pedidos),
                )

            # --- 4. INSERT ITENS (IGNORE duplicados) ---
            if not df_itens.empty:
                df_itens.to_sql(
                    "stg_itens", con=conn, if_exists="replace", index=False
                )
                conn.execute(
                    text("""
                        INSERT IGNORE INTO tb_itens_pedido
                            (id_pedido, id_produto, quantidade,
                             preco_unitario, taxa_venda)
                        SELECT id_pedido, id_produto, quantidade,
                               preco_unitario, taxa_venda
                        FROM stg_itens;
                    """)
                )
                conn.execute(text("DROP TABLE IF EXISTS stg_itens;"))
                logger.info(
                    "Upsert Itens: %d registros processados.", len(df_itens)
                )

    except Exception as exc:
        logger.error("Erro crítico no banco de dados: %s", exc, exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Atualização de custos no banco
# ---------------------------------------------------------------------------

def atualizar_custos_no_banco(df_custos: pd.DataFrame) -> int:
    """Atualiza a coluna ``custo_unitario`` na tabela ``tb_produto``
    usando a planilha de custos já processada.

    Recebe um DataFrame pré-processado (saída de
    ``extract.local_data.carregar_planilha_custos``) e executa UPDATEs
    em lote via staging table.

    Args:
        df_custos: DataFrame com colunas ``sku`` (str) e ``custo`` (float).

    Returns:
        Quantidade de produtos atualizados no banco.
    """
    if df_custos.empty:
        logger.warning("DataFrame de custos vazio. Nenhum UPDATE executado.")
        return 0

    engine = conectar_mysql()
    produtos_atualizados = 0

    try:
        with engine.begin() as conn:
            # Staging com custos — dentro da mesma transação
            df_custos[["sku", "custo"]].to_sql(
                "stg_custos", con=conn, if_exists="replace", index=False
            )

            resultado = conn.execute(
                text("""
                    UPDATE tb_produto p
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
# Consulta — carga incremental
# ---------------------------------------------------------------------------

def obter_ultima_data_pedido() -> Optional[str]:
    """Busca a data do pedido mais recente salvo no banco para
    viabilizar a carga incremental.

    Returns:
        Data formatada no padrão ISO 8601 exigido pela API do ML,
        ou None se o banco estiver vazio ou a tabela não existir.
    """
    engine = conectar_mysql()
    try:
        with engine.connect() as conn:
            resultado = conn.execute(
                text("SELECT MAX(data_criacao) FROM tb_pedido")
            ).scalar()
            if resultado:
                return resultado.strftime("%Y-%m-%dT%H:%M:%S.000-00:00")
    except Exception as exc:
        logger.warning("Aviso ao buscar última data no banco: %s", exc)

    return None


def salvar_custos_operacionais(df_op: pd.DataFrame) -> int:
    """Insere/atualiza custos operacionais via staging table.

    Args:
        df_op: DataFrame com colunas: data_metrica, tipo_custo, valor.

    Returns:
        Quantidade de registros processados.
    """
    if df_op.empty:
        return 0

    engine = conectar_mysql()

    with engine.begin() as conn:
        df_op.to_sql("stg_custos_op", con=conn, if_exists="replace", index=False)

        conn.execute(text("""
            INSERT INTO tb_custos_operacionais (data_metrica, tipo_custo, valor)
            SELECT data_metrica, tipo_custo, valor
            FROM stg_custos_op
            ON DUPLICATE KEY UPDATE
                valor = VALUES(valor);
        """))
        conn.execute(text("DROP TABLE IF EXISTS stg_custos_op;"))

    registros = len(df_op)
    logger.info("Custos operacionais atualizados: %d registros.", registros)
    return registros