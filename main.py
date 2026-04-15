"""
main — Multi-Tenant ETL Pipeline Orchestrator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Coordinates the complete flow for a specific store (tenant):
    0. Setup   → Select store, load its .env, initialize settings
    1. Extract → Orders (Marketplace API) + Costs (Spreadsheet/JSON)
    2. Transform → Star Schema + Cost enrichment
    3. Load    → MySQL (Upsert + Cost update)
"""

import argparse
import datetime
import logging
import os
import sys
import pandas as pd
from typing import Optional

# ---------------------------------------------------------------------------
# Logging (configured before anything else)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Multi-tenant setup (must run BEFORE any src.* import)
# ---------------------------------------------------------------------------

def configurar_ambiente() -> str:
    """Determines the target store and loads its environment file.

    Resolution order:
        1. ``--loja`` CLI argument (e.g. ``python main.py --loja prohair``)
        2. Interactive ``input()`` prompt if no CLI argument was given.

    After resolving the store name, locates the corresponding
    ``.env.<store>`` file, validates its existence, and calls
    ``settings.inicializar()`` to populate all module-level
    configuration variables.

    Returns:
        Sanitized store name (e.g. 'prohair').
    """
    parser = argparse.ArgumentParser(
        description="ETL Pipeline — Mercado Livre (Multi-Tenant)",
    )
    parser.add_argument(
        "--loja",
        type=str,
        required=False,
        default=None,
        help="Nome da loja (ex: prohair, progrowth). "
             "Se omitido, será solicitado interativamente.",
    )
    args = parser.parse_args()

    # --- Resolve store name ---
    if args.loja:
        nome_loja = args.loja
    else:
        print("=====================================================")
        print("  Extrator de Dados do Mercado Livre — Multi-Tenant  ")
        print("=====================================================\n")
        nome_loja = input("Digite o nome da loja (ex: prohair, progrowth): ")

    # --- Sanitize ---
    nome_loja = nome_loja.strip().lower()

    if not nome_loja:
        print("\n[ERRO] Nenhum nome de loja foi informado.")
        sys.exit(1)

    # --- Locate and validate .env file ---
    env_file = f".env.{nome_loja}"

    if not os.path.exists(env_file):
        print(f"\n[ERRO] Arquivo '{env_file}' nao encontrado.")
        print(f"   A loja '{nome_loja}' nao esta cadastrada.")
        print(f"   Crie o arquivo '{env_file}' na raiz do projeto com as")
        print("   credenciais da loja antes de executar o pipeline.")
        sys.exit(1)

    # --- Load environment and initialize settings ---
    from src.config.settings import inicializar
    inicializar(env_file)

    logger.info("Ambiente configurado para a loja: '%s'", nome_loja)
    return nome_loja


# ---------------------------------------------------------------------------
# Helpers (use lazy imports — settings already loaded when called)
# ---------------------------------------------------------------------------

def _calcular_data_retroativa(dias: int) -> str:
    """Calculates a retroactive ISO 8601 date from the current UTC time.

    Args:
        dias: Number of days to go back.

    Returns:
        Date formatted for the marketplace API.
    """
    data_passada = datetime.datetime.now(
        datetime.timezone.utc
    ) - datetime.timedelta(days=dias)
    return data_passada.strftime("%Y-%m-%dT%H:%M:%S.000-00:00")


def _carregar_custos_combinados() -> Optional["pd.DataFrame"]:
    """Loads cost data from all available sources and consolidates by SKU.

    Reads both the Excel spreadsheet and JSON cost file, concatenates
    them, and deduplicates by SKU (last occurrence wins).

    Returns:
        Consolidated DataFrame with 'sku' and 'custo' columns,
        or None if no cost source is available.
    """
    import pandas as pd
    from src.config.settings import get_caminho_custos, get_caminho_json_custos
    from src.extract.local_data import carregar_planilha_custos, carregar_json_custos

    dfs: list[pd.DataFrame] = []

    try:
        dfs.append(carregar_planilha_custos(get_caminho_custos()))
    except (FileNotFoundError, ValueError) as exc:
        logger.warning("Planilha de custos indisponível (%s).", exc)

    try:
        dfs.append(carregar_json_custos(get_caminho_json_custos()))
    except (FileNotFoundError, ValueError) as exc:
        logger.warning("JSON de custos indisponível (%s).", exc)

    valid = [df for df in dfs if df is not None and not df.empty]
    if not valid:
        logger.warning("Custos não serão enriquecidos nesta execução.")
        return None

    df = pd.concat(valid, ignore_index=True)
    df = df.drop_duplicates(subset=["sku"], keep="last")
    logger.info("Custos consolidados de ambas as fontes: %d SKUs.", len(df))
    return df


def _despachar_modulo(
    modulo_fn,
    dt_inicio_str: Optional[str],
    dt_fim_str: Optional[str],
    dias: int,
    escolha_bruta: str,
) -> None:
    """Dispatches an extraction module with the correct date parameters.

    Centralizes the if/elif/else logic that determines whether to call
    a job with explicit dates, retroactive days, or the default 30 days.

    Args:
        modulo_fn: Callable (e.g., atualizar_modulo_ads).
        dt_inicio_str: Explicit start date string (YYYY-MM-DD), or None.
        dt_fim_str: Explicit end date string (YYYY-MM-DD), or None.
        dias: Number of retroactive days.
        escolha_bruta: Raw user input string.
    """
    if dt_inicio_str and dt_fim_str:
        modulo_fn(data_inicio_str=dt_inicio_str, data_fim_str=dt_fim_str)
    elif escolha_bruta.strip().isdigit():
        modulo_fn(dias_retroativos=dias)
    else:
        modulo_fn(dias_retroativos=30)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def executar_pipeline(nome_loja: str) -> None:
    """Executes the complete ETL pipeline for the given store.

    Args:
        nome_loja: Sanitized store name (already validated).
    """
    # --- Lazy imports (settings already loaded by configurar_ambiente) ---
    from src.extract.meli_client import MercadoLivreClient
    from src.load.database import (
        atualizar_custos_no_banco,
        obter_ultima_data_pedido,
        salvar_no_banco,
    )
    from src.transform.data_processor import (
        enriquecer_produtos_com_custos,
        processar_pedidos,
    )
    from src.jobs.run_ads_update import atualizar_modulo_ads
    from src.jobs.run_costs_update import atualizar_modulo_operacional

    print(f"\n  [Loja ativa] {nome_loja.upper()}")
    print("=====================================================\n")

    # --- Period selection interface ---
    print("Escolha o período de extração:")
    print("[ ENTER  ] Incremental: Puxar a partir da última venda salva.")
    print("[ NÚMERO ] Retroativo: Quantos dias para trás deseja buscar.")
    print("[ DATA   ] Intervalo específico: AAAA-MM-DD,AAAA-MM-DD (ex: 2025-01-01,2025-01-31)")
    escolha = input("\nSua escolha: ")

    data_inicio: Optional[str] = None
    data_fim: Optional[str] = None
    dt_inicio_str: Optional[str] = None
    dt_fim_str: Optional[str] = None
    dias: int = 30

    if "," in escolha:
        # Modo Intervalo: datas explícitas
        dt_inicio_str, dt_fim_str = escolha.split(",", 1)
        dt_inicio_str = dt_inicio_str.strip()
        dt_fim_str = dt_fim_str.strip()

        # Explicit date range — ISO format required by the Orders API
        data_inicio = f"{dt_inicio_str}T00:00:00.000-00:00"
        data_fim = f"{dt_fim_str}T23:59:59.000-00:00"
        logger.info("Modo Intervalo: buscando de %s até %s.", dt_inicio_str, dt_fim_str)
    elif escolha.strip().isdigit():
        dias = int(escolha.strip())
        data_inicio = _calcular_data_retroativa(dias)
        logger.info("Modo Retroativo: buscando pedidos dos últimos %d dias.", dias)
    else:
        data_inicio = obter_ultima_data_pedido()
        if data_inicio:
            logger.info(
                "Modo Incremental: puxando pedidos a partir de %s.", data_inicio
            )
        else:
            logger.info("Carga Histórica: banco vazio. Puxando todo o histórico.")

    try:
        # ==============================================================
        # EXTRACT
        # ==============================================================
        logger.info("Etapa 1/8: Obtendo token de acesso...")
        cliente_ml = MercadoLivreClient()
        cliente_ml.obter_token_acesso()
        logger.info("Token validado!")

        logger.info("Etapa 2/8: Extraindo pedidos da API...")
        dados_brutos = cliente_ml.buscar_todos_pedidos(
            date_from=data_inicio, date_to=data_fim
        )

        if not dados_brutos:
            logger.info(
                "Nenhum pedido novo encontrado. Banco já está atualizado!"
            )
            # Even without new orders, update costs if the spreadsheet changed
            df_custos = _carregar_custos_combinados()
            if df_custos is not None:
                atualizados = atualizar_custos_no_banco(df_custos)
                logger.info("Custos atualizados (standalone): %d produto(s).", atualizados)

            logger.info("Executando módulo de Ads (standalone)...")
            _despachar_modulo(atualizar_modulo_ads, dt_inicio_str, dt_fim_str, dias, escolha)

            logger.info("Executando módulo de Custos Operacionais (standalone)...")
            _despachar_modulo(atualizar_modulo_operacional, dt_inicio_str, dt_fim_str, dias, escolha)

            print(f"\n>>> Pipeline finalizado com sucesso (Modo Standalone)! Loja: {nome_loja.upper()}")
            return

        logger.info("Etapa 3/8: Extraindo fontes de custos...")
        df_custos = _carregar_custos_combinados()

        # ==============================================================
        # TRANSFORM
        # ==============================================================
        logger.info("Etapa 4/8: Transformando dados (Star Schema)...")
        df_dim_cliente, df_dim_produto, df_fato_pedido, df_fato_itens_pedido = processar_pedidos(
            dados_brutos
        )

        if df_custos is not None and not df_custos.empty:
            logger.info("Enriquecendo produtos com custos da planilha...")
            df_dim_produto = enriquecer_produtos_com_custos(df_dim_produto, df_custos)

        # ==============================================================
        # LOAD
        # ==============================================================
        logger.info("Etapa 5/8: Inserindo dados no MySQL...")
        salvar_no_banco(df_dim_cliente, df_dim_produto, df_fato_pedido, df_fato_itens_pedido)

        logger.info("Etapa 6/8: Atualizando custos no banco de dados...")
        if df_custos is not None and not df_custos.empty:
            atualizados = atualizar_custos_no_banco(df_custos)
            logger.info("%d produto(s) com custo atualizado.", atualizados)

        logger.info("Etapa 7/8: Extraindo custos do Mercado Ads...")
        _despachar_modulo(atualizar_modulo_ads, dt_inicio_str, dt_fim_str, dias, escolha)

        logger.info("Etapa 8/8: Extraindo Custos Operacionais (Full e Devoluções)...")
        _despachar_modulo(atualizar_modulo_operacional, dt_inicio_str, dt_fim_str, dias, escolha)

        print(f"\n>>> Pipeline finalizado com sucesso! Loja: {nome_loja.upper()} -- Dados prontos para o Power BI.")

    except Exception as exc:
        logger.critical("Erro crítico durante a execução: %s", exc, exc_info=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    loja = configurar_ambiente()
    executar_pipeline(loja)