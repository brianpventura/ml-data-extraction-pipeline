"""
main — Multi-Tenant ETL Pipeline Orchestrator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Coordinates the complete flow for a specific store (tenant):
    0. Setup   -> Select store, load its .env, initialize settings
    1. Extract -> Orders (every channel in CHANNELS) + Costs (Spreadsheet/JSON)
    2. Transform -> Star Schema via per-channel adapter
    3. Load    -> MySQL (Upsert + Cost update)

The set of marketplaces is driven by ``src.config.channels.CHANNELS``,
so adding a new channel does NOT require changes to this file.
"""

import datetime
import logging
import sys
from typing import Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Logging (configured before anything else)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
for noisy in (
    "urllib3", "urllib3.connectionpool", "requests",
    "sqlalchemy.engine", "sqlalchemy.pool",
):
    logging.getLogger(noisy).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Multi-tenant setup (must run BEFORE any src.* import that reads settings)
# ---------------------------------------------------------------------------

from src.config.tenant import configurar_ambiente


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _calcular_data_retroativa(dias: int) -> str:
    """Calculates a retroactive ISO 8601 date from current UTC time."""
    data_passada = (
        datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(days=dias)
    )
    return data_passada.strftime("%Y-%m-%dT%H:%M:%S.000-00:00")


def _carregar_custos_combinados() -> Optional[pd.DataFrame]:
    """Loads cost data from all available sources and consolidates by SKU."""
    from src.config.settings import get_caminho_custos, get_caminho_json_custos
    from src.extract.local_data import carregar_planilha_custos, carregar_json_custos

    dfs: list[pd.DataFrame] = []

    try:
        dfs.append(carregar_planilha_custos(get_caminho_custos()))
    except (FileNotFoundError, ValueError) as exc:
        logger.warning("Planilha de custos indisponivel (%s).", exc)

    try:
        dfs.append(carregar_json_custos(get_caminho_json_custos()))
    except (FileNotFoundError, ValueError) as exc:
        logger.warning("JSON de custos indisponivel (%s).", exc)

    valid = [df for df in dfs if df is not None and not df.empty]
    if not valid:
        logger.warning("Custos nao serao enriquecidos nesta execucao.")
        return None

    df = pd.concat(valid, ignore_index=True).drop_duplicates(
        subset=["sku"], keep="last"
    )
    logger.info("Custos consolidados de ambas as fontes: %d SKUs.", len(df))
    return df


def _despachar_modulo(
    modulo_fn,
    dt_inicio_str: Optional[str],
    dt_fim_str: Optional[str],
    dias: int,
    escolha_bruta: str,
) -> None:
    """Dispatches an extraction module with the correct date parameters."""
    if dt_inicio_str and dt_fim_str:
        modulo_fn(data_inicio_str=dt_inicio_str, data_fim_str=dt_fim_str)
    elif escolha_bruta.strip().isdigit():
        modulo_fn(dias_retroativos=dias)
    else:
        modulo_fn(dias_retroativos=30)


def _resolver_periodo(escolha: str) -> tuple[
    Optional[str], Optional[str], Optional[str], Optional[str], int
]:
    """Resolves user input into (data_inicio_iso, data_fim_iso,
    dt_inicio_str, dt_fim_str, dias)."""
    from src.load.database import obter_ultima_data_pedido

    data_inicio: Optional[str] = None
    data_fim: Optional[str] = None
    dt_inicio_str: Optional[str] = None
    dt_fim_str: Optional[str] = None
    dias = 30

    if "," in escolha:
        dt_inicio_str, dt_fim_str = (s.strip() for s in escolha.split(",", 1))
        data_inicio = f"{dt_inicio_str}T00:00:00.000-00:00"
        data_fim = f"{dt_fim_str}T23:59:59.000-00:00"
        logger.info("Modo Intervalo: %s ate %s.", dt_inicio_str, dt_fim_str)

    elif escolha.strip().isdigit():
        dias = int(escolha.strip())
        data_inicio = _calcular_data_retroativa(dias)
        logger.info("Modo Retroativo: ultimos %d dias.", dias)

    else:
        data_inicio = obter_ultima_data_pedido()
        if data_inicio:
            logger.info("Modo Incremental: a partir de %s.", data_inicio)
        else:
            logger.info("Carga Historica: banco vazio. Puxando todo o historico.")

    return data_inicio, data_fim, dt_inicio_str, dt_fim_str, dias


# ---------------------------------------------------------------------------
# Channel-driven extraction step
# ---------------------------------------------------------------------------

def _executar_canal(
    spec,
    data_inicio: Optional[str],
    data_fim: Optional[str],
) -> int:
    """Extracts + Transforms + Loads orders for ONE channel.

    Returns the number of orders processed (0 if the channel is
    optional and not configured for this tenant).
    """
    from src.load.database import salvar_no_banco
    from src.transform.data_processor import processar_pelo_canal

    logger.info("[%s] Validando token...", spec.nome)
    try:
        client = spec.client_factory()
        client.obter_token_acesso()
    except (ValueError, FileNotFoundError) as exc:
        if spec.opcional:
            logger.info("[%s] nao configurado para esta loja (pulando): %s",
                        spec.nome, exc)
            return 0
        raise

    logger.info("[%s] Extraindo pedidos...", spec.nome)
    dados_brutos = client.buscar_todos_pedidos(
        date_from=data_inicio, date_to=data_fim
    )

    if not dados_brutos:
        logger.info("[%s] Nenhum pedido novo no periodo.", spec.nome)
        return 0

    logger.info("[%s] Transformando %d pedidos...", spec.nome, len(dados_brutos))
    resultados = processar_pelo_canal(spec, dados_brutos)

    logger.info("[%s] Inserindo dados no MySQL...", spec.nome)
    salvar_no_banco(
        df_dim_cliente=resultados.get("df_dim_cliente"),
        df_dim_produto=pd.DataFrame(),
        df_fato_pedido=resultados.get("df_fato_pedido"),
        df_fato_itens_pedido=resultados.get("df_fato_itens"),
        df_dim_anuncios=resultados.get("df_dim_anuncios"),
        df_fato_transacoes=resultados.get("df_fato_transacoes"),
    )
    return len(dados_brutos)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def executar_pipeline(nome_loja: str) -> None:
    """Executes the complete ETL pipeline for the given store."""
    # --- Lazy imports (settings already loaded by configurar_ambiente) ---
    from src.config.channels import CHANNELS
    from src.load.database import atualizar_custos_no_banco
    from src.jobs.run_ads_update import atualizar_modulo_ads
    from src.jobs.run_costs_update import atualizar_modulo_operacional
    from src.jobs.run_shopee_ads_update import atualizar_modulo_shopee_ads

    print(f"\n  [Loja ativa] {nome_loja.upper()}")
    print("=====================================================\n")

    # --- Period selection interface ---
    print("Escolha o periodo de extracao:")
    print("[ ENTER  ] Incremental: Puxar a partir da ultima venda salva.")
    print("[ NUMERO ] Retroativo: Quantos dias para tras deseja buscar.")
    print("[ DATA   ] Intervalo: AAAA-MM-DD,AAAA-MM-DD")
    escolha = input("\nSua escolha: ")

    data_inicio, data_fim, dt_inicio_str, dt_fim_str, dias = (
        _resolver_periodo(escolha)
    )

    try:
        # --- Costs are loaded once and applied at the end ---
        df_custos = _carregar_custos_combinados()

        # ------------------------------------------------------------------
        # EXTRACT + TRANSFORM + LOAD — every registered marketplace
        # ------------------------------------------------------------------
        total_pedidos = 0
        for spec in CHANNELS:
            try:
                total_pedidos += _executar_canal(spec, data_inicio, data_fim)
            except Exception as exc:  # noqa: BLE001
                # A failure in one channel must not abort the others.
                logger.error("[%s] Falha durante extracao: %s",
                             spec.nome, exc, exc_info=True)

        # ------------------------------------------------------------------
        # Cost updates (always run, even when no orders came in)
        # ------------------------------------------------------------------
        if df_custos is not None and not df_custos.empty:
            logger.info("Atualizando custos no banco de dados...")
            atualizados = atualizar_custos_no_banco(df_custos)
            logger.info("%d produto(s) com custo atualizado.", atualizados)

        # ------------------------------------------------------------------
        # Standalone modules (Ads + Operational)
        # ------------------------------------------------------------------
        logger.info("Extraindo custos do Mercado Ads...")
        _despachar_modulo(atualizar_modulo_ads,
                          dt_inicio_str, dt_fim_str, dias, escolha)

        logger.info("Extraindo custos do Shopee Ads...")
        _despachar_modulo(atualizar_modulo_shopee_ads,
                          dt_inicio_str, dt_fim_str, dias, escolha)

        logger.info("Extraindo Custos Operacionais (Full e Devolucoes)...")
        _despachar_modulo(atualizar_modulo_operacional,
                          dt_inicio_str, dt_fim_str, dias, escolha)

        modo = "Standalone" if total_pedidos == 0 else "Completo"
        print(f"\n>>> Pipeline finalizado com sucesso ({modo})! "
              f"Loja: {nome_loja.upper()}")

    except Exception as exc:
        logger.critical("Erro critico durante a execucao: %s", exc, exc_info=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    loja = configurar_ambiente()
    executar_pipeline(loja)
