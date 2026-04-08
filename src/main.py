"""
main — Orquestrador do Pipeline ETL
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Coordena o fluxo completo:
    1. Extract  → Pedidos (API ML) + Custos (Planilha)
    2. Transform → Star Schema + Enriquecimento de custos
    3. Load     → MySQL (Upsert + Atualização de custos)
"""

import datetime
import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# Garante que a raiz do projeto esteja no sys.path, independentemente
# de como o script for executado (direto, python -m, VS Code, etc.)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config.settings import get_caminho_custos, get_caminho_json_custos
from src.extract.local_data import carregar_planilha_custos, carregar_json_custos
from src.extract.mercadolivre_client import MercadoLivreClient
from src.load.database import (
    atualizar_custos_no_banco,
    obter_ultima_data_pedido,
    salvar_no_banco,
)
from src.transform.data_processor import (
    enriquecer_produtos_com_custos,
    processar_pedidos,
)
from src.atualizar_ads import atualizar_modulo_ads
from src.atualizar_custos_operacionais import atualizar_modulo_operacional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _calcular_data_retroativa(dias: int) -> str:
    """Calcula a data no formato ISO 8601 voltando X dias a partir de agora.

    Args:
        dias: Quantidade de dias retroativos.

    Returns:
        Data formatada para a API do Mercado Livre.
    """
    data_passada = datetime.datetime.now(
        datetime.timezone.utc
    ) - datetime.timedelta(days=dias)
    return data_passada.strftime("%Y-%m-%dT%H:%M:%S.000-00:00")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def executar_pipeline() -> None:
    """Executa o pipeline ETL completo."""

    print("=====================================================")
    print("  Extrator de Dados do Mercado Livre — Start Data   ")
    print("=====================================================\n")

    # --- Interface de escolha de período ---
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

        # Formato ISO obrigatório para a API de Pedidos do ML
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
            # Mesmo sem pedidos novos, atualiza custos caso a planilha tenha mudado
            _atualizar_custos_standalone()
            
            logger.info("Executando módulo de Ads (standalone)...")
            if dt_inicio_str and dt_fim_str:
                atualizar_modulo_ads(data_inicio_str=dt_inicio_str, data_fim_str=dt_fim_str)
            elif escolha.strip().isdigit():
                atualizar_modulo_ads(dias_retroativos=dias)
            else:
                atualizar_modulo_ads(dias_retroativos=30)

            logger.info("Executando módulo de Custos Operacionais (standalone)...")
            if dt_inicio_str and dt_fim_str:
                atualizar_modulo_operacional(data_inicio_str=dt_inicio_str, data_fim_str=dt_fim_str)
            elif escolha.strip().isdigit():
                atualizar_modulo_operacional(dias_retroativos=dias)
            else:
                atualizar_modulo_operacional(dias_retroativos=30)

            print("\n🚀 Pipeline finalizado com sucesso (Modo Standalone)! Dados prontos para o Power BI.")
            return

        logger.info("Etapa 3/8: Extraindo fontes de custos...")
        df_custos_planilha = None
        df_custos_json = None
        
        try:
            df_custos_planilha = carregar_planilha_custos(get_caminho_custos())
        except (FileNotFoundError, ValueError) as exc:
            logger.warning("Planilha de custos indisponível (%s).", exc)

        try:
            df_custos_json = carregar_json_custos(get_caminho_json_custos())
        except (FileNotFoundError, ValueError) as exc:
            logger.warning("JSON de custos indisponível (%s).", exc)

        cost_dfs = [df for df in [df_custos_planilha, df_custos_json] if df is not None and not df.empty]
        if cost_dfs:
            df_custos = pd.concat(cost_dfs, ignore_index=True)
            df_custos = df_custos.drop_duplicates(subset=["sku"], keep="last")
            logger.info("Custos consolidados de ambas as fontes: %d SKUs.", len(df_custos))
        else:
            df_custos = None
            logger.warning("Custos não serão enriquecidos nesta execução.")

        # ==============================================================
        # TRANSFORM
        # ==============================================================
        logger.info("Etapa 4/8: Transformando dados (Star Schema)...")
        df_clientes, df_produtos, df_pedidos, df_itens = processar_pedidos(
            dados_brutos
        )

        if df_custos is not None and not df_custos.empty:
            logger.info("Enriquecendo produtos com custos da planilha...")
            df_produtos = enriquecer_produtos_com_custos(df_produtos, df_custos)

        # ==============================================================
        # LOAD
        # ==============================================================
        logger.info("Etapa 5/8: Inserindo dados no MySQL...")
        salvar_no_banco(df_clientes, df_produtos, df_pedidos, df_itens)

        logger.info("Etapa 6/8: Atualizando custos no banco de dados...")
        if df_custos is not None and not df_custos.empty:
            atualizados = atualizar_custos_no_banco(df_custos)
            logger.info("%d produto(s) com custo atualizado.", atualizados)

        logger.info("Etapa 7/8: Extraindo custos do Mercado Ads...")
        if dt_inicio_str and dt_fim_str:
            atualizar_modulo_ads(data_inicio_str=dt_inicio_str, data_fim_str=dt_fim_str)
        elif escolha.strip().isdigit():
            atualizar_modulo_ads(dias_retroativos=dias)
        else:
            atualizar_modulo_ads(dias_retroativos=30)
        
        logger.info("Etapa 8/8: Extraindo Custos Operacionais (Full e Devoluções)...")
        if dt_inicio_str and dt_fim_str:
            atualizar_modulo_operacional(data_inicio_str=dt_inicio_str, data_fim_str=dt_fim_str)
        elif escolha.strip().isdigit():
            atualizar_modulo_operacional(dias_retroativos=dias)
        else:
            atualizar_modulo_operacional(dias_retroativos=30)

        print("\n🚀 Pipeline finalizado com sucesso! Dados prontos para o Power BI.")

    except Exception as exc:
        logger.critical("Erro crítico durante a execução: %s", exc, exc_info=True)
        sys.exit(1)


def _atualizar_custos_standalone() -> None:
    """Atualiza custos mesmo quando não há pedidos novos."""
    df_custos_planilha = None
    df_custos_json = None
    
    try:
        df_custos_planilha = carregar_planilha_custos(get_caminho_custos())
    except (FileNotFoundError, ValueError) as exc:
        logger.warning("Planilha de custos indisponível: %s", exc)

    try:
        df_custos_json = carregar_json_custos(get_caminho_json_custos())
    except (FileNotFoundError, ValueError) as exc:
        logger.warning("JSON de custos indisponível: %s", exc)

    cost_dfs = [df for df in [df_custos_planilha, df_custos_json] if df is not None and not df.empty]
    if cost_dfs:
        df_custos = pd.concat(cost_dfs, ignore_index=True)
        df_custos = df_custos.drop_duplicates(subset=["sku"], keep="last")
        atualizados = atualizar_custos_no_banco(df_custos)
        logger.info(
            "Custos atualizados (standalone): %d produto(s).", atualizados
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    executar_pipeline()