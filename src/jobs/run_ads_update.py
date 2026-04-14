"""
run_ads_update — Advertising Metrics Extraction Job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Extracts daily cost metrics from Product Ads (PADS) campaigns
and persists them into the ads cost fact table.

API workflow:
  1. GET /advertising/advertisers?product_id=PADS  → resolve advertiser_id
  2. GET /advertising/advertisers/{id}/campaigns    → list active campaigns
  3. GET .../campaigns/{id}/metrics                 → daily metrics per campaign

Required headers:
  - Authorization: Bearer {access_token}
  - Content-Type: application/json
  - Api-Version: 1

Expected HTTP error codes:
  - 400: Missing required 'Api-Version: 1' header.
  - 404: Deprecated endpoint or inactive advertising module.
  - 405: Routes with strict GET-only support.
"""

import datetime
import logging
from typing import Any, Optional

import pandas as pd
import requests
from sqlalchemy import text

from src.extract.marketplace_client import MercadoLivreClient
from src.load.database import conectar_mysql
from src.config.settings import (
    ADS_BASE_URL,
    REQUEST_TIMEOUT,
    ADS_CHUNK_DAYS,
    ADS_BATCH_SIZE,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_ADS_BASE_URL = ADS_BASE_URL
_REQUEST_TIMEOUT = REQUEST_TIMEOUT


def _ads_headers(access_token: str) -> dict[str, str]:
    """Returns the mandatory headers for the Ads API.

    The 'Api-Version: 1' header is required — without it, the API
    returns a 400 "Type mismatch" error on otherwise valid requests.
    """
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Api-Version": "1",
    }


# ---------------------------------------------------------------------------
# Step 1: Obtain advertiser_id
# ---------------------------------------------------------------------------

def _obter_advertiser_id(access_token: str) -> Optional[int]:
    """Queries the advertiser_id for the seller's Product Ads (PADS).

    The advertiser_id is required for all subsequent API calls.
    Returns None if the Ads module is not activated.

    Returns:
        advertiser_id (int) or None if not found.
    """
    url = f"{_ADS_BASE_URL}/advertisers"
    params = {"product_id": "PADS"}
    headers = _ads_headers(access_token)

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=_REQUEST_TIMEOUT)

        if resp.status_code == 200:
            dados = resp.json()

            # Expected format: {"advertisers": [{"advertiser_id": <int>, ...}]}
            if isinstance(dados, dict) and "advertisers" in dados:
                lista = dados["advertisers"]
                if isinstance(lista, list) and len(lista) > 0:
                    adv_id = lista[0].get("advertiser_id")
                    if adv_id:
                        logger.info("Advertiser ID encontrado: %s", adv_id)
                        return adv_id

            # Fallback: direct list response
            if isinstance(dados, list) and len(dados) > 0:
                adv_id = dados[0].get("advertiser_id")
                if adv_id:
                    logger.info("Advertiser ID encontrado: %s", adv_id)
                    return adv_id

            # Fallback: direct object response
            if isinstance(dados, dict) and "advertiser_id" in dados:
                adv_id = dados["advertiser_id"]
                logger.info("Advertiser ID encontrado: %s", adv_id)
                return adv_id

            logger.warning("Resposta 200, mas sem advertiser_id: %s", dados)
            return None

        elif resp.status_code == 404:
            logger.warning(
                "HTTP 404 — Módulo de Ads não encontrado para esta conta. "
                "Verifique se o módulo está ativado no painel do ML."
            )
            return None
        else:
            logger.warning(
                "Erro ao consultar advertisers (HTTP %d): %s",
                resp.status_code, resp.text
            )
            return None

    except requests.exceptions.RequestException as exc:
        logger.warning("Falha de rede ao consultar advertisers: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Step 2: List campaigns
# ---------------------------------------------------------------------------

def _listar_campanhas(
    access_token: str, advertiser_id: int
) -> list[dict[str, Any]]:
    """Lists all Product Ads campaigns for the seller.

    Returns:
        List of dictionaries with campaign data.
    """
    url = f"{_ADS_BASE_URL}/advertisers/{advertiser_id}/product_ads/campaigns"
    headers = _ads_headers(access_token)

    try:
        resp = requests.get(url, headers=headers, timeout=_REQUEST_TIMEOUT)

        if resp.status_code == 200:
            dados = resp.json()
            campanhas = dados.get("results", dados) if isinstance(dados, dict) else dados
            if isinstance(campanhas, list):
                logger.info("%d campanha(s) encontrada(s).", len(campanhas))
                return campanhas
            return []
        else:
            logger.warning(
                "Erro ao listar campanhas (HTTP %d): %s",
                resp.status_code, resp.text
            )
            return []

    except requests.exceptions.RequestException as exc:
        logger.warning("Falha de rede ao listar campanhas: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Step 3: Fetch metrics
# ---------------------------------------------------------------------------

def _buscar_metricas_advertiser(
    access_token: str,
    advertiser_id: int,
    data_inicio: str,
    data_fim: str,
) -> list[dict[str, Any]]:
    """Fetches global daily metrics for the advertiser (all campaigns).

    Uses aggregation_type=daily for per-day granularity.

    Returns:
        List of dictionaries with daily metrics.
    """
    url = f"{_ADS_BASE_URL}/metrics"
    headers = _ads_headers(access_token)
    params = {
        "advertiser_id": advertiser_id,
        "date_from": data_inicio,
        "date_to": data_fim,
        "aggregation_type": "daily",
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=_REQUEST_TIMEOUT)

        if resp.status_code == 200:
            dados = resp.json()
            # Response may be {results: [...]} or a direct list
            if isinstance(dados, dict):
                return dados.get("results", [dados])
            elif isinstance(dados, list):
                return dados
            return []
        else:
            logger.warning(
                "Erro ao buscar métricas globais (HTTP %d): %s",
                resp.status_code, resp.text
            )
            return []

    except requests.exceptions.RequestException as exc:
        logger.warning("Falha de rede ao buscar métricas: %s", exc)
        return []

def _buscar_metricas_campanha(
    access_token: str,
    campaign_id: str,
    data_inicio: str,
    data_fim: str,
    advertiser_id: int,
    user_id: int,
) -> list[dict[str, Any]]:
    """Probes multiple metric endpoint variants for a specific campaign.

    The API architecture varies between accounts. Tests 4 known
    route patterns and returns the first one responding with HTTP 200.

    Returns:
        List of metric dictionaries, or empty list.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Api-Version": "1",
    }

    urls_para_testar = [
        # 1. New architecture (with product_ads and advertiser_id)
        f"https://api.mercadolibre.com/advertising/product_ads/campaigns/{campaign_id}/metrics?advertiser_id={advertiser_id}&date_from={data_inicio}&date_to={data_fim}&aggregation_type=daily&group_by=date",
        # 2. New architecture (with product_ads and user_id)
        f"https://api.mercadolibre.com/advertising/product_ads/campaigns/{campaign_id}/metrics?user_id={user_id}&date_from={data_inicio}&date_to={data_fim}&aggregation_type=daily&group_by=date",
        # 3. Unified architecture (without product_ads, with advertiser_id)
        f"https://api.mercadolibre.com/advertising/campaigns/{campaign_id}/metrics?advertiser_id={advertiser_id}&date_from={data_inicio}&date_to={data_fim}&aggregation_type=daily&group_by=date",
        # 4. Unified architecture (without product_ads, with user_id)
        f"https://api.mercadolibre.com/advertising/campaigns/{campaign_id}/metrics?user_id={user_id}&date_from={data_inicio}&date_to={data_fim}&aggregation_type=daily&group_by=date",
    ]

    for url in urls_para_testar:
        try:
            resp = requests.get(url, headers=headers, timeout=_REQUEST_TIMEOUT)
            if resp.status_code == 200:
                dados = resp.json()
                # Normalize to list
                if isinstance(dados, dict):
                    return dados.get("results", [dados])
                elif isinstance(dados, list):
                    return dados
                return [dados] if dados else []
        except Exception:
            continue

    return []


# ---------------------------------------------------------------------------
# Job orchestrator
# ---------------------------------------------------------------------------

def atualizar_modulo_ads(
    dias_retroativos: int = 30,
    data_inicio_str: Optional[str] = None,
    data_fim_str: Optional[str] = None,
) -> None:
    """Extracts daily Ads costs and saves to the fato_custos_ads table.

    Workflow:
      1. Obtains token via MercadoLivreClient
      2. Resolves advertiser_id via /advertising/advertisers?product_id=PADS
      3. Attempts global metrics (consolidated per day)
      4. If available, also fetches per-campaign metrics
      5. Inserts/updates via staging table

    Args:
        dias_retroativos: Number of days to look back.
        data_inicio_str: Explicit start date (YYYY-MM-DD). Overrides dias_retroativos.
        data_fim_str: Explicit end date (YYYY-MM-DD). Overrides dias_retroativos.
    """
    print("\n=========================================")
    print("   Módulo Extração - Mercado Ads API   ")
    print("=========================================\n")

    try:
        # --- 1. Initial authentication ---
        print("1. Validando Token...")
        cliente_ml = MercadoLivreClient()
        access_token, user_id = cliente_ml.obter_token_acesso()

        if data_inicio_str and data_fim_str:
            data_inicio = datetime.datetime.strptime(data_inicio_str, "%Y-%m-%d").date()
            hoje = datetime.datetime.strptime(data_fim_str, "%Y-%m-%d").date()
            str_inicio = data_inicio_str
            str_fim = data_fim_str
        else:
            hoje = datetime.date.today()
            data_inicio = hoje - datetime.timedelta(days=dias_retroativos)
            str_inicio = data_inicio.strftime("%Y-%m-%d")
            str_fim = hoje.strftime("%Y-%m-%d")

        # --- 2. Obter advertiser_id ---
        print("2. Consultando advertiser_id para Product Ads (PADS)...")
        advertiser_id = _obter_advertiser_id(access_token)

        if advertiser_id is None:
            print()
            print("=" * 60)
            print("  ⚠️  MÓDULO DE ADS NÃO ATIVADO NESTA CONTA")
            print("=" * 60)
            print()
            print("  A API retornou que não há um 'advertiser_id' vinculado")
            print("  à sua conta. Isso pode ocorrer por:")
            print()
            print("  1. O módulo de Publicidade (Mercado Ads) nunca foi")
            print("     ativado na sua conta. Ative em:")
            print("     → mercadolivre.com.br > Minha conta > Publicidade")
            print()
            print("  2. Sua conta ainda não atende os requisitos mínimos:")
            print("     → Reputação verde (ou superior)")
            print("     → Mínimo de 10 vendas concluídas")
            print("     → Sem faturas em aberto no Mercado Livre")
            print()
            print("  3. Termos e Condições do Mercado Ads pendentes.")
            print("     Acesse o painel de Publicidade e aceite os termos.")
            print()
            print("  O pipeline continuará normalmente sem dados de Ads.\n")
            return

        print(f"   Advertiser ID: {advertiser_id}")

      # --- 3. Fetch metrics with date chunking + batch save ---
        _CHUNK_DIAS = ADS_CHUNK_DAYS
        _BATCH_SIZE = ADS_BATCH_SIZE

        # Generate day-by-day date blocks
        chunks: list[tuple[str, str]] = []
        cursor = data_inicio
        while cursor <= hoje:  # <= INCLUI O DIA DE HOJE
            chunk_fim = cursor  # INÍCIO E FIM SÃO O MESMO DIA
            chunks.append((
                cursor.strftime("%Y-%m-%d"),
                chunk_fim.strftime("%Y-%m-%d"),
            ))
            cursor = cursor + datetime.timedelta(days=1)

        total_chunks = len(chunks)
        print(f"3. Extraindo métricas de Ads de {str_inicio} até {str_fim}...")
        print(f"   Período dividido em {total_chunks} bloco(s) de {_CHUNK_DIAS} dia(s).")
        print(f"   Salvamento no banco a cada {_BATCH_SIZE} blocos.\n")

        dados_ads: list[dict] = []
        debug_impresso = False
        total_salvos = 0

        # Initialize engine and list campaigns once
        engine = conectar_mysql()
        campanhas = _listar_campanhas(access_token, advertiser_id)
        if campanhas:
            print(f"   {len(campanhas)} campanha(s) encontrada(s).")

        def _salvar_lote_no_banco(dados: list, eng) -> int:
            """Saves a batch of metrics to MySQL via staging table."""
            if not dados:
                return 0

            df = pd.DataFrame(dados)
            df = df[df["data_metrica"].astype(str).str.strip() != ""]

            if df.empty:
                return 0

            with eng.begin() as conn:
                df.to_sql("stg_ads", con=conn, if_exists="replace", index=False)
                conn.execute(text("""
                    INSERT INTO fato_custos_ads
                        (data_metrica, id_campanha, nome_campanha,
                         impressoes, cliques, custo, receita)
                    SELECT data_metrica, id_campanha, nome_campanha,
                           impressoes, cliques, custo, receita
                    FROM stg_ads
                    ON DUPLICATE KEY UPDATE
                        nome_campanha = VALUES(nome_campanha),
                        impressoes = VALUES(impressoes),
                        cliques = VALUES(cliques),
                        custo = VALUES(custo),
                        receita = VALUES(receita);
                """))
                conn.execute(text("DROP TABLE IF EXISTS stg_ads;"))

            return len(df)

        for idx, (chunk_inicio, chunk_fim) in enumerate(chunks, start=1):
            # Renew token per chunk to prevent expiration on long runs
            access_token, user_id = cliente_ml.obter_token_acesso()

            print(f"   [{idx}/{total_chunks}] Bloco: {chunk_inicio} → {chunk_fim}...", end=" ")
            registros_bloco = 0

            # Strategy A: Per-campaign metrics
            if campanhas:
                for camp in campanhas:
                    camp_id = str(camp.get("id", ""))
                    camp_nome = camp.get("name", "Campanha Sem Nome")

                    metricas_brutas = _buscar_metricas_campanha(
                        access_token, camp_id, chunk_inicio, chunk_fim,
                        advertiser_id, user_id
                    )

                    # Extract metrics list dynamically
                    lista_metricas = []
                    if isinstance(metricas_brutas, list):
                        lista_metricas = metricas_brutas
                    elif isinstance(metricas_brutas, dict):
                        lista_metricas = metricas_brutas.get(
                            "metrics", metricas_brutas.get("results", [])
                        )

                    # Debug: print raw format on first occurrence
                    if lista_metricas and not debug_impresso:
                        print(f"\n   [DEBUG] Formato bruto da 1ª métrica: {lista_metricas[0]}")
                        debug_impresso = True

                    for metrica in lista_metricas:
                        data_metrica = metrica.get("date") or metrica.get("day") or chunk_fim

                        dados_ads.append({
                            "data_metrica": data_metrica,
                            "id_campanha": camp_id,
                            "nome_campanha": camp_nome,
                            "impressoes": metrica.get("impressions", metrica.get("prints", 0)),
                            "cliques": metrica.get("clicks", 0),
                            "custo": metrica.get("cost", metrica.get("consumed_budget", 0.0)),
                            "receita": metrica.get("amount_total", metrica.get("amount", metrica.get("gmv", 0.0))),
                        })
                        registros_bloco += 1

            # Strategy B: Consolidated metrics (fallback if no per-campaign data)
            if registros_bloco == 0:
                metricas_globais = _buscar_metricas_advertiser(
                    access_token, advertiser_id, chunk_inicio, chunk_fim
                )

                lista_global = []
                if isinstance(metricas_globais, list):
                    lista_global = metricas_globais
                elif isinstance(metricas_globais, dict):
                    lista_global = metricas_globais.get(
                        "metrics", metricas_globais.get("results", [])
                    )

                for metrica in lista_global:
                    data_metrica = metrica.get("date") or metrica.get("day") or chunk_fim

                    dados_ads.append({
                        "data_metrica": data_metrica,
                        "id_campanha": "CONSOLIDADO",
                        "nome_campanha": "Custos Gerais da Conta",
                        "impressoes": metrica.get("impressions", metrica.get("prints", 0)),
                        "cliques": metrica.get("clicks", 0),
                        "custo": metrica.get("cost", metrica.get("consumed_budget", 0.0)),
                        "receita": metrica.get("amount_total", metrica.get("amount", metrica.get("gmv", 0.0))),
                    })
                    registros_bloco += 1

            if registros_bloco > 0:
                print(f"{registros_bloco} registro(s) ✓")
            else:
                print("vazio (provavelmente além da retenção)")

            # --- Batch save: persist every _BATCH_SIZE chunks ---
            if idx % _BATCH_SIZE == 0 and dados_ads:
                salvos = _salvar_lote_no_banco(dados_ads, engine)
                total_salvos += salvos
                dados_ads.clear()
                print(f"   💾 Lote de {_BATCH_SIZE} dias salvo no banco ({salvos} registros, {total_salvos} total).")

        # --- Flush: persist remaining data after loop ---
        if dados_ads:
            salvos = _salvar_lote_no_banco(dados_ads, engine)
            total_salvos += salvos
            dados_ads.clear()
            print(f"   💾 Lote final salvo no banco ({salvos} registros).")

        # --- 4. Results ---
        if total_salvos > 0:
            print(f"\n✅ SUCESSO! {total_salvos} registros de Ads atualizados no banco de dados.\n")
        else:
            print("   -> Nenhuma métrica retornada para este período.")
            print("   -> Pode significar que não houve gastos com Ads nestes dias.")
            print("   -> O pipeline continuará normalmente.\n")

    except Exception as e:
        # Attempt partial data save before reporting the error
        if dados_ads:
            try:
                eng = conectar_mysql()
                salvos = _salvar_lote_no_banco(dados_ads, eng)
                print(f"   💾 Salvamento de emergência: {salvos} registros salvos antes do erro.")
                dados_ads.clear()
            except Exception:
                pass

        logger.error("Erro no módulo de Ads: %s", e, exc_info=True)
        print(f"❌ Módulo Ads encontrou um erro: {e}")
        print("   O pipeline continuará normalmente sem dados de Ads.\n")


# ---------------------------------------------------------------------------
# Entry point (standalone execution)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    escolha = input(
        "Quantos dias de custos do Mercado Ads? (ENTER para 30 dias): "
    )
    dias = int(escolha.strip()) if escolha.strip().isdigit() else 30
    atualizar_modulo_ads(dias)