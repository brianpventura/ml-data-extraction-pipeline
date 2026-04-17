"""
run_shopee_ads_update — Shopee Advertising Metrics Extraction Job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Extracts daily cost metrics from Shopee Ads campaigns
and persists them into the ads cost fact table.
"""

import datetime
import logging
import time
from typing import Optional
from tqdm import tqdm

import pandas as pd
from sqlalchemy import text

from src.extract.shopee_client import ShopeeClient
from src.load.database import conectar_mysql

logger = logging.getLogger(__name__)

# Configurações de chunks para evitar timeout/sobrecarga na API
_CHUNK_DAYS = 15

def atualizar_modulo_shopee_ads(
    dias_retroativos: int = 30,
    data_inicio_str: Optional[str] = None,
    data_fim_str: Optional[str] = None,
) -> None:
    """Extrai os custos diários de Ads da Shopee e salva na tabela fato_custos_ads.

    Args:
        dias_retroativos: Número de dias para buscar retroativamente.
        data_inicio_str: Data de início explícita (YYYY-MM-DD).
        data_fim_str: Data de fim explícita (YYYY-MM-DD).
    """
    logger.info("Iniciando extração de métricas de Shopee Ads...")

    try:
        cliente_shopee = ShopeeClient()
        # Garante que o token esteja válido/renovado antes de iniciar
        cliente_shopee.obter_token_acesso()
    except Exception as exc:
        logger.error("Erro ao instanciar ou autenticar ShopeeClient: %s", exc)
        return

    # 1. Determinação do range de datas
    hoje = datetime.date.today()
    if data_inicio_str and data_fim_str:
        try:
            dt_inicio = datetime.datetime.strptime(data_inicio_str, "%Y-%m-%d").date()
            dt_fim = datetime.datetime.strptime(data_fim_str, "%Y-%m-%d").date()
        except ValueError as exc:
            logger.error("Formato de data inválido. Use YYYY-MM-DD: %s", exc)
            return
    else:
        dt_fim = hoje
        dt_inicio = dt_fim - datetime.timedelta(days=dias_retroativos)

    total_dias = (dt_fim - dt_inicio).days + 1
    if total_dias <= 0:
        logger.error("Período de extração inválido: início após o fim.")
        return

    logger.info("Shopee Ads: Período de %s até %s (%d dias).", dt_inicio, dt_fim, total_dias)

    # 2. Busca das Campanhas Ativas
    try:
        campanhas = cliente_shopee.obter_campanhas_ads()
    except Exception as exc:
        logger.error("Falha fatal ao buscar campanhas Shopee: %s", exc)
        return

    if not campanhas:
        logger.info("Nenhuma campanha de Shopee Ads retornada. Encerrando módulo.")
        return

    logger.info("Shopee Ads: %d campanha(s) encontrada(s). Buscando métricas...", len(campanhas))

    # 3. Divisão do tempo em janelas menores (ex: 15 em 15 dias) para buscar métricas
    janelas = []
    data_atual = dt_fim
    while data_atual >= dt_inicio:
        inicio_janela = max(dt_inicio, data_atual - datetime.timedelta(days=_CHUNK_DAYS - 1))
        janelas.append((inicio_janela, data_atual))
        data_atual = inicio_janela - datetime.timedelta(days=1)

    janelas.reverse() # Cronológico
    
    dados_ads = []

    # 4. Extração de Métricas por Campanha e por Janela de Tempo
    pbar_campanhas = tqdm(campanhas, desc="Shopee Ads Campanhas", unit="camp", colour="red")
    for camp in pbar_campanhas:
        camp_id = str(camp.get("campaign_id", ""))
        if not camp_id:
            continue
            
        camp_nome = camp.get("campaign_name", "Campanha_Sem_Nome")
        id_campanha_formatado = f"SHP_{camp_id}"  # Prefixo obrigatório mapeado

        for janela_inicio, janela_fim in janelas:
            # Converte para string pois o cliente espera strings ou formata lá dentro
            inicio_str = janela_inicio.strftime("%Y-%m-%d")
            fim_str = janela_fim.strftime("%Y-%m-%d")
            
            # Buscar métricas de performance da campanha na janela
            metricas_diarias = cliente_shopee.obter_metricas_ads_campanha(camp_id, inicio_str, fim_str)
            
            for metrica in metricas_diarias:
                # API normalmente retorna unix time em 'timestamp' ou yyyy-mm-dd em 'date'
                data_metrica_unix = metrica.get("timestamp")
                if data_metrica_unix:
                    data_metrica = datetime.datetime.fromtimestamp(data_metrica_unix).strftime("%Y-%m-%d")
                else:
                    data_metrica = metrica.get("date", fim_str)

                # Extrai custo, cliques, views e gmv
                # Nomes das chaves podem variar um pouco, dependendo da doc exata, mas mapeio as comuns
                impressoes = int(metrica.get("impression", metrica.get("views", 0)))
                cliques = int(metrica.get("click", metrica.get("clicks", 0)))
                custo = float(metrica.get("expense", metrica.get("cost", 0.0)))
                receita = float(metrica.get("gmv", metrica.get("direct_gmv", 0.0)))
                
                dados_ads.append({
                    "data_metrica": data_metrica,
                    "id_campanha": id_campanha_formatado,
                    "nome_campanha": camp_nome,
                    "impressoes": impressoes,
                    "cliques": cliques,
                    "custo": custo,
                    "receita": receita,
                })

    pbar_campanhas.close()
    
    if not dados_ads:
        logger.info("Nenhuma métrica coletada para as campanhas da Shopee. Módulo finalizado.")
        return

    # 5. Salvar/Atualizar no Banco de Dados
    df_ads = pd.DataFrame(dados_ads)
    
    # Consolidar duplicatas (pode acontecer se sobrepor fatias)
    df_ads = df_ads.groupby(["data_metrica", "id_campanha", "nome_campanha"], as_index=False).sum()
    
    try:
        engine = conectar_mysql()
        with engine.begin() as conn:
            df_ads.to_sql(
                "stg_shopee_ads", con=conn, if_exists="replace", index=False
            )
            
            # Upsert idêntico ao run_ads_update.py
            conn.execute(text("""
                INSERT INTO fato_custos_ads 
                    (data_metrica, id_campanha, nome_campanha, 
                     impressoes, cliques, custo, receita)
                SELECT data_metrica, id_campanha, nome_campanha, 
                       impressoes, cliques, custo, receita 
                FROM stg_shopee_ads
                ON DUPLICATE KEY UPDATE 
                    nome_campanha = VALUES(nome_campanha),
                    impressoes = VALUES(impressoes),
                    cliques = VALUES(cliques),
                    custo = VALUES(custo),
                    receita = VALUES(receita);
            """))
            conn.execute(text("DROP TABLE IF EXISTS stg_shopee_ads;"))
            
        logger.info("Shopee Ads: %d registros inseridos/atualizados na fato_custos_ads.", len(df_ads))
    except Exception as exc:
        logger.error("Falha ao salvar dados de Shopee Ads no banco: %s", exc)

