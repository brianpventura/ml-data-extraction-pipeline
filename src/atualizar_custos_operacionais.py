"""
atualizar_custos_operacionais — Extração de Custos Operacionais (Full / Devoluções)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Extrai custos operacionais do extrato de billing do Mercado Livre:
  - Armazenamento Full (STORAGE)
  - Coleta Full / Fulfillment (FULFILLMENT)
  - Custos de Devolução (RETURN)

A API de Billing retorna dados agrupados por período mensal. Cada resposta
contém um array 'charges' com os custos classificados por 'type'.
"""

import sys
from pathlib import Path

# Garante que a raiz do projeto esteja no sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import datetime
import time
import logging
from typing import Any, Optional

import pandas as pd
import requests

from src.extract.mercadolivre_client import MercadoLivreClient
from src.load.database import salvar_custos_operacionais

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
_BILLING_BASE_URL = "https://api.mercadolibre.com/billing/integration"
_REQUEST_TIMEOUT = 15

# Mapeamento: type da API → tipo_custo no banco de dados
_TIPO_CUSTO_MAP = {
    "STORAGE": "ARMAZENAMENTO_FULL",
    "FULFILLMENT": "COLETA_FULL",
    "RETURN": "DEVOLUCAO",
    "AFFILIATES": "CUSTO_AFILIADO",
    "PADS_AFFILIATES": "CUSTO_AFILIADO",
    "AFFILIATE_PROGRAM": "CUSTO_AFILIADO",
    "PRODUCT_ADS_AFFILIATES": "CUSTO_AFILIADO",
}

# Labels alternativos (a API pode retornar variações)
_LABEL_MAP = {
    "full storage": "ARMAZENAMENTO_FULL",
    "storage": "ARMAZENAMENTO_FULL",
    "armazenamento": "ARMAZENAMENTO_FULL",
    "almacenamiento": "ARMAZENAMENTO_FULL",
    "fulfillment": "COLETA_FULL",
    "full fulfillment": "COLETA_FULL",
    "inbound": "COLETA_FULL",
    "coleta": "COLETA_FULL",
    "return": "DEVOLUCAO",
    "devolução": "DEVOLUCAO",
    "devolucion": "DEVOLUCAO",
    "devolucao": "DEVOLUCAO",
    "afiliado": "CUSTO_AFILIADO",
    "afiliados": "CUSTO_AFILIADO",
    "affiliate": "CUSTO_AFILIADO",
    "campanha de afiliados": "CUSTO_AFILIADO",
    "porcentagem de parceria": "CUSTO_AFILIADO",
}


def _classificar_custo(tipo: str, label: str) -> Optional[str]:
    """Classifica um charge da API em um dos 3 tipos operacionais.

    Tenta primeiro pelo campo 'type', depois pelo 'label'.

    Returns:
        Tipo normalizado ou None se não for custo operacional.
    """
    # Tenta pelo type direto
    tipo_upper = tipo.upper().strip()
    if tipo_upper in _TIPO_CUSTO_MAP:
        return _TIPO_CUSTO_MAP[tipo_upper]

    # Tenta pelo label (busca parcial, case-insensitive)
    label_lower = label.lower().strip()
    for chave, valor in _LABEL_MAP.items():
        if chave in label_lower:
            return valor

    return None


def _gerar_periodos_mensais(
    data_inicio: datetime.date, data_fim: datetime.date
) -> list[str]:
    """Gera lista de períodos mensais no formato YYYY-MM-01 (Fallback)."""
    periodos = []
    cursor = data_inicio.replace(day=1)
    fim = data_fim.replace(day=1)

    while cursor <= fim:
        periodos.append(cursor.strftime("%Y-%m-%d"))
        # Avança para o primeiro dia do próximo mês
        if cursor.month == 12:
            cursor = cursor.replace(year=cursor.year + 1, month=1)
        else:
            cursor = cursor.replace(month=cursor.month + 1)

    return periodos


def _obter_periodos_validos_api(access_token: str) -> list[str]:
    """Consulta o Mercado Livre para obter as chaves de faturamento oficiais abertas."""
    url = f"{_BILLING_BASE_URL}/monthly/periods"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    periodos_unicos = set()
    
    for grupo in ["ML", "MP"]:
        params = {"group": grupo}
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=_REQUEST_TIMEOUT)
            if resp.status_code == 200:
                dados = resp.json()
                lista = dados if isinstance(dados, list) else dados.get("results", [])
                for item in lista:
                    key = item.get("period", {}).get("key")
                    if key:
                        periodos_unicos.add(key)
            else:
                print(f"   -> Aviso: Não foi possível listar os períodos do grupo {grupo} (HTTP {resp.status_code}).")
        except Exception as e:
            print(f"   -> Falha de rede ao buscar períodos do grupo {grupo}: {e}")
            
    return sorted(list(periodos_unicos))


def _buscar_summary_mensal(access_token: str, periodo: str) -> list[dict[str, Any]]:
    # Rota oficial consolidada: /periods/key/{periodo}/summary/details
    url = f"{_BILLING_BASE_URL}/periods/key/{periodo}/summary/details"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    params = {"document_type": "BILL"}
    todas_charges = []
    
    for tentativa in range(3):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=_REQUEST_TIMEOUT)
            if resp.status_code == 200:
                dados = resp.json()
                bill = dados.get("bill_includes", dados)
                charges = bill.get("charges", [])
                todas_charges.extend(charges)
                break
            elif resp.status_code == 429:
                print(f"      -> Limite da API (429) no Summary. Aguardando 12s...")
                time.sleep(12)
            else:
                print(f"      -> Aviso: Summary indisponível (HTTP {resp.status_code}). Tentando details... ({resp.text})")
                break
        except requests.exceptions.RequestException as exc:
            print(f"      -> Falha de rede: {exc}")
            break
            
    return todas_charges


def _buscar_details_mensal(access_token: str, periodo: str) -> list[dict[str, Any]]:
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    todas_charges = []
    
    for grupo in ["ML", "MP"]:
        # Rota oficial de detalhes: /periods/key/{periodo}/group/{grupo}/details
        url = f"{_BILLING_BASE_URL}/periods/key/{periodo}/group/{grupo}/details"
        params = {
            "limit": 1000,
            "document_type": "BILL"
        }
        
        for tentativa in range(3):
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=_REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    dados = resp.json()
                    if isinstance(dados, list):
                        todas_charges.extend(dados)
                    elif isinstance(dados, dict):
                        charges = dados.get("charges", dados.get("details", dados.get("results", [])))
                        todas_charges.extend(charges)
                    break
                elif resp.status_code == 429:
                    print(f"      -> Limite da API (429) no Details {grupo}. Aguardando 12s...")
                    time.sleep(12)
                else:
                    print(f"      -> Aviso: Details {grupo} indisponível (HTTP {resp.status_code}). ({resp.text})")
                    break
            except requests.exceptions.RequestException as exc:
                print(f"      -> Falha de rede: {exc}")
                break
        time.sleep(1) # Pausa amigável entre requisições de grupos diferentes

    return todas_charges


# ---------------------------------------------------------------------------
# Orquestrador
# ---------------------------------------------------------------------------

def atualizar_modulo_operacional(
    dias_retroativos: int = 30,
    data_inicio_str: Optional[str] = None,
    data_fim_str: Optional[str] = None,
) -> None:
    """Extrai custos operacionais (Full, Devoluções) do billing e salva no MySQL.

    Fluxo:
      1. Autenticação via MercadoLivreClient
      2. Gera lista de períodos mensais
      3. Para cada mês, busca summary e classifica os charges
      4. Agrega por (data, tipo_custo) e salva no banco

    Args:
        dias_retroativos: Dias para trás a buscar (default: 30).
        data_inicio_str: Data início explícita AAAA-MM-DD.
        data_fim_str: Data fim explícita AAAA-MM-DD.
    """
    print("\n=========================================")
    print("   Módulo Extração - Custos Operacionais")
    print("=========================================\n")

    try:
        # --- 1. Autenticação ---
        print("1. Validando Token...")
        cliente_ml = MercadoLivreClient()
        access_token, _ = cliente_ml.obter_token_acesso()

        # --- 2. Calcular datas ---
        if data_inicio_str and data_fim_str:
            data_inicio = datetime.datetime.strptime(data_inicio_str, "%Y-%m-%d").date()
            data_fim = datetime.datetime.strptime(data_fim_str, "%Y-%m-%d").date()
        else:
            data_fim = datetime.date.today()
            data_inicio = data_fim - datetime.timedelta(days=dias_retroativos)

        str_inicio = data_inicio.strftime("%Y-%m-%d")
        str_fim = data_fim.strftime("%Y-%m-%d")

        # --- 3. Consultar períodos oficiais ---
        print(f"2. Extraindo custos operacionais de {str_inicio} até {str_fim}...")
        print("   Consultando chaves de faturamento oficiais do Mercado Livre...")
        periodos_api = _obter_periodos_validos_api(access_token)
        
        periodos = []
        if periodos_api:
            # Filtra apenas os períodos oficiais que caem na janela solicitada
            inicio_mes = data_inicio.replace(day=1)
            fim_mes = data_fim.replace(day=1)
            for p in periodos_api:
                try:
                    p_date = datetime.datetime.strptime(p, "%Y-%m-%d").date()
                    if inicio_mes <= p_date <= fim_mes:
                        periodos.append(p)
                except ValueError:
                    pass
        else:
            print("   -> Utilizando gerador manual de datas (Fallback)...")
            periodos = _gerar_periodos_mensais(data_inicio, data_fim)
            
        print(f"   {len(periodos)} período(s) oficial(is) encontrado(s) para consulta.")

        dados_op: list[dict] = []
        debug_impresso = False

        for idx, periodo in enumerate(periodos, start=1):
            # Renova token a cada iteração para execuções longas
            access_token, _ = cliente_ml.obter_token_acesso()

            print(f"   [{idx}/{len(periodos)}] Período: {periodo}...", end=" ")

            # Tenta summary primeiro, depois details como fallback
            charges = _buscar_summary_mensal(access_token, periodo)
            fonte = "summary"

            if not charges:
                charges = _buscar_details_mensal(access_token, periodo)
                fonte = "details"

            if not charges:
                print("vazio")
                continue

            # Debug: mostra primeiro charge na primeira vez
            if not debug_impresso:
                print(f"\n   [DEBUG] Fonte: {fonte} | 1º charge: {charges[0]}")
                debug_impresso = True

            # Extrai o primeiro dia do período como data_metrica
            # (billing é mensal, mas atribuímos ao 1º dia do mês)
            periodo_date = periodo  # já é YYYY-MM-DD (primeiro dia do mês)

            registros_periodo = 0

            for charge in charges:
                tipo_raw = str(charge.get("type", ""))
                label_raw = str(charge.get("label", ""))
                amount = charge.get("amount", 0.0)

                tipo_custo = _classificar_custo(tipo_raw, label_raw)

                if tipo_custo is None:
                    continue  # Não é custo operacional (ex: comissão de venda)

                # Garante valor absoluto (positivo) para o BI
                valor = abs(float(amount)) if amount else 0.0

                dados_op.append({
                    "data_metrica": periodo_date,
                    "tipo_custo": tipo_custo,
                    "valor": valor,
                })
                registros_periodo += 1

            if registros_periodo > 0:
                print(f"{registros_periodo} custo(s) operacional(is) ✓")
            elif debug_impresso:
                print("nenhum custo operacional neste período")

        # --- 4. Agregar e salvar ---
        if not dados_op:
            print("   -> Nenhum custo operacional encontrado para este período.")
            print("   -> O pipeline continuará normalmente.\n")
            return

        df_op = pd.DataFrame(dados_op)

        # Agrega: SUM dos valores por (data_metrica, tipo_custo)
        df_op = (
            df_op
            .groupby(["data_metrica", "tipo_custo"], as_index=False)
            .agg({"valor": "sum"})
        )

        print(f"\n3. Salvando {len(df_op)} registros agregados no MySQL...")
        salvos = salvar_custos_operacionais(df_op)

        if salvos > 0:
            print(f"✅ SUCESSO! {salvos} registros de custos operacionais salvos.\n")

            # Resumo por tipo
            resumo = df_op.groupby("tipo_custo")["valor"].sum()
            print("   📊 Resumo:")
            for tipo, total in resumo.items():
                print(f"      {tipo}: R$ {total:,.2f}")
            print()
        else:
            print("   -> Nenhum dado para salvar.\n")

    except Exception as e:
        logger.error("Erro no módulo de Custos Operacionais: %s", e, exc_info=True)
        print(f"❌ Módulo Operacional encontrou um erro: {e}")
        print("   O pipeline continuará normalmente.\n")


# ---------------------------------------------------------------------------
# Entry point (execução standalone)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    escolha = input(
        "Quantos dias de custos operacionais? (ENTER para 30 dias): "
    )
    dias = int(escolha.strip()) if escolha.strip().isdigit() else 30
    atualizar_modulo_operacional(dias)