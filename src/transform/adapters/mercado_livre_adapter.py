from typing import Any, Dict, List, Optional

from .base_adapter import BaseMarketplaceAdapter


def _truncar(valor: Optional[str], limite: int) -> str:
    """Truncates a string to fit a VARCHAR column safely."""
    if not valor:
        return ""
    return str(valor)[:limite]

class MercadoLivreAdapter(BaseMarketplaceAdapter):
    """
    Adapter implementation for Mercado Livre API data.
    """

    def padronizar_clientes(self) -> List[Dict[str, Any]]:
        clientes = []
        for pedido in self.raw_data:
            comprador = pedido.get("buyer") or {}
            id_cliente = int(comprador.get("id")) if comprador.get("id") else 0
            
            clientes.append({
                "id_cliente": id_cliente,
                "nickname": _truncar(str(comprador.get("nickname") or "CLIENTE NÃO INFORMADO"), 100),
                "nome_completo": ""
            })
        return clientes

    def padronizar_pedidos(self) -> List[Dict[str, Any]]:
        pedidos = []
        for pedido in self.raw_data:
            id_pedido = str(pedido.get("id", ""))
            
            comprador = pedido.get("buyer") or {}
            id_cliente = int(comprador.get("id")) if comprador.get("id") else 0
            
            pedidos.append({
                "id_pedido": id_pedido,
                "id_canal": self.id_canal,
                "id_cliente": id_cliente,
                "data_criacao": pedido.get("date_created", ""),
                "status": pedido.get("status", ""),
                "valor_produtos": float(pedido.get("total_amount") or 0.0),
                "total_pago_comprador": float(pedido.get("paid_amount") or 0.0),
                "origem_venda": "MERCADO LIVRE"
            })
            
        return pedidos

    def padronizar_itens(self) -> List[Dict[str, Any]]:
        itens = []
        for pedido in self.raw_data:
            id_pedido = str(pedido.get("id", ""))
            for item in pedido.get("order_items", []):
                produto = item.get("item", {})
                itens.append({
                    "id_pedido": id_pedido,
                    "id_anuncio": str(produto.get("id", "")),
                    "quantidade": int(item.get("quantity") or 1),
                    "preco_unitario": float(item.get("unit_price") or 0.0)
                })
        return itens

    def padronizar_transacoes(self) -> List[Dict[str, Any]]:
        transacoes = []
        for pedido in self.raw_data:
            id_pedido = str(pedido.get("id", ""))
            
            # Formatação de data transação (apenas AAAA-MM-DD extraído do ISO)
            data_str = pedido.get("date_created", "")
            data_transacao = data_str[:10] if data_str else ""
            
            taxas = {}
            
            # --- 1. Comissão de Venda (Extraída dos Itens) ---
            comissao = 0.0
            for item in pedido.get("order_items", []):
                comissao += float(item.get("sale_fee") or 0.0)
                
            if comissao != 0.0:
                taxas["COMISSAO"] = comissao

            # --- 2. Custo de Envio e Fee Details (Multi-get) ---
            frete_financeiro = 0.0
            outras_taxas = {}
            
            for tarifa in pedido.get("fee_details", []):
                tipo = tarifa.get("type", "")
                amt = float(tarifa.get("amount") or 0.0)
                
                if tipo in ("shipping_fee", "shipping_cost"):
                    frete_financeiro += amt
                else:
                    cat_nome = tipo.upper() if tipo else "OUTROS"
                    outras_taxas[cat_nome] = outras_taxas.get(cat_nome, 0.0) + amt

            # Aplica MAX entre fee_details e envio separado
            frete_multiget = float(pedido.get("custo_frete_real") or 0.0)
            frete_final = max(frete_financeiro, frete_multiget)
            
            if frete_final != 0.0:
                taxas["FRETE"] = frete_final
                
            for k, v in outras_taxas.items():
                taxas[k] = v

            # --- UNPIVOT ---
            for categoria, valor in taxas.items():
                if valor != 0.0:
                    transacoes.append({
                        "id_transacao": f"ML-{id_pedido}-{categoria}",
                        "id_pedido": id_pedido,
                        "id_canal": self.id_canal,
                        "data_transacao": data_transacao,
                        "categoria_custo": categoria,
                        "valor": valor
                    })
                    
        return transacoes

    def padronizar_anuncios(self) -> List[Dict[str, Any]]:
        anuncios = []
        for pedido in self.raw_data:
            for item in pedido.get("order_items", []):
                produto = item.get("item", {})
                anuncios.append({
                    "id_anuncio": str(produto.get("id", "")),
                    "id_canal": self.id_canal,
                    "sku": _truncar(produto.get("seller_sku", ""), 100),
                    "titulo_anuncio": _truncar(produto.get("title", ""), 255),
                    "tipo_anuncio": _truncar(produto.get("listing_type_id", ""), 50)
                })
        return anuncios