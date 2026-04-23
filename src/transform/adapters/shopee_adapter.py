from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .base_adapter import BaseMarketplaceAdapter


def _truncar(valor: Optional[str], limite: int) -> str:
    """Truncates a string to fit a VARCHAR column safely."""
    if not valor:
        return ""
    return str(valor)[:limite]

class ShopeeAdapter(BaseMarketplaceAdapter):
    """
    Adapter implementation for Shopee API json data.
    """

    def _unix_to_str(self, unix_ts: int) -> str:
        """Helper to convert unix timestamp to DATETIME string."""
        if not unix_ts:
            return ""
        return datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def _unix_to_date_str(self, unix_ts: int) -> str:
        """Helper to convert unix timestamp to DATE string."""
        if not unix_ts:
            return ""
        return datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%d")

    def padronizar_clientes(self) -> List[Dict[str, Any]]:
        clientes = []
        for pedido in self.raw_data:
            id_cliente = int(pedido.get("buyer_user_id") or 0)
            
            clientes.append({
                "id_cliente": id_cliente,
                "nickname": _truncar(str(pedido.get("buyer_username") or "CLIENTE NÃO INFORMADO"), 100),
                "nome_completo": ""
            })
        return clientes

    def padronizar_pedidos(self) -> List[Dict[str, Any]]:
        pedidos = []
        for pedido in self.raw_data:
            order_sn = str(pedido.get("order_sn", ""))
            
            # Cascata de total pago conforme exigido
            escrow = pedido.get("escrow_detail", {})
            order_income = escrow.get("order_income", {})
            buyer_info = escrow.get("buyer_payment_info", {})
            
            total_pago = float(
                buyer_info.get("buyer_total_amount") or 
                order_income.get("buyer_total_amount") or 
                pedido.get("total_amount", 0.0) or
                0.0
            )

            data_criacao = self._unix_to_str(pedido.get("create_time", 0))

            # Calcular valor dos produtos iterando sobre a lista de itens
            item_list = pedido.get("item_list", [])
            valor_produtos = sum(
                int(item.get("model_quantity_purchased") or 1) * float(item.get("model_discounted_price") or 0.0)
                for item in item_list
            )
            
            # Shopee API typically exposes 'buyer_user_id' in v2
            id_cliente = int(pedido.get("buyer_user_id") or 0)

            pedidos.append({
                "id_pedido": order_sn,
                "id_canal": self.id_canal,
                "id_cliente": id_cliente,
                "data_criacao": data_criacao,
                "status": pedido.get("order_status", ""),
                "valor_produtos": valor_produtos,
                "total_pago_comprador": total_pago,
                "origem_venda": "SHOPEE"
            })
            
        return pedidos

    def padronizar_itens(self) -> List[Dict[str, Any]]:
        itens = []
        for pedido in self.raw_data:
            order_sn = str(pedido.get("order_sn", ""))
            for item in pedido.get("item_list", []):
                itens.append({
                    "id_pedido": order_sn,
                    "id_anuncio": str(item.get("item_id", "")),
                    "quantidade": int(item.get("model_quantity_purchased") or 1),
                    "preco_unitario": float(item.get("model_discounted_price") or 0.0)
                })
        return itens

    def padronizar_transacoes(self) -> List[Dict[str, Any]]:
        transacoes = []
        for pedido in self.raw_data:
            order_sn = str(pedido.get("order_sn", ""))
            data_transacao = self._unix_to_date_str(pedido.get("create_time", 0))
            
            escrow = pedido.get("escrow_detail", {})
            order_income = escrow.get("order_income", {})

            # Mapeamento Unpivot das taxas
            taxas = {
                "COMISSAO": float(order_income.get("commission_fee") or 0.0),
                "TAXA_TRANSACAO": float(order_income.get("seller_transaction_fee") or 0.0),
                "TAXA_SERVICO": float(order_income.get("service_fee") or 0.0),
                "FRETE": float(order_income.get("actual_shipping_fee") or 0.0),
                "CUSTO_FULL": float(order_income.get("fbs_fee") or 0.0),
                "AFILIADOS": float(order_income.get("order_ams_commission_fee") or 0.0)
            }

            for categoria, valor in taxas.items():
                if valor != 0.0:
                    # Garantindo um ID de transação unívoco pseudo-aleatório reprodutível se via chave combinada,
                    # Mas gerando UUID como safe fallback preferencialmente para não haver conflitos e bater com UUID do DB
                    # Ou f"SHP-{order_sn}-{categoria}" se preferir PK determinística.
                    id_identidade = f"SHP-{order_sn}-{categoria}"
                    
                    transacoes.append({
                        "id_transacao": id_identidade,
                        "id_pedido": order_sn,
                        "id_canal": self.id_canal,
                        "data_transacao": data_transacao,
                        "categoria_custo": categoria,
                        "valor": valor
                    })
                    
        return transacoes

    def padronizar_anuncios(self) -> List[Dict[str, Any]]:
        anuncios = []
        for pedido in self.raw_data:
            for item in pedido.get("item_list", []):
                anuncios.append({
                    "id_anuncio": str(item.get("item_id", "")),
                    "id_canal": self.id_canal,
                    "sku": _truncar(item.get("item_sku", ""), 100),
                    "titulo_anuncio": _truncar(item.get("item_name", ""), 255),
                    "tipo_anuncio": ""
                })
        return anuncios
