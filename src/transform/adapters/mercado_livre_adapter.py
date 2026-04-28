"""
transform.adapters.mercado_livre_adapter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Adapter implementation for Mercado Livre API data.
"""

from typing import Any, Dict, List

from src.config.utils import truncar
from .base_adapter import BaseMarketplaceAdapter


class MercadoLivreAdapter(BaseMarketplaceAdapter):
    """Adapter implementation for Mercado Livre API data."""

    # Stable origin label written into ``fato_pedido.origem_venda``.
    origem_venda: str = "MERCADO LIVRE"
    # Prefix used to build deterministic transaction IDs.
    transacao_prefix: str = "ML"

    def padronizar_clientes(self) -> List[Dict[str, Any]]:
        clientes = []
        for pedido in self.raw_data:
            comprador = pedido.get("buyer") or {}
            id_cliente = int(comprador.get("id")) if comprador.get("id") else 0

            clientes.append({
                "id_cliente": id_cliente,
                "nickname": truncar(
                    comprador.get("nickname") or "CLIENTE NÃO INFORMADO", 100
                ),
                "nome_completo": "",
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
                "origem_venda": self.origem_venda,
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
                    "preco_unitario": float(item.get("unit_price") or 0.0),
                })
        return itens

    def padronizar_transacoes(self) -> List[Dict[str, Any]]:
        transacoes = []
        for pedido in self.raw_data:
            id_pedido = str(pedido.get("id", ""))

            data_str = pedido.get("date_created", "")
            data_transacao = data_str[:10] if data_str else ""

            taxas: Dict[str, float] = {}

            # 1. Sale commission (sum of sale_fee on items)
            comissao = sum(
                float(item.get("sale_fee") or 0.0)
                for item in pedido.get("order_items", [])
            )
            if comissao:
                taxas["COMISSAO"] = comissao

            # 2. Shipping & other fees
            frete_financeiro = 0.0
            outras_taxas: Dict[str, float] = {}

            for tarifa in pedido.get("fee_details", []):
                tipo = tarifa.get("type", "")
                amt = float(tarifa.get("amount") or 0.0)

                if tipo in ("shipping_fee", "shipping_cost"):
                    frete_financeiro += amt
                else:
                    cat_nome = tipo.upper() if tipo else "OUTROS"
                    outras_taxas[cat_nome] = outras_taxas.get(cat_nome, 0.0) + amt

            # MAX between fee_details and the multi-get shipping endpoint
            frete_multiget = float(pedido.get("custo_frete_real") or 0.0)
            frete_final = max(frete_financeiro, frete_multiget)
            if frete_final:
                taxas["FRETE"] = frete_final

            taxas.update(outras_taxas)

            # UNPIVOT
            for categoria, valor in taxas.items():
                if valor != 0.0:
                    transacoes.append({
                        "id_transacao": f"{self.transacao_prefix}-{id_pedido}-{categoria}",
                        "id_pedido": id_pedido,
                        "id_canal": self.id_canal,
                        "data_transacao": data_transacao,
                        "categoria_custo": categoria,
                        "valor": valor,
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
                    "sku": truncar(produto.get("seller_sku", ""), 100),
                    "titulo_anuncio": truncar(produto.get("title", ""), 255),
                    "tipo_anuncio": truncar(produto.get("listing_type_id", ""), 50),
                })
        return anuncios
