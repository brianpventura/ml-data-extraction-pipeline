"""
tests.test_adapters
~~~~~~~~~~~~~~~~~~~
Unit tests for the marketplace adapters.

Run with:: pytest tests/

These tests use synthetic raw payloads — no network, no DB. They are
the safety net we want to grow the most: every time a new marketplace
is integrated, copying one of these test files is the cheapest way to
prevent regressions on the existing channels.
"""

from __future__ import annotations

# The settings module reads the tenant .env at import time. For the
# unit tests we want isolation from any real configuration, so we
# stub the initialization flag before the adapters' transitive imports
# touch settings.
import sys
import types

if "src.config.settings" not in sys.modules:
    fake = types.ModuleType("src.config.settings")
    fake._inicializado = True
    fake.SHOPEE_PARTNER_ID = "0"
    fake.SHOPEE_PARTNER_KEY = "x"
    fake.APP_ID = ""
    fake.CLIENT_SECRET = ""
    fake.REDIRECT_URI = ""
    fake.AUTHORIZATION_CODE = ""
    fake.SHOPEE_REDIRECT_URI = ""
    fake.DB_HOST = fake.DB_PORT = fake.DB_USER = fake.DB_PASS = fake.DB_NAME = ""
    fake.API_BASE_URL = "https://api.example.com"
    fake.ADS_BASE_URL = ""
    fake.BILLING_BASE_URL = ""
    fake.REQUEST_TIMEOUT = 5
    fake.SHIPPING_TIMEOUT = 5
    fake.MAX_TOKEN_RETRIES = 1
    fake.MAX_NETWORK_RETRIES = 1
    fake.RETRY_DELAY_SECONDS = 0
    fake.THROTTLE_DELAY_SECONDS = 0
    fake.RATE_LIMIT_BACKOFF_SECONDS = 0
    fake.BILLING_GROUPS = []
    fake.ADS_CHUNK_DAYS = 1
    fake.ADS_BATCH_SIZE = 30
    sys.modules["src.config.settings"] = fake

import pandas as pd  # noqa: E402  (after settings stub)
import pytest  # noqa: E402

from src.config.utils import normalizar_sku, truncar  # noqa: E402
from src.transform.adapters.mercado_livre_adapter import (  # noqa: E402
    MercadoLivreAdapter,
)
from src.transform.adapters.shopee_adapter import ShopeeAdapter  # noqa: E402
from src.transform.data_processor import (  # noqa: E402
    consolidar_dim_produto,
    processar_pedidos,
)


# ===========================================================================
# Helpers
# ===========================================================================

@pytest.fixture
def ml_pedido() -> dict:
    """Synthetic Mercado Livre order — kept small but realistic."""
    return {
        "id": "12345",
        "date_created": "2026-04-20T10:00:00.000-00:00",
        "status": "paid",
        "total_amount": 200.0,
        "paid_amount": 220.0,
        "buyer": {"id": 999, "nickname": "fulano"},
        "order_items": [{
            "item": {
                "id": "MLB1",
                "seller_sku": "1234.0",
                "title": "Produto A",
                "listing_type_id": "gold_special",
            },
            "quantity": 2,
            "unit_price": 100.0,
            "sale_fee": 5.0,
        }],
        "fee_details": [{"type": "shipping_fee", "amount": 15.0}],
        "custo_frete_real": 18.0,
    }


@pytest.fixture
def shopee_pedido() -> dict:
    """Synthetic Shopee order with escrow + buyer_payment_info."""
    return {
        "order_sn": "SHP1",
        "create_time": 1714000000,
        "order_status": "COMPLETED",
        "total_amount": "50.00",
        "buyer_user_id": 5,
        "buyer_username": "beltrano",
        "item_list": [{
            "item_id": 7,
            "item_sku": "777",
            "item_name": "Produto B",
            "model_quantity_purchased": 1,
            "model_discounted_price": 50.0,
        }],
        "escrow_detail": {
            "order_income": {
                "commission_fee": 4.0,
                "service_fee": 1.0,
                "actual_shipping_fee": 7.0,
                "fbs_fee": 0.0,
                "seller_transaction_fee": 0.5,
            },
            "buyer_payment_info": {"buyer_total_amount": 60.0},
        },
    }


# ===========================================================================
# normalizar_sku — regression test for the silent .0 bug
# ===========================================================================

class TestNormalizarSku:
    def test_remove_excel_dot_zero_suffix(self):
        s = pd.Series(["1010003.0", "999.0"])
        assert normalizar_sku(s).tolist() == ["1010003", "999"]

    def test_strips_whitespace(self):
        s = pd.Series(["  ABC  ", "\tXYZ\n"])
        assert normalizar_sku(s).tolist() == ["ABC", "XYZ"]

    def test_keeps_alphanumeric_skus(self):
        s = pd.Series(["AB-123", "PROD.5.X"])
        assert normalizar_sku(s).tolist() == ["AB-123", "PROD.5.X"]


# ===========================================================================
# truncar
# ===========================================================================

class TestTruncar:
    def test_truncates_to_limit(self):
        assert truncar("abcdefgh", 3) == "abc"

    def test_handles_none(self):
        assert truncar(None, 5) == ""

    def test_passthrough_when_under_limit(self):
        assert truncar("ab", 5) == "ab"

    def test_casts_non_string(self):
        assert truncar(12345, 3) == "123"


# ===========================================================================
# MercadoLivreAdapter
# ===========================================================================

class TestMercadoLivreAdapter:
    def test_padronizar_pedidos(self, ml_pedido):
        adapter = MercadoLivreAdapter([ml_pedido], id_canal=1)
        out = adapter.padronizar_pedidos()
        assert len(out) == 1
        p = out[0]
        assert p["id_pedido"] == "12345"
        assert p["id_canal"] == 1
        assert p["valor_produtos"] == 200.0
        assert p["origem_venda"] == "MERCADO LIVRE"

    def test_padronizar_itens(self, ml_pedido):
        adapter = MercadoLivreAdapter([ml_pedido], id_canal=1)
        out = adapter.padronizar_itens()
        assert out == [{
            "id_pedido": "12345",
            "id_anuncio": "MLB1",
            "quantidade": 2,
            "preco_unitario": 100.0,
        }]

    def test_padronizar_transacoes_takes_max_frete(self, ml_pedido):
        # frete_financeiro = 15.0 (fee_details), frete_multiget = 18.0
        # The adapter must keep MAX(15, 18) = 18.0
        adapter = MercadoLivreAdapter([ml_pedido], id_canal=1)
        out = adapter.padronizar_transacoes()
        cats = {r["categoria_custo"]: r["valor"] for r in out}
        assert cats["FRETE"] == 18.0
        assert cats["COMISSAO"] == 5.0
        assert all(r["id_canal"] == 1 for r in out)
        assert all(r["id_transacao"].startswith("ML-12345-") for r in out)

    def test_padronizar_clientes(self, ml_pedido):
        adapter = MercadoLivreAdapter([ml_pedido], id_canal=1)
        out = adapter.padronizar_clientes()
        assert out[0]["id_cliente"] == 999
        assert out[0]["nickname"] == "fulano"

    def test_handles_empty_input(self):
        adapter = MercadoLivreAdapter([], id_canal=1)
        assert adapter.padronizar_pedidos() == []
        assert adapter.padronizar_transacoes() == []


# ===========================================================================
# ShopeeAdapter
# ===========================================================================

class TestShopeeAdapter:
    def test_padronizar_pedidos(self, shopee_pedido):
        adapter = ShopeeAdapter([shopee_pedido], id_canal=2)
        out = adapter.padronizar_pedidos()
        assert len(out) == 1
        p = out[0]
        assert p["id_pedido"] == "SHP1"
        assert p["id_canal"] == 2
        assert p["valor_produtos"] == 50.0
        assert p["total_pago_comprador"] == 60.0  # from buyer_payment_info
        assert p["origem_venda"] == "SHOPEE"

    def test_padronizar_transacoes_unpivots_fees(self, shopee_pedido):
        adapter = ShopeeAdapter([shopee_pedido], id_canal=2)
        out = adapter.padronizar_transacoes()
        cats = {r["categoria_custo"]: r["valor"] for r in out}
        # fbs_fee == 0.0 must be filtered out (no zero rows)
        assert "CUSTO_FULL" not in cats
        assert cats["COMISSAO"] == 4.0
        assert cats["TAXA_SERVICO"] == 1.0
        assert cats["FRETE"] == 7.0
        assert cats["TAXA_TRANSACAO"] == 0.5
        assert all(r["id_canal"] == 2 for r in out)
        assert all(r["id_transacao"].startswith("SHP-SHP1-") for r in out)

    def test_handles_missing_escrow(self):
        # Order with no escrow yet should not crash
        pedido = {
            "order_sn": "X1", "create_time": 1714000000,
            "order_status": "PENDING", "total_amount": "10",
            "buyer_user_id": 1, "buyer_username": "u",
            "item_list": [],
        }
        adapter = ShopeeAdapter([pedido], id_canal=2)
        # Empty item_list -> valor_produtos = 0
        assert adapter.padronizar_pedidos()[0]["valor_produtos"] == 0
        assert adapter.padronizar_transacoes() == []


# ===========================================================================
# data_processor.processar_pedidos — generic entry point
# ===========================================================================

class TestProcessarPedidos:
    def test_empty_input_returns_empty_dataframes(self):
        out = processar_pedidos([], MercadoLivreAdapter, 1)
        assert set(out.keys()) == {
            "df_fato_pedido", "df_fato_itens", "df_fato_transacoes",
            "df_dim_anuncios", "df_dim_cliente",
        }
        for df in out.values():
            assert df.empty

    def test_full_ml_flow(self, ml_pedido):
        out = processar_pedidos([ml_pedido], MercadoLivreAdapter, 1, "ML")
        assert len(out["df_fato_pedido"]) == 1
        assert len(out["df_fato_itens"]) == 1
        assert len(out["df_dim_cliente"]) == 1

    def test_aggregates_duplicate_items(self, ml_pedido):
        # Two orders with the same item: must be grouped by (id_pedido, id_anuncio)
        ml_pedido2 = dict(ml_pedido, id="99999")
        out = processar_pedidos(
            [ml_pedido, ml_pedido2], MercadoLivreAdapter, 1, "ML"
        )
        # Distinct (id_pedido, id_anuncio) pairs:
        assert len(out["df_fato_itens"]) == 2


# ===========================================================================
# consolidar_dim_produto
# ===========================================================================

class TestConsolidarDimProduto:
    def test_dedupe_skus_across_channels(self):
        ml = pd.DataFrame([{
            "id_anuncio": "MLB1", "id_canal": 1, "sku": "1234",
            "titulo_anuncio": "Produto A no ML", "tipo_anuncio": "gold",
        }])
        sp = pd.DataFrame([{
            "id_anuncio": "SHP1", "id_canal": 2, "sku": "1234",
            "titulo_anuncio": "Produto A na Shopee", "tipo_anuncio": "",
        }, {
            "id_anuncio": "SHP2", "id_canal": 2, "sku": "5678",
            "titulo_anuncio": "Produto B", "tipo_anuncio": "",
        }])
        out = consolidar_dim_produto([ml, sp])
        # 2 unique SKUs across the two channels
        assert sorted(out["sku"].tolist()) == ["1234", "5678"]
        assert "custo_unitario" in out.columns

    def test_normalizes_excel_sku(self):
        ml = pd.DataFrame([{
            "id_anuncio": "X", "id_canal": 1, "sku": "999.0",
            "titulo_anuncio": "Item", "tipo_anuncio": "",
        }])
        out = consolidar_dim_produto([ml])
        assert out["sku"].iloc[0] == "999"

    def test_drops_empty_sku(self):
        ml = pd.DataFrame([
            {"id_anuncio": "A", "id_canal": 1, "sku": "",
             "titulo_anuncio": "x", "tipo_anuncio": ""},
            {"id_anuncio": "B", "id_canal": 1, "sku": "abc",
             "titulo_anuncio": "y", "tipo_anuncio": ""},
        ])
        out = consolidar_dim_produto([ml])
        assert out["sku"].tolist() == ["abc"]

    def test_handles_no_input(self):
        assert consolidar_dim_produto([]).empty
