from dotenv import load_dotenv
import os
from src.extract.shopee_client import ShopeeClient

# 1. Carrega as credenciais da loja que você quer testar primeiro
# Mude para '.env.prohair' quando for testar a outra loja
load_dotenv('.env.prohair') 

print("=========================================")
print("  GERADOR DE LINK DE ACESSO - SHOPEE     ")
print("=========================================")

try:
    cliente = ShopeeClient()
    url = cliente.gerar_url_autenticacao()
    print("\n✅ Link gerado com sucesso! Clique abaixo para autorizar:")
    print(f"\n{url}\n")
    print("⚠️ ATENÇÃO: Após autorizar, olhe para a barra de endereços do navegador.")
    print("Copie os valores de 'code' e 'shop_id' e salve no bloco de notas.")
except Exception as e:
    print(f"\n❌ Erro: {e}")