import hmac
import hashlib
import time
import requests
import urllib.parse

def gerar_assinatura(partner_key, base_string):
    return hmac.new(
        partner_key.encode('utf-8'),
        base_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

def main():
    print("="*50)
    print("   AUTENTICADOR SHOPEE V2 - MULTI-TENANT")
    print("="*50)
    
    partner_id = input("1. Digite o seu Partner ID (App ID): ").strip()
    partner_key = input("2. Digite a sua Partner Key (App Secret): ").strip()
    
    # ---------------------------------------------------------
    # PASSO 1: Gerar Link de Autorização
    # ---------------------------------------------------------
    path_auth = "/api/v2/shop/auth_partner"
    timestamp = int(time.time())
    
    base_string_auth = f"{partner_id}{path_auth}{timestamp}"
    sign_auth = gerar_assinatura(partner_key, base_string_auth)
    
    redirect_url = "https://prohair.com.vc"
    
    auth_link = (
        f"https://partner.shopeemobile.com{path_auth}"
        f"?partner_id={partner_id}"
        f"&timestamp={timestamp}"
        f"&sign={sign_auth}"
        f"&redirect={redirect_url}"
    )
    
    print("\n" + "-"*50)
    print("PASSO 1: Clique no link abaixo, faça o login com a conta da PROGROWTH e autorize o App.")
    print("O navegador vai dar 'Página não encontrada' (localhost). Isso é normal!")
    print("-" * 50)
    print(f"\n{auth_link}\n")
    
    # ---------------------------------------------------------
    # PASSO 2: Extrair Código e Obter Tokens
    # ---------------------------------------------------------
    url_retorno = input("PASSO 2: Cole aqui a URL inteira que ficou no seu navegador (com o erro de localhost): ").strip()
    
    try:
        parsed_url = urllib.parse.urlparse(url_retorno)
        params = urllib.parse.parse_qs(parsed_url.query)
        
        code = params.get('code', [None])[0]
        shop_id = params.get('shop_id', [None])[0]
        
        if not code or not shop_id:
            print("\n❌ Erro: Não foi possível encontrar o 'code' ou 'shop_id' na URL colada.")
            return

        print(f"\n✅ Sucesso! Code: {code} | Shop ID: {shop_id}")
        print("Buscando Tokens definitivos na API...\n")
        
        path_token = "/api/v2/auth/token/get"
        timestamp_token = int(time.time())
        base_string_token = f"{partner_id}{path_token}{timestamp_token}"
        sign_token = gerar_assinatura(partner_key, base_string_token)
        
        url_token = f"https://partner.shopeemobile.com{path_token}?partner_id={partner_id}&timestamp={timestamp_token}&sign={sign_token}"
        
        payload = {
            "code": code,
            "shop_id": int(shop_id),
            "partner_id": int(partner_id)
        }
        
        headers = {"Content-Type": "application/json"}
        
        response = requests.post(url_token, json=payload, headers=headers)
        data = response.json()
        
        if data.get("error"):
            print(f"❌ Erro da API: {data.get('message')}")
        else:
            access_token = data.get("access_token")
            refresh_token = data.get("refresh_token")
            
            print("="*50)
            print("🎉 TOKENS GERADOS COM SUCESSO! Copie o bloco abaixo para o seu .env.progrowth")
            print("="*50)
            print(f"SHOPEE_PARTNER_ID={partner_id}")
            print(f"SHOPEE_PARTNER_KEY={partner_key}")
            print(f"SHOPEE_SHOP_ID={shop_id}")
            print(f"SHOPEE_ACCESS_TOKEN={access_token}")
            print(f"SHOPEE_REFRESH_TOKEN={refresh_token}")
            print("="*50)

    except Exception as e:
        print(f"\n❌ Ocorreu um erro no processamento: {e}")

if __name__ == "__main__":
    main()