# Arquitetura Multi-Channel do Pipeline ETL

## 1. Visão Geral
Este projeto é um Pipeline de ETL (Extract, Transform, Load) Multi-Channel focado em extrair dados massivos e granulares de comércio eletrônico (pedidos, custos de campanhas ads, tarifas financeiras) provenientes de gateways de e-commerce e processá-los para Inteligência de Negócio (BI).

Nossa arquitetura prioriza abstração de negócios rígida. Ao invés de uma infinidade de for-loops por toda a central, o processamento ocorre via **Adapter Pattern**. Todos os marketplaces distintos obedecem a uma única padronização transacional para injetar os dados num formato otimizado "Unpivoted" (ideal para relatórios financeiros dinâmicos).

---

## 2. Arquitetura de Dados (Database Schema)

Adotamos a implementação de modelo estrutural **Star Schema Unpivoted**. Neste modelo, a tabela `fato_pedido` atua estritamente como um cabeçalho relacional. Taxas financeiras de vendas e transigências foram extraídas, organizadas em linhas verticais (UNPIVOT) e direcionadas à `fato_transacoes_financeiras`.

### Dimensões (Entities)
- **`dim_canal_venda`**: Registra os canais conectados `(Ex: 1=Mercado Livre, 2=Shopee)`.
- **`dim_cliente`**: Centralização de dados globais dos compradores.
- **`dim_produto`**: Centralização local dos SKUs, descrições base e custo da planilha primária.
- **`dim_anuncio_marketplace`**: Mapeia anúncios publicamente listados por plataforma conectando-os internamente ao SKU.

### Fatos (Transactions)
- **`fato_pedido`**: Tabela cabeçalho. Detém os totais contábeis e status do pedido, porém **exclui** campos extensos/horizontais de taxas do canal.
- **`fato_itens_pedido`**: Granularidade item a item. Mapeada ao `id_anuncio`.
- **`fato_transacoes_financeiras`**: Representante do coração relacional do banco. Todas as taxas (Comissão, Frete, Serviços, Full) caem aqui num modelo simplificado contendo: `data`, `id_canal`, `id_pedido`, `categoria_custo` e `valor`.
- **`fato_custos_ads`**: Absorve o extrato operacional de Custo por Clique e Impressões das APIs específicas de Marketing.
- **`fato_custos_operacionais`**: Mantém custos extras mensais/avulsos independentes dos gateways.

---

## 3. Arquitetura de Software (Design Patterns)

O projeto base da camada de transformação funciona fundamentado no padrão **Adapter Pattern**. A necessidade foi enxergada para apartar a volatilidade das rotas das APIs de Marketplaces.

* `src/transform/adapters/base_adapter.py`: A nossa interface Abstrata de Contrato. Nenhuma classe interage com as tabelas de destino (DataFrames do banco) a não ser que apliquem a arquitetura de listas dicionárias exigida por este Base.
* `MercadoLivreAdapter` e `ShopeeAdapter`: Acoplam as regras sujas do processamento, conversão de Unix timestamp, Unpivot local, identificador canônico `id_canal` e consolida a compatibilidade exigida e limpa para o `data_processor.py`. 

---

## 4. Fluxo do Pipeline ETL

A Orquestração e as etapas atômicas fluem unicamente partindo do executor `main.py`:

1. **Extract**: `main` aciona a API de destino (`ShopeeClient` ou `Meli`). Toda autoria via HMAC-SHA256 ou Tokens de Acesso se encerra em src/extract/*. Uma lista bruta de JSONs cruos é levantada em memória.
2. **Transform (`data_processor.py`)**: Uma orquestração aciona as invocações `.padronizar_...` do adaptador pertencente à carga levantada. Modela os retornos preenchendo todos os 4 Data Frames base: *Pedidos, Transações, Itens, Anúncios*.
3. **Load (`database.py`)**: Sub-processo de carga com proteção ACID `with engine.begin()`. Operamos Upsert (`INSERT ON DUPLICATE KEY UPDATE`) nas referências de topo, enquanto utilizamos Deleção Radically Idempotente Baseada no `id_pedido` para limpar e atualizar instâncias da fato Itens ou Financeiras e não saturar em cascata o banco relacional.
4. **Sub-Workers Standalone**: Paralelamente, processadores engajados via `run_..._update.py` sobem na memória preenchendo os esquemas paralelos de Ads/Performance e Operacional.

---

## 5. Estrutura de Pastas

```text
├── main.py                          # Orquestrador central
├── config/
│   ├── env/                         # Arquivos .env por loja (Tenant)
│   └── json/                        # Tokens OAuth persistidos
├── src/
│   ├── config/                      # Globais de credencial e auth
│   │   ├── settings.py              # Constantes e carregamento .env
│   │   ├── tenant.py                # Resolvedor de ambiente (CLI/Input)
│   │   └── paths.py                 # Helper centralizado de caminhos
│   ├── extract/
│   │   ├── shopee_client.py         # Conector Hmac/API multi-modulo shopee
│   │   └── ...                      # Demais conexões brutas
│   ├── transform/
│   │   ├── adapters/                # ADAPTER PATTERN CORE
│   │   └── data_processor.py        # Orquestração dos adapters
│   └── load/
│       └── database.py              # Camada de banco de dados
└── ARCHITECTURE.md                  # Este documento
```

---

## 6. Guia de Expansão: Adicionando um Novo Marketplace (Ex: Amazon)

Implementamos um padrão plug-and-play. Quando entrar um novo canal, siga as etapas abaixo, as chances de impacto colateral nas tabelas antigas passam a ser 0!

**Passo 1: Banco de Dados**
1. Atualize a tabela relacional mapeada. Adicione `3 = Amazon` na sua representação ou insira na base `dim_canal_venda`.

**Passo 2: Extrato (`src/extract/amazon_client.py`)**
1. Crie o cliente para interagir com SP-API e monte funções que devolvam estritamente Listas/Dicts Python.

**Passo 3: Transformação (`src/transform/adapters/amazon_adapter.py`)**
1. Crie o arquivo baseando-se no import `from .base_adapter import BaseMarketplaceAdapter`.
2. Herde a base, e crie os 4 métodos obrigatórios: `padronizar_pedidos`, `padronizar_itens`, `padronizar_transacoes`, `padronizar_anuncios`.
3. Certifique-se que o construtor invoque internamente `self.id_canal = 3`. 
4. Lembrete: Em `padronizar_transacoes`, aplique um Unpivot (pegue dezenas de taxas devolvidas e as converta em dicionários listados com a key `"categoria_custo": tipo_taxa`).

**Passo 4: Integração de Núcleo (`src/transform/data_processor.py`)**
1. Exponha o seu Adaptador em uma função envelope como `processar_pedidos_amazon_v2(dados_brutos)` devolvendo um Dicionário de DataFrames. (Mesmo padrão visto nas V2 atuais).

**Passo 5: Orquestrador (`main.py`)**
1. Instancie sua lógica envolta nas checagens if condicional. Obtenha os 4 dataframes pelo `dict.get()`.
2. Acione e descarregue os DataFrames no banco via `salvar_no_banco()`.

Pronto! Ao final destes passos genéricos a estrutura Multi-Channel expandirá seus horizontes atestando os princípios Open-Closed (S**O**LID) aplicados localmente!
