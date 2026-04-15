# Documento Arquitetural do Sistema — ETL Multi-Tenant

> **Status:** Ativo | **Versão:** 1.0
> **Escopo:** Pipeline de Extração, Transformação e Carga (ETL) de APIs de E-commerce.

Este documento serve como o guia definitivo do sistema `data_mercadoLivre`, delineando suas responsabilidades, decisões de design e detalhes estruturais.

---

## 1. Visão Geral do Sistema (System Overview)

O sistema consiste em um Pipeline ETL construído em arquitetura **Multi-Tenant**, voltado a dados do setor de E-Commerce. Ele automatiza as rotinas de:

- **Extração** de pedidos, conversões, itens das campanhas de publicidade e faturas através de integrações via conectores de API (atualmente Mercado Livre, com arquitetura pronta para Shopee).
- **Transformação** de dados complexos aninhados e de formato JSON dinâmico para tabelas simplificadas (Star Schema) visando visualização e Inteligência de Negócio. Efetua a fusão de APIs de consumo com planilhas de custo.
- **Carga** robusta, tolerante a falhas em um banco de dados relacional MySQL formatado sob princípios de Data Warehouse (Tabelas Fato e Dimensões).

---

## 2. Stack Tecnológica (Tech Stack)

A infraestrutura foi construída sobre tecnologias padronizadas do ecossistema de dados moderno em Python:

- **Linguagem Base:** Python 3.x
- **Pacotes Essenciais:**
  - `pandas`: Processamento de regras de negócios na memória, construção dos fluxos de dimensão e limpezas.
  - `requests`: Comunicação REST API sincronizada aos endpoints dos Marketplaces.
  - `SQLAlchemy` e `PyMySQL`: Mecanismo ORM e Engine transacional relacional para conversão nativa do DataFrame ao SQL de modo eficaz.
  - `python-dotenv`: Gestão e isolamento rígido de múltiplos escopos de segredos por tenant.
- **Banco de Dados:** MySQL (atuando como repositório final Data Warehouse).

---

## 3. Arquitetura e Padrões de Projeto (Architecture & Patterns)

O projeto baseia-se numa estrutura pragmática que divide rigorosamente responsabilidades e implementa diversos de padrões sólidos (*SOLID*).

### Camadas de Responsabilidade

1. **Extract (src/extract):** Intermediam exclusivamente o contrato em HTTP ou sistema de arquivos com plataformas de terceiros.
2. **Transform (src/transform):** Realizam as mutações sistêmicas como merge, mapeamento e de duplicações de dicionários isolados do código DB ou API.
3. **Load (src/load):** A camada transacional (DDL e DML), sem processamento comercial, cuidando apenas de transições SQL brutas, integridades ACID com garantias de atomicidade usando pool de conexões.

### Padrões de Implementação (Design Patterns e Práticas)

- **Multi-Tenancy (Separação por Loja):** A execução possui injeção tardia (*lazy loading*) via `src.config.tenant` onde o argparse carrega o arquivo `.env.<nome_da_loja>` apenas durante runtime via CLI request. As importações de pacotes posteriores são expostas baseadas nas variáveis de tal tenant globalmente.
- **Padrão Singleton:** Empregado na camada do Banco (`database.py`), o pool de conexão do SQLAlchemy usa um estado estático para instanciar a Engine (`_engine`) única por run minimizando overhead de latência por conexão, contendo salvaguardas de reset de escopo inter-tenant.
- **DRY (Don't Repeat Yourself):** A agregação global centralizou algoritmos idênticos dentro de helpers (`src.config.utils.py`) resolvendo limpeza massiva de prefixos sujos de planilhas de SKU em único provedor, além do inicializador da aplicação (`src.config.tenant.py`).
- **Star Schema Data Warehouse:** Modelagem denormalizada otimizada para Analytics usando blocos de *Dimensões* (Produtos e Clientes) atrelados referencialmente em *Tabelas Fato* iterativas (Fato Pedidos, Fato Custos e Publicações), mitigando anomalia de informações.
- **Padrão Staging-to-Prod / Idempotência:** A camada de load previne chaves duplicadas. A injeção do DataFrame copia para tabelas sujas (Staging Table: `stg_`), cruzas as informações aplicando queries diretas transacionais `INSERT ... ON DUPLICATE KEY UPDATE` na tabela primária, validando integridade no DB de maneira unificada e por último, expurga a Staging Table na finalização do cursor.

---

## 4. Estrutura de Diretórios (Folder Tree)

```text
data_mercadoLivre/
├── main.py                     # Entrypoint principal do pipeline
├── .env.example                # Template de configuração e doc root
├── .gitignore                  # Políticas restritas de versionamento local
├── scripts/
│   └── batch_cost_update.py    # Atualizações stand-alones avulsas em banco
├── src/
│   ├── config/                 # Lida com o núcleo base e settings globais
│   │   ├── settings.py         # Declaração tardia estática das keys do sistema
│   │   ├── tenant.py           # Valida inputs e vincula a inicialização na loja certa
│   │   └── utils.py            # Helpers gerais e normalização (SKUs)
│   ├── extract/                # Lida com conexões ativas de API  REST e planilhas
│   │   ├── local_data.py       # Leitores de planilhas e tratativas primitivas
│   │   ├── meli_client.py      # Agente de consumo/auth de rotas do Mercado Livre
│   │   └── shopee_client.py    # Agente de HMAC auth/assinatura para Shopee Seller API
│   ├── jobs/                   # Fluxos agregadores em batches
│   │   ├── run_ads_update.py   # Lida com publicidade (PADS) focado na timeline retroativa 
│   │   └── run_costs_update.py # Conector focado em contas a pagar (Full/Reembolso)
│   ├── load/                   # Lida com Queries MySQL puras
│   │   └── database.py         # Construção tabelas (CREATE), Singleton Pool e Upsert
│   └── transform/              # Lida com Pandas e Modelagem Fato/Dim
│       └── data_processor.py   # Lida com tipagem, preenchimento base e joins Dataframes
└── material/                   # Repositório de arquivos de input manuais offline 
    └── produtos_custo.xlsx
```

---

## 5. Fluxo de Dados (Data Flow)

O ciclo de vida principal de processamento de informações roda a partir da chamada de orquestração:

1. **Gatilho Inicial**: O `main.py` roda com uma flag de requisição (ex: `--loja prohair`).
2. **Contextualização Setup**: A rotina `configurar_ambiente()` invoca o path correspondente carregando variáveis ocultas do DB e Auth (`.env.prohair`), populando todo o `src.config.settings`.
3. **Extração API (Extract)**: Módulos como `meli_client.py` injetam Autorizações Bearer com checagem de timeout nos endpoints para varrer todos os JSONs em blocos retroativos da operação na collection final de Pedidos.
4. **Mutação Multidimensional (Transform)**: Lida com vetores cruzados. `data_processor.py` fraciona os dicionários de ordens, isola Clientes na sua dimensão unificando identificadores e separa em fatias o modelo Fato de itens de acordo com as SKU base vindas de planilhas importadas de inventário via utilitários locais. Adiciona parâmetros nativos como `origem_venda`.
5. **Carga Segregada Transacional (Load)**: Cada vetor de Dataframe limpo usa de tabelas espelhos temporárias via biblioteca local (*sqlalchemy to_sql stg_*). Seguidamente aciona *statements SQL Update on Duplicate Keys*, forçando atualização incremental nos Custos Atrelados aos Skus baseados nos preenchimento originais sem truncamento da tabela real.
6. O pipeline reporta métricas da transação fechando o ambiente com isolamento lógico protegido para a próxima iteração.

---

## 6. Segurança e Configuração (Security & Setup)

A segurança Multi-Tenant é garantida através dos seguintes controles arquitetônicos:

- **Tokens Ignorados:** Os repositórios do Git operam bloqueios totais coringa (`*`) contra os artefatos `tokens_meli_*.json`, `.env*` e demais caches confidenciais, assegurando o vazamento nulo das chaves criptográficas OAuth trocadas pela API.
- **Validações Antecipadas:** Falta de credenciais obrigatórias não gera warnings silenciosos e sim exceptions explícitas no motor (`ValueError`, detecção restritiva os.getenv), travando imediatamente a execução sistêmica.
- **Log Sanitation:** Ausência total de logs base contendo variáveis sensíveis do cliente e/ou fragmentos f-strings vazados das exceptions do conector que acidentalmente exibiria senhas.
- **Persistência Volátil Isolada:** O Tenant X nunca pode interferir nos blocos de autenticação HTTP ou Database do Tenant Y devido à rotina lazy initializers da Engine referenciando o DB e os caminhos de persistência na raiz.
