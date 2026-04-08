# 📊 Mercado Livre ETL Pipeline

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) ![MySQL](https://img.shields.io/badge/mysql-4479A1.svg?style=for-the-badge&logo=mysql&logoColor=white) ![Power Bi](https://img.shields.io/badge/power_bi-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)  

## 📌 Visão Geral
Este projeto consiste em um pipeline de Engenharia de Dados (ETL) construído em Python. O objetivo principal é automatizar a extração de custos operacionais (Logística, Coleta, Devoluções e Ads) via API REST do Mercado Livre, tratar as informações e carregá-las em um banco de dados MySQL para posterior consumo em dashboards no Power BI.

Este projeto demonstra conhecimentos práticos em consumo de APIs, tratamento de dados, resiliência de código e integração com bancos de dados relacionais.

## ⚙️ Arquitetura e Fluxo de Dados
O pipeline segue a estrutura clássica de ETL (Extract, Transform, Load):

1. **Extract (Extração):**
   - Autenticação e gestão de tokens via API do Mercado Livre.
   - Consulta aos endpoints de faturamento.
   - **Tratamento de Exceções:** Implementação de *fallback* (gerador manual de datas) para garantir a coleta mesmo quando a API retorna instabilidades (`HTTP 422`) na listagem oficial de períodos.
2. **Transform (Transformação):**
   - Limpeza e padronização com Pandas.
   - Agregação de custos operacionais por categoria (ex: `COLETA_FULL`, `DEVOLUCAO`, `PADS`).
3. **Load (Carga):**
   - Inserção dos dados consolidados no MySQL para armazenamento histórico seguro.
4. **Data Viz:**
   - Conexão do Power BI ao MySQL para visualização dinâmica dos indicadores financeiros.


## 📂 Estrutura do Projeto

```text
/
├── dashboard/               # Arquivos .pbix do Power BI
├── src/                     # Código-fonte principal
│   ├── config/              # Configurações globais e de ambiente
│   ├── extract/             # Scripts de requisição à API (mercadolivre_client.py)
│   ├── transform/           # Regras de negócio e processamento de dados
│   └── load/                # Conexões e queries de inserção no banco de dados (database.py)
├── .env.example             # Template das variáveis de ambiente necessárias
├── requirements.txt         # Dependências do projeto
└── main.py                  # Orquestrador do pipeline
```

---

# 🚀 Como Executar o Projeto

## 📋 Pré-requisitos

- **Python 3.8+**
- **Servidor MySQL** local ou em nuvem
- **Conta de desenvolvedor no Mercado Livre**

Você precisará de:

- `Client ID`
- `Client Secret`

---

# ⚡ Passo a Passo

## 1️⃣ Clone o repositório

```bash
git clone https://github.com/brianpventura/ml-data-extraction-pipeline.git
cd ml-data-extraction-pipeline
```

## 2️⃣ Crie e ative um ambiente virtual
# Linux / Mac
```bash
python -m venv .venv
source .venv/bin/activate
```

# Windows
```bash
python -m venv .venv
.venv\Scripts\activate
```

## 3️⃣ Instale as dependências
```bash
pip install -r requirements.txt
```

## 4️⃣ Configure as variáveis de ambiente
# Crie um arquivo .env na raiz do projeto
```bash
cp .env.example .env
```

## 5️⃣ Execute o pipeline
```bash
python main.py
```

## 👨‍💻 Autor

Brian Pereira Ventura
Analista de Dados / Desenvolvedor

🔗 [LinkedIn](https://br.linkedin.com/in/brian-ventura-68081a25a)
🐙 [GitHub](https://github.com/brianpventura)