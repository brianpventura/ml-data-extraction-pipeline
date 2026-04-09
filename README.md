# 📊 Mercado Livre ETL Pipeline

![Python](https://img.shields.io/badge/python-3.10+-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) ![MySQL](https://img.shields.io/badge/mysql-4479A1.svg?style=for-the-badge&logo=mysql&logoColor=white) ![Power Bi](https://img.shields.io/badge/power_bi-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

## 📌 Overview

End-to-end **ETL pipeline** built in Python that extracts operational data from the Mercado Livre REST API (Orders, Advertising Costs, Billing / Operational Costs), transforms it into a Star Schema, and loads it into a MySQL database for consumption in Power BI dashboards.

### Key Features

- **OAuth 2.0 authentication** with automatic token refresh and persistence.
- **Paginated extraction** with dynamic `date_to` pivot to bypass the API's 10k offset limit.
- **Shipping cost enrichment** via the Shipments API (multi-source comparison).
- **Cost integration** from Excel and JSON local sources via SKU-based joins.
- **Advertising metrics** extraction with per-campaign daily granularity.
- **Operational costs** (Fulfillment storage, inbound, returns, affiliates) from the Billing API.
- **Idempotent upserts** via staging tables — safe to re-run without duplicates.
- **Centralized configuration** — all secrets in `.env`, all constants in `settings.py`.

---

## ⚙️ Architecture

The pipeline follows the classic **ETL** pattern with a Star Schema data model:

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌───────────┐
│   EXTRACT   │ ──▸ │  TRANSFORM   │ ──▸ │    LOAD     │ ──▸ │  DATA VIZ │
│             │     │              │     │             │     │           │
│ • Orders    │     │ • Star Schema│     │ • MySQL     │     │ • Power BI│
│ • Costs     │     │ • SKU Merge  │     │ • Upsert    │     │           │
│ • Ads       │     │ • Aggregation│     │ • Staging   │     │           │
│ • Billing   │     │              │     │             │     │           │
└─────────────┘     └──────────────┘     └─────────────┘     └───────────┘
```

---

## 📂 Project Structure

```text
/
├── main.py                          # Pipeline orchestrator (entry point)
├── src/
│   ├── config/
│   │   └── settings.py              # Centralized configuration & constants
│   ├── extract/
│   │   ├── marketplace_client.py    # OAuth + Orders + Shipping API client
│   │   └── local_data.py            # Excel / JSON cost file readers
│   ├── transform/
│   │   └── data_processor.py        # Star Schema + cost enrichment
│   ├── load/
│   │   └── database.py              # MySQL DDL, upserts, staging tables
│   └── jobs/
│       ├── run_ads_update.py        # Advertising metrics extraction job
│       └── run_costs_update.py      # Operational costs extraction job
├── scripts/
│   └── batch_cost_update.py         # Standalone batch cost update utility
├── tests/                           # Unit tests (pytest)
├── material/                        # Local data files (not tracked)
├── dashboard/                       # Power BI files (not tracked)
├── .env.example                     # Environment variables template
├── .gitignore
└── requirements.txt
```

---

## 🚀 Getting Started

### Prerequisites

- **Python 3.10+**
- **MySQL Server** (local or cloud)
- **Mercado Livre Developer Account** ([developers.mercadolivre.com.br](https://developers.mercadolivre.com.br))
  - You will need: `Client ID`, `Client Secret`, `Redirect URI` and an `Authorization Code`.

### 1. Clone the repository

```bash
git clone https://github.com/brianpventura/ml-data-extraction-pipeline.git
cd ml-data-extraction-pipeline
```

### 2. Create and activate a virtual environment

```bash
# Linux / macOS
python -m venv .venv
source .venv/bin/activate

# Windows
python -m venv .venv
.venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your actual credentials
```

### 5. Run the pipeline

```bash
python main.py
```

The CLI will prompt you to choose an extraction mode:
- **Incremental** — fetches orders since the last saved date.
- **Retroactive** — specify number of days to look back.
- **Date range** — explicit start and end dates (`YYYY-MM-DD,YYYY-MM-DD`).

---

## 🧱 Database Schema (Star Schema)

| Table | Type | Description |
|---|---|---|
| `tb_cliente` | Dimension | Customer data (ID, nickname) |
| `tb_produto` | Dimension | Product catalog (SKU, description, unit cost) |
| `tb_pedido` | Fact | Order headers (date, totals, shipping cost) |
| `tb_itens_pedido` | Fact | Order line items (quantity, unit price) |
| `tb_custos_ads` | Fact | Daily advertising metrics per campaign |
| `tb_custos_operacionais` | Fact | Monthly operational costs (storage, fulfillment, returns) |

---

## 🔐 Security

- All credentials are loaded from `.env` (never hardcoded).
- `tokens.json` and `.env` are excluded from version control via `.gitignore`.
- `.env.example` is provided as a safe template with no real values.

---

## 👨‍💻 Author

**Brian Pereira Ventura**
Data Analyst / Developer

🔗 [LinkedIn](https://br.linkedin.com/in/brian-ventura-68081a25a)
🐙 [GitHub](https://github.com/brianpventura)
