# UCM TFM Lakehouse

End-to-end lakehouse project for the UCM Big Data Master’s TFM. It includes IaC for Azure + Databricks + AKS, orchestration with Airflow, Bronze/Silver/Gold data pipelines, Databricks notebooks, and a Python application with a Telegram bot and Streamlit UI for natural‑language analytics on top of Databricks.

## Overview

- Lakehouse layers (Bronze/Silver/Gold) with modular Python libraries and JSON configs
- Databricks notebooks that execute the engines using configs uploaded to DBFS
- Orchestration via Airflow (deployed to AKS with Helm)
- Terraform modules to provision Azure resources, Databricks workspace, Unity Catalog, clusters, and AKS
- Optional user interfaces: Telegram bot and Streamlit chat UI powered by Databricks Genie
- Docker image and Makefile for local development and packaging

## Repository Structure

- `code/` – Python project (libraries, apps, Dockerfile, Makefile)
  - `src/libraries/bronze_layer/` – ingestion engine and ingestors
  - `src/libraries/silver_layer/` – transformation engine and processors
  - `src/libraries/gold_layer/` – semantic modeling engine and processors
  - `src/databricks_telegram_bot/` – Telegram bot + shared Databricks client + Streamlit UI
  - `src/streaming/` – producer/consumer stubs for real‑time ingestion
  - `tests/` – pytest suite for bronze/silver/gold and bot
  - `pyproject.toml` – dependencies (managed with `uv`)
  - `Dockerfile` – universal image for bot/UI
  - `Makefile` – dev, test, and docker commands
- `databricks_notebooks/` – notebooks for Bronze, Silver, Gold that call the Python engines
- `config_files/` – dataset configs for each layer (used by engines/notebooks)
- `airflow/` – DAGs to orchestrate Bronze→Silver in Databricks and usage notes
- `infrastructure/` – Terraform for Azure, Databricks, Unity Catalog, AKS, Helm charts
- `scripts/` – helper scripts (e.g., AKS login)
- `env.example` – environment template for local runs

## Architecture

- Bronze: batch/stream ingestion into external locations
- Silver: business transformations and data quality
- Gold: dimensional/fact modeling for analytics
- Databricks Unity Catalog external locations map to ADLS Gen2 (bronze/silver/gold)
- Airflow DAGs call Databricks notebooks which load the engines with configs from DBFS
- ML/Analytics friendly: optional MLflow Helm chart and Streamlit UI for exploration

High level flow
1) Ingestion (Bronze) → 2) Transform (Silver) → 3) Model (Gold) → 4) Query via UI/Bot

## Data Pipelines

- Bronze engine: `code/src/libraries/bronze_layer/ingest_engine.py`
- Silver engine: `code/src/libraries/silver_layer/process_engine.py`
- Gold engine: `code/src/libraries/gold_layer/gold_engine.py`
- Notebooks:
  - Bronze: `databricks_notebooks/Bronze.py`
  - Silver: `databricks_notebooks/Silver.py`
  - Gold: `databricks_notebooks/Gold.py`
- Configs (examples):
  - Bronze: `config_files/bronze/facturas_config.json`
  - Silver: `config_files/silver/facturas_config.json`
  - Gold: `config_files/gold/fact_facturas_config.json`

Unity Catalog module uploads all `infrastructure/modules/unity_catalog/config_files/**` to DBFS at `/FileStore/config/**` so notebooks can consume them with parameters.

## Orchestration (Airflow on AKS)

- DAGs:
  - `airflow/dags/databricks/datasets_to_bronze.py` – triggers Bronze loads then Silver DAG
  - `airflow/dags/databricks/datasets_to_silver.py` – loads Silver datasets with dependencies
- Both DAGs use `DatabricksTaskOperator` against a shared cluster/warehouse
- See `airflow/README.md` for accessing the Airflow UI via port‑forward after AKS deploy

## Infrastructure as Code (Terraform)

- Azure resources: Resource Groups, ADLS Gen2 for landing/lakehouse, Key Vault, AKS
- Databricks: Workspace, Access Connector, Unity Catalog external locations, clusters, secret scope
- Helm: Airflow chart deployed to AKS (and MLflow values provided)

Key files:
- Root stack: `infrastructure/main.tf`, `infrastructure/providers.tf`, `infrastructure/variables.tf`, `infrastructure/terraform.tfvars`
- Modules: `infrastructure/modules/*` (resource_group, storage_account, databricks_workspace, databricks_clusters, key_vault, access_connector, unity_catalog, aks)
- Helm values: `infrastructure/helm_charts/airflow.yaml`

Apply (example workflow):
1. Ensure Azure and Databricks provider credentials are available (AAD SP, PAT)
2. Review and adapt `infrastructure/terraform.tfvars`
3. `terraform init` → `terraform plan` → `terraform apply`

Notes
- The Unity Catalog module also enables the DBFS browser and uploads config files to `/FileStore/config/...`
- External locations for `bronze`, `silver`, `gold` are created and granted to account users

## Applications (Bot + Web UI)

- Telegram bot and Streamlit UI share a Databricks client to send natural‑language prompts to Genie and render results with human‑friendly number formatting.
- Entrypoints:
  - Bot: `code/src/databricks_telegram_bot/main.py`
  - Streamlit: `code/src/databricks_telegram_bot/streamlit_ui.py`
- Make targets (from `code/Makefile`): `run-bot`, `run-chat`, `build`, `docker-run-bot`, `docker-run-chat`

## Local Development

Prerequisites
- Python 3.11+
- `uv` package manager
- Docker (optional)

Install and run
```bash
cd code
uv sync
make test           # run pytest suite
make run-bot        # Telegram bot
make run-chat       # Streamlit UI (default port 8501)
```

Docker
```bash
cd code
make build
make docker-run-bot
STREAMLIT_PORT=8080 make docker-run-chat
```

## Configuration

Environment variables (copy `env.example` → `.env` at repo root):
- `DATABRICKS_WORKSPACE_URL` – Databricks workspace URL
- `DATABRICKS_ACCESS_TOKEN` – Personal Access Token
- `DATABRICKS_CATALOG` / `DATABRICKS_SCHEMA` – Optional; defaults are supported by the code
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_ALLOWED_USERS` – Required for Telegram bot
- `LOG_LEVEL`, `MAX_QUERY_TIMEOUT`, `STREAMLIT_PORT` – Optional runtime settings

Dataset configs
- Each dataset’s JSON config defines source/sink (schema/table), storage, catalog, and optional imputation and watermarking fields.
- Local copies live under `config_files/**`; Terraform also uploads notebook configs to DBFS under `/FileStore/config/**` for use by notebooks.

## Testing

Run with `make test` or `make test-coverage` inside `code/`. Tests cover bronze/silver processors and the Databricks bot client.

## Airflow Access (after AKS deploy)

See `airflow/README.md` for `kubectl port-forward`/`k9s` steps to reach the UI at `http://localhost:8080`.

## Notes

- This repository contains both platform IaC and application code; apply Terraform only when you intend to provision/update cloud resources.
- Keep secrets out of VCS; prefer Azure Key Vault and Databricks secret scopes.
