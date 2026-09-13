# BookMyShow Data Platform

A Databricks-on-Azure lakehouse data platform for a BookMyShow-style ticketing
business — bookings, payments, seat inventory, pricing/promotions, and
personalization analytics — designed as a Databricks Asset Bundle (DABs)
project and, so far, **built and fully tested locally without any cloud
access**.

- [`BookMyShow_Data_Platform_Implementation_Plan.md`](BookMyShow_Data_Platform_Implementation_Plan.md) — the full architecture and 5-phase roadmap this project follows.
- [`bmsdp/`](bmsdp/) — the actual project: pipelines, tests, infra-as-code.
- [`bmsdp/reports/pipeline_run_report.html`](bmsdp/reports/pipeline_run_report.html) — a snapshot of real output from a local pipeline run (download and open in a browser; GitHub won't render it inline).

## About this project

The implementation plan describes a full Medallion-architecture (Bronze →
Silver → Gold) lakehouse on Azure Databricks: Azure Data Factory / Event Hubs
for ingestion, ADLS Gen2 + Delta Lake for storage, Databricks Jobs/DLT for
processing, and Unity Catalog for governance — supporting real-time and batch
analytics, fraud detection, and ML use cases (demand forecasting, dynamic
pricing, recommendations).

This repository is the **implementation** of that plan, currently covering:

| Phase | Status |
|---|---|
| Phase 0 — Foundations (DABs scaffold, Unity Catalog setup, CI) | ✅ Built |
| Phase 1 — Bookings & Payments | ✅ Built, tested locally |
| Phase 1 — Inventory & Seating | ✅ Built, tested locally |
| Phase 1 — Pricing & Promotions | ✅ Built, tested locally |
| Phase 1 — Users 360 (personalization gold layer) | ✅ Built, tested locally |
| Phase 1 — Marketing/CRM, Partner/Venue data | ⬜ Not started |
| Phase 2 — Streaming/CDC (Event Hubs, Debezium) | ⬜ Not started |
| Phase 3 — Governance & DQ hardening (Lakehouse Monitoring) | ⬜ Not started |
| Phase 4 — ML & advanced analytics (Feature Store, MLflow) | ⬜ Not started |
| Phase 5 — Scale/optimize/productionize | ⬜ Not started |
| **Deployed to a real Azure Databricks workspace** | ⬜ **Not yet — no cloud access used so far** |

Everything marked "built" has been verified against **real local Delta Lake
tables** (not just in-memory checks): pytest unit tests for every
transformation, an end-to-end run against generated synthetic data, and a
second incremental run proving the CDC dedup and SCD Type 2 dimension merge
logic actually work across runs — see [`bmsdp/README.md`](bmsdp/README.md#running-locally)
for the exact commands.

## Architecture (target state)

```
Sources                Ingestion              Storage (ADLS Gen2)         Compute/Processing      Serving
─────────              ──────────             ───────────────────         ──────────────────      ───────
OLTP DBs, payment       Azure Data Factory /    Bronze (raw)                Databricks Jobs         AI/BI Dashboards
gateway, clickstream,   Debezium CDC     ─────► Silver (cleansed)  ◄─────►  (Auto Loader,           Feature Store
seat inventory,         Event Hubs / Kafka      Gold (business-level        DLT pipelines)          Model Serving
marketing/CRM APIs                              facts/dims/aggregates)      Unity Catalog           Reverse ETL
```

This repo currently implements the Bronze→Silver→Gold pipeline logic and the
Unity Catalog / DABs scaffolding for that architecture; the Azure
infrastructure itself (ADLS, Event Hubs, a live Databricks workspace) has not
been provisioned.

## Requirements

To run and test this project locally:

| Requirement | Version | Why |
|---|---|---|
| Python | 3.11 | PySpark 3.5.x does not yet support Python 3.12+ |
| Java (JDK) | 17 | Required by Spark's JVM |
| pip packages (dev) | see `bmsdp/requirements-dev.txt` | `pyspark==3.5.3`, `delta-spark==3.2.0`, `pytest>=8.0.0`, `chispa>=0.10.1`, `ruff>=0.6.0` |
| pip packages (runtime) | see `bmsdp/requirements.txt` | `databricks-sdk>=0.30.0` |

To eventually **deploy** this to Azure (not required for local dev/test):

- An Azure subscription with an Azure Databricks workspace (Unity Catalog-enabled)
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) configured with workspace auth
- ADLS Gen2 storage account for the landing zone
- Azure AD (Entra ID) groups for role-based Unity Catalog grants

### Setup (macOS)

```bash
brew install openjdk@17 python@3.11
cd bmsdp
python3.11 -m venv ../.venv && source ../.venv/bin/activate
pip install -r requirements-dev.txt
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"
```

### Run the tests

```bash
cd bmsdp
python -m pytest -q
```

### Run the full pipeline and see real output

```bash
python data/sample/generate_sample_data.py
python scripts/run_local_pipeline.py
```

This generates synthetic landing-zone data and writes real Delta Lake tables
under `bmsdp/data/local_lakehouse/` for all five domains. Full details,
including how to test the incremental SCD2 merge and how to set this up in
VS Code, are in [`bmsdp/README.md`](bmsdp/README.md).

## Project structure

```
.
├── BookMyShow_Data_Platform_Implementation_Plan.md   # architecture + roadmap
└── bmsdp/                                             # the Databricks Asset Bundle project
    ├── databricks.yml                                 # DABs bundle config (dev/stage/prod targets)
    ├── resources/                                      # DLT pipeline + job definitions
    ├── src/                                             # pipeline logic, one package per domain
    │   ├── pipelines/{bookings,inventory,pricing,users}/
    │   ├── setup/*.sql                                  # Unity Catalog bootstrap SQL
    │   └── utils/                                        # shared schemas + Spark helpers
    ├── data/sample/                                      # synthetic data generators
    ├── scripts/                                           # local run scripts (no Databricks needed)
    ├── tests/                                              # pytest unit tests
    ├── reports/pipeline_run_report.html                    # static output snapshot
    └── .vscode/                                            # VS Code test/debug config
```

See [`bmsdp/README.md`](bmsdp/README.md) for the full breakdown of what each
domain implements, what's deliberately not done yet, and deployment steps
once Azure access is available.
