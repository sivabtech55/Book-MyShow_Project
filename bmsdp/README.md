# BMSDP — BookMyShow Data Platform (Phase 0 + Phase 1 domains)

Databricks Asset Bundle (DABs) project implementing Phase 0 (foundations) and
five Phase 1 domains from the platform implementation plan — **Bookings**,
**Payments**, **Inventory & Seating**, **Pricing & Promotions**, and a
**Users 360** gold layer — each a full Bronze → Silver → Gold pipeline, built
and tested locally (no cloud access used), ready to deploy to a real Azure
Databricks workspace.

## What's here

```
bmsdp/
  databricks.yml                     # DABs bundle: targets dev/stage/prod
  resources/
    jobs/bootstrap_unity_catalog.job.yml   # one-time UC catalog/schema/grant/masking setup
    pipelines/bookings_pipeline.yml        # Bookings + Payments DLT pipeline + scheduled job
    pipelines/inventory_pipeline.yml       # Inventory & Seating DLT pipeline
    pipelines/pricing_pipeline.yml         # Pricing & Promotions + Users 360 DLT pipelines
  src/
    setup/*.sql                      # Unity Catalog bootstrap SQL (catalogs, grants, PII masking views)
    utils/schemas.py                 # explicit bronze source schemas, one per domain
    utils/spark_helpers.py           # shared transforms used across domains (dedup, ingestion metadata)
    pipelines/bookings/              # Bookings + Payments: dedup, DQ, SCD2 dims, fact_bookings/fact_payments
    pipelines/inventory/             # Inventory & Seating: seat-hold state, fact_seat_occupancy
    pipelines/pricing/               # Pricing & Promotions: fact_promotion_redemptions, performance agg
    pipelines/users/                 # Users 360: per-user booking behavior gold table
      (each domain has transformations.py [pure, unit-tested] + dlt_pipeline.py [thin @dlt.table wrapper])
  data/sample/generate_sample_data.py        # synthetic landing-zone data generator (batch 1), all domains
  data/sample/generate_incremental_batch.py  # "day 2" batch: new bookings, cancellations, a venue capacity change
  data/local_lakehouse/                      # real Delta Lake tables written by run_local_pipeline.py (gitignored)
  scripts/run_local_e2e.py                   # quick in-memory (no Delta) run of the Bookings logic
  scripts/run_local_pipeline.py              # full run, all domains, against real local Delta tables, SCD2 merge across runs
  tests/                                     # pytest + local SparkSession unit tests, one file per domain
  .github/workflows/ci.yml                   # lint, test, bundle validate
```

Why the split between `transformations.py` and `dlt_pipeline.py`: DLT's
`dlt.table`/`dlt.read` APIs only exist inside a running Databricks DLT
pipeline, so logic written directly against them can't be unit tested off
of Databricks. All actual business logic (dedup, DQ rules, SCD2, fact
building) is plain functions over DataFrames in `transformations.py`; the
DLT file just wires those functions to `@dlt.table` sources/sinks. Same
logic, testable locally, no behavior duplicated.

## What's implemented

**Bookings + Payments** (`src/pipelines/bookings/`): Auto Loader ingestion of
`bookings`, `payments`, `users`, `venues`, `movies`, `shows`; last-write-wins
CDC dedup; DQ gate on bookings (valid keys, non-negative amounts, known
status) with a `quarantine_bookings` table for rejected rows; SCD Type 2
dimensions (`dim_user`, `dim_venue`, `dim_show`); `fact_bookings` with
computed revenue (0 for cancellations); `fact_payments` enriched with
booking context; `gold_daily_revenue` aggregate.

**Inventory & Seating** (`src/pipelines/inventory/`): seat-lock/hold events
(`HELD`/`BOOKED`/`RELEASED`/`EXPIRED`), latest-state-per-seat dedup, DQ gate
on show/seat keys and known status, `fact_seat_occupancy` per show
(capacity from `dim_venue`, occupied-seat count, occupancy %).

**Pricing & Promotions** (`src/pipelines/pricing/`): promotion definitions
and redemption events, DQ gates (valid discount type/value, known status,
valid FK references), `fact_promotion_redemptions` with computed net revenue
(gross revenue minus discount, floored at 0), `gold_promotion_performance`
aggregate (redemptions/discount given/net revenue per promotion).

**Users 360** (`src/pipelines/users/`): pure gold-layer aggregation on top
of `fact_bookings`/`dim_user` — per-user lifetime spend, booking count,
cancellation rate, booking recency — the feature set a recommendation or
churn model would start from (per the platform's personalization objective).

**Governance**: catalog/schema bootstrap job, Azure AD group → Unity Catalog
grants (data engineers / analysts / ML engineers), a masked `dim_user_masked`
view so non-engineers never see raw email/phone.

## Running locally

Requires Java 17 and Python 3.11 (PySpark 3.5.x doesn't yet support newer
Pythons). On macOS:

```bash
brew install openjdk@17 python@3.11
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"
```

Unit tests (pure transformation logic, local SparkSession, no Databricks
needed):

```bash
cd bmsdp
python -m pytest -q
```

Generate synthetic sample data and run the full pipeline end-to-end in
batch mode (proves the bronze→silver→gold logic against real-shaped data,
including a few deliberately dirty/duplicate records to exercise the DQ and
dedup rules):

```bash
python data/sample/generate_sample_data.py
python scripts/run_local_e2e.py
```

To exercise the pipeline against **real local Delta Lake tables** (not just
in-memory DataFrames), including the incremental SCD2 dimension merge that a
single batch can't test, run it twice with a second landing batch in between:

```bash
python data/sample/generate_sample_data.py
python scripts/run_local_pipeline.py            # run 1: bootstraps all Delta tables from scratch

python data/sample/generate_incremental_batch.py  # "day 2": new bookings, some cancellations,
                                                    # and a venue capacity change
python scripts/run_local_pipeline.py            # run 2: dedups across both batches, SCD2-merges
                                                 # dim_venue against what run 1 persisted
```

Run 2's output shows the changed venue getting a closed-out old row
(`is_current=false`, `effective_end` set) and a new current row with the
updated capacity, while every unchanged venue keeps its single row —
verified against the actual Delta tables under `data/local_lakehouse/`.
Re-running a third time with no new batch is a no-op (idempotent).

Lint:

```bash
ruff check .
```

## Deploying to a real Azure Databricks workspace

This project has not been deployed to a live workspace — it was built and
tested locally only. To deploy:

1. Install the Databricks CLI (`brew install databricks/tap/databricks` or
   see Databricks docs) and configure auth for your workspace(s).
2. Fill in the placeholders in `databricks.yml`: workspace `host` per target,
   `sql_warehouse_id`, and the prod `service_principal_name`.
3. Point `landing_root` at your actual ADLS Gen2 landing container.
4. Create the Azure AD groups referenced in `src/setup/01_grants.sql`
   (`bmsdp-data-engineers`, `bmsdp-analysts`, `bmsdp-ml-engineers`) or rename
   them to match your existing groups.
5. Validate and deploy:

   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   databricks bundle run bootstrap_unity_catalog -t dev
   databricks bundle run run_bookings_pipeline -t dev
   ```

6. Land some sample files under `<landing_root>/bookings/`, `/payments/`,
   etc. (the same shape as `data/sample/generate_sample_data.py` produces)
   and re-run the pipeline, or point Auto Loader at your real CDC/event
   output once that ingestion is wired up (Phase 2 of the plan).

## What's deliberately not done yet

Per the phased roadmap this is Phase 0 + Phase 1 domains only:
- **Marketing/CRM and Partner/Venue data** (the remaining two Phase 1
  domains from the plan) are not built.
- No CDC/Event Hubs streaming sources yet (Phase 2) — bronze reads land
  files via Auto Loader; wiring a real Debezium/Event Hubs source only
  changes the bronze read, not silver/gold.
- No Lakehouse Monitoring / drift detection (Phase 3).
- No Feature Store / MLflow / model serving (Phase 4).
- `dlt_pipeline.py` (the Databricks deployment path) still calls
  `bootstrap_scd2` on every run rather than `build_scd2`, because DLT
  materialized views don't give the table function a handle to read its own
  previous output. `scripts/run_local_pipeline.py` demonstrates the real
  merge pattern (read prior Delta state, call `build_scd2`) against real
  local Delta tables — port that read-then-merge pattern into the DLT
  pipeline (e.g. via `dlt.apply_changes`/CDC `AUTO CDC` flow, which handles
  SCD2 natively) before relying on dimension history in Databricks.
- The `inventory_pipeline` and `pricing_pipeline` DLT resources read
  `bmsdp_bookings.dim_venue` / `bmsdp_bookings.fact_bookings` etc. by
  hardcoded catalog-qualified name across pipelines. This works once
  deployed (Unity Catalog tables are addressable across DLT pipelines) but
  hasn't been validated against a real workspace — verify the fully
  qualified names match your actual `catalog_name`/pipeline `target` once
  you deploy.
- Seat-hold synthetic data is generated independently of the synthetic
  bookings (not derived from actual booking seat counts), so
  `fact_seat_occupancy` numbers won't reconcile against `fact_bookings` in
  this sample dataset — fine for exercising the pipeline logic, not
  meaningful as a consistency check between the two domains.
