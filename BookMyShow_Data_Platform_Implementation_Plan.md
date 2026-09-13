# BookMyShow Data Platform — Implementation Plan
## Databricks on Azure (Lakehouse Architecture)

---

## 1. Objective

Build a scalable, secure, and governed data platform for a BookMyShow-like ticketing business, supporting:

- Real-time and batch analytics on bookings, payments, seat inventory, pricing, and marketing
- Personalization and recommendation use cases
- Fraud/anomaly detection (payment fraud, bot bookings, scalping)
- Business reporting (revenue, occupancy, cancellations, regional performance)
- ML use cases (demand forecasting, dynamic pricing, churn, recommendations)

---

## 2. High-Level Architecture

```
Sources                Ingestion              Storage (ADLS Gen2)         Compute/Processing      Serving
─────────              ──────────             ───────────────────         ──────────────────      ───────
OLTP DBs (bookings,     Azure Data Factory /    Bronze (raw)                Databricks Jobs         AI/BI Dashboards
venues, users)          Debezium CDC     ─────► Silver (cleansed)  ◄─────►  (Auto Loader,           SQL endpoints
Payment gateway logs    Event Hubs /            Gold (business-level        DLT pipelines,          Feature Store
Clickstream/App events  Kafka                   aggregates)                 Spark jobs)             (ML models)
Marketing/CRM APIs      Azure Event Hubs                                   Unity Catalog            Reverse ETL
Seat inventory service  Capture (streaming)                                (governance)             (CRM/marketing)
```

**Core pattern:** Medallion architecture (Bronze → Silver → Gold) on Azure Data Lake Storage Gen2, orchestrated and processed via Databricks, governed by Unity Catalog.

---

## 3. Azure + Databricks Component Map

| Layer | Azure/Databricks Service | Purpose |
|---|---|---|
| Ingestion (batch) | Azure Data Factory (ADF) | Extract from OLTP (bookings, venues, users), SFTP/partner feeds |
| Ingestion (CDC) | Debezium / ADF CDC connectors → Event Hubs | Near-real-time change capture from transactional DBs |
| Ingestion (streaming) | Azure Event Hubs (Kafka-compatible) | Clickstream, seat-lock events, payment events |
| Storage | Azure Data Lake Storage Gen2 (ADLS) | Bronze/Silver/Gold zones, Delta Lake format |
| Processing | Azure Databricks (Jobs Compute, DLT) | ETL/ELT, streaming, data quality, transformations |
| Governance | Unity Catalog | Centralized access control, lineage, audit, data classification |
| Orchestration | Databricks Workflows (+ ADF for cross-system orchestration) | DAG scheduling, retries, alerting |
| Secrets/Identity | Azure Key Vault + Databricks Secret Scopes, Azure AD (Entra ID) | Credential management, SSO, service principals |
| Serving (BI) | Databricks SQL Warehouses + AI/BI Dashboards | Dashboards for business/ops teams |
| Serving (ML) | Databricks Feature Store, MLflow, Model Serving | Recommendations, dynamic pricing, fraud scoring |
| Networking | VNet injection, Private Link, NSGs | Secure Databricks workspace deployment |
| CI/CD | Azure DevOps (or GitHub Actions) + Databricks Asset Bundles (DABs) | Version-controlled deployment of notebooks/jobs/DLT pipelines |
| Monitoring | Azure Monitor, Databricks system tables, Log Analytics | Job health, cost tracking, SLA alerting |

---

## 4. Data Domains & Key Sources

| Domain | Example Sources | Ingestion Mode |
|---|---|---|
| Bookings & Transactions | Booking OLTP DB (tickets, orders, cancellations) | CDC (streaming) |
| Payments | Payment gateway webhooks/logs | Streaming (Event Hubs) |
| Inventory & Seating | Seat-lock/hold service, venue/show scheduling | CDC + streaming |
| Users & Profiles | User service, auth/CRM | Batch (daily) + CDC |
| Clickstream/App Events | Mobile/web app telemetry (SDK events) | Streaming (Event Hubs/Kafka) |
| Pricing & Promotions | Pricing engine, coupon/offer service | Batch |
| Marketing/CRM | Email/SMS/push campaign systems, ad platforms | Batch (API pulls via ADF) |
| Partner/Venue Data | Cinema/venue partner feeds | Batch (SFTP/API) |

---

## 5. Medallion Layer Design

### Bronze (Raw)
- Stored as-is (JSON/Avro/CSV/CDC records) in Delta format
- Schema-on-read, minimal transformation
- Partitioned by ingestion date + source system
- Databricks Auto Loader for incremental file ingestion; Structured Streaming for Event Hubs

### Silver (Cleansed/Conformed)
- Deduplication, schema enforcement, data type standardization
- Business key resolution (booking_id, user_id, show_id, venue_id)
- Slowly Changing Dimensions (SCD Type 2) for users, venues, pricing
- Data quality rules via Delta Live Tables (DLT) expectations
- PII handling: masking/tokenization for user PII (name, phone, email)

### Gold (Business/Curated)
- Star-schema fact/dimension models:
  - `fact_bookings`, `fact_payments`, `fact_seat_occupancy`, `fact_clickstream_sessions`
  - `dim_user`, `dim_venue`, `dim_show`, `dim_movie`, `dim_date`, `dim_promotion`
- Aggregates: daily revenue, occupancy %, cancellation rate, regional performance
- ML feature tables (Feature Store) for recommendations/fraud/pricing

---

## 6. Governance & Security (Unity Catalog)

- **Catalog structure:** `env` (dev/stage/prod) → `catalog` (bronze/silver/gold or domain-based) → `schema` → `table`
- **Access control:** Role-based via Azure AD groups mapped to Unity Catalog grants (data engineers, analysts, ML engineers, business users)
- **PII governance:** Column-level tagging + dynamic views/row filters for masking PII (phone, email, payment info)
- **Lineage:** Automatic via Unity Catalog for audit and impact analysis
- **Audit logs:** Unity Catalog audit logs → Log Analytics for compliance (PCI-DSS relevant for payment data)
- **Secrets:** Azure Key Vault-backed secret scopes (no hardcoded credentials)
- **Network:** Private Link/VNet injection so Databricks control plane traffic doesn't traverse public internet; NSGs restricting storage/DB access

---

## 7. Orchestration & CI/CD

- **Databricks Asset Bundles (DABs)** for defining jobs, DLT pipelines, and workflows as code
- **Git integration:** Databricks Repos synced to Azure DevOps/GitHub; branch-based dev (feature → dev → stage → prod)
- **CI pipeline:** lint (sqlfluff/black), unit tests (pytest + `chispa`/`dbx` for PySpark), DLT expectations as data quality gates
- **CD pipeline:** Deploy DABs to dev/stage/prod workspaces via Azure DevOps pipelines with environment-specific configs
- **Orchestration:** Databricks Workflows for intra-platform DAGs; ADF for cross-system triggers (e.g., waiting on external partner file drop)

---

## 8. Monitoring & Cost Management

- Databricks system tables (`system.billing.usage`, `system.compute.clusters`) for cost attribution by team/domain
- Job alerting via Databricks Workflows (email/Slack/Teams webhook) on failure or SLA breach
- Cluster policies to control instance types, auto-termination, and spot usage (cost control)
- Azure Monitor + Log Analytics dashboards for infra-level metrics
- Data quality monitoring via DLT expectations + Lakehouse Monitoring (drift detection on key tables)

---

## 9. Phased Implementation Roadmap

### Phase 0 — Foundations (Weeks 1–3)
- Azure landing zone setup: resource groups, VNet, subnets, NSGs, Key Vault
- Deploy Azure Databricks workspace (VNet-injected, Unity Catalog-enabled)
- Set up ADLS Gen2 with bronze/silver/gold containers
- Configure Unity Catalog metastore, catalogs, and Azure AD group-based access
- Set up Azure DevOps repo + DABs project scaffold

### Phase 1 — Batch Ingestion & Core Domains (Weeks 4–7)
- ADF pipelines for OLTP batch extracts (bookings, users, venues)
- Bronze/Silver/Gold pipelines for booking and payment domains (DLT)
- Core dimensional model (dim_user, dim_venue, dim_show, fact_bookings)
- Basic AI/BI dashboards (bookings, revenue)

### Phase 2 — Streaming & CDC (Weeks 8–11)
- Event Hubs setup for clickstream and seat-lock events
- Debezium/ADF CDC for near-real-time booking/payment updates
- Structured Streaming jobs in Databricks for Bronze ingestion
- Real-time seat occupancy and fraud-signal tables

### Phase 3 — Governance & Data Quality Hardening (Weeks 12–13)
- Column-level PII tagging, masking views for payment/user data
- DLT data quality expectations across all pipelines
- Lakehouse Monitoring for drift/anomaly detection
- Audit log wiring to Log Analytics; PCI-DSS control review

### Phase 4 — ML & Advanced Analytics (Weeks 14–18)
- Feature Store setup for recommendation and pricing features
- MLflow-tracked models: demand forecasting, dynamic pricing, churn, fraud scoring
- Model Serving endpoints for real-time inference (fraud scoring at checkout)
- Reverse ETL to CRM/marketing tools for personalization

### Phase 5 — Scale, Optimize, Productionize (Weeks 19+)
- Cost optimization: cluster policies, Photon, serverless SQL warehouses
- DR/backup strategy (cross-region replication for ADLS, workspace config backup)
- Full CI/CD maturity across dev/stage/prod
- Runbooks, on-call alerting, SLA documentation

---

## 10. Key Risks & Mitigations

| Risk | Mitigation |
|---|---|
| PII/payment data exposure | Unity Catalog masking, tokenization, PCI-DSS scoped network isolation |
| Streaming backpressure during high-demand events (movie releases, big matches) | Auto-scaling clusters, Event Hubs throughput unit scaling, backpressure-aware Structured Streaming configs |
| Schema drift from upstream OLTP changes | Schema evolution handling in Auto Loader/DLT, schema registry for CDC |
| Cost overruns from always-on clusters | Cluster policies, job clusters (not all-purpose), auto-termination, serverless SQL for BI |
| Data quality issues breaking downstream dashboards | DLT expectations as hard gates, quarantine tables for failed records |

---

## 11. Immediate Next Steps

1. Confirm Azure subscription/tenant and existing landing zone conventions (naming, tagging, RGs)
2. Confirm source systems for bookings/payments/inventory (identify CDC feasibility)
3. Stand up dev Databricks workspace + Unity Catalog metastore
4. Scaffold DABs project structure and Azure DevOps repo
5. Prioritize first domain end-to-end (recommend: Bookings + Payments, since they unlock revenue reporting fastest)
