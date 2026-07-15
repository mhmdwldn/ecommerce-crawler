# E-Commerce Crawler Pipeline — PRD & Technical Product Specification

**Version:** 1.4  
**Status:** Delivered (Fase 0–4 complete)  
**Audience:** Internal Data Engineer & Software Engineer  
**Last updated:** 2026-07-15

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Problem Statement](#problem-statement)
3. [Architecture Overview](#architecture-overview)
4. [Feature 1 — Automated Crawling via Asset Registry](#feature-1--automated-crawling-via-asset-registry)
5. [Feature 2 — Medallion Pipeline (Bronze → Silver → Gold)](#feature-2--medallion-pipeline-bronze--silver--gold)
6. [Feature 3 — Quality Gate](#feature-3--quality-gate)
7. [Feature 4 — Dual Serving + Dual BI](#feature-4--dual-serving--dual-bi)
8. [Feature 5 — Self-Healing & Observability](#feature-5--self-healing--observability)
9. [Data Model](#data-model)
10. [Infrastructure & Resource Budget](#infrastructure--resource-budget)
11. [Non-Goals & Backlog](#non-goals--backlog)

---

## Executive Summary

**E-Commerce Crawler Pipeline** crawls Tokopedia products hourly, processes the data through a **Medallion Architecture** (bronze → silver → gold), validates quality automatically, and serves results to **two BI tools** (Metabase + Superset) via **two databases** (Postgres + ClickHouse). The pipeline is **idempotent** (safe to re-run), **config-driven** (no code changes to switch environments), and **self-monitoring** (audit trail + alerting + circuit breaker).

- **11 Docker services**, ~5.3 GB RAM
- **67 automated tests** (60 crawler unit + 7 pipeline integration)
- **5 quality checks** executed before data reaches the mart
- **23 crawl assets** managed via Streamlit UI, auto-dispatched by Airflow

---

## Problem Statement

A portfolio-grade data engineering project must demonstrate **production patterns** across the full data lifecycle:

1. **Ingestion** — Real-world API consumption with rate limiting and error handling
2. **Processing** — Medallion architecture (bronze/silver/gold) with schema enforcement
3. **Quality** — Automated validation that fails the pipeline before bad data reaches consumers
4. **Serving** — Multi-database, multi-BI-tool architecture demonstrating flexibility
5. **Operations** — Idempotency, observability, circuit breakers, and maintenance automation

This project satisfies all five requirements against **live Tokopedia data**, not synthetic datasets.

---

## Architecture Overview

```
Asset Registry (Postgres) ──► Airflow DAG (@hourly) ──► Crawler (httpx) ──► Tokopedia GraphQL API
                                                                                │
                                                                        Kafka (produce)
                                                                                │
                                                                    Spark Structured Streaming
                                                                                │
                                                                        BRONZE (Delta @ MinIO)
                                                                          │ Spark batch
                                                                        SILVER (Delta @ MinIO)
                                                                          │ Quality Gate (5 checks)
                                                                          │ dbt (DuckDB)
                                                                        GOLD (star schema)
                                                                         /          \
                                                          Postgres mart        ClickHouse serving
                                                              │                      │
                                                          Metabase              Superset
                                                              │                      │
                                                          Audit trail (pipeline_runs)
```

### Technology Stack

| Layer | Component | Version | Purpose |
|-------|-----------|---------|---------|
| Runtime | Python | 3.10+ | Async crawler + pipeline scripts |
| HTTP | httpx | 0.28 | Async GraphQL client |
| Queue | Apache Kafka | 7.5.0 (Confluent) | Decouple crawl from processing |
| Lakehouse | MinIO + Delta Lake | latest / 3.3.0 | S3-compatible storage, ACID transactions |
| Processing | Apache Spark | 3.5.4 | Structured Streaming (bronze) + batch (silver) |
| Transformation | dbt + DuckDB | 1.11 / 1.1 | SQL-based star schema, 7 data tests |
| Serving | Postgres + ClickHouse | 16 / 24.8 | Row-oriented mart + columnar analytics |
| BI | Metabase + Superset | 0.53 / latest | Drag-drop dashboards + advanced analytics |
| Orchestration | Apache Airflow | 2.10.4 | Hourly DAG, retries, alerting |
| Logging | loguru | 0.7 | Colored, structured output for crawler |
| Config | pydantic-settings | 2.14 | Env/YAML/.env layered config |
| Logging | loguru | 0.7 | Colored, structured; InterceptHandler captures stdlib |
| Linting | ruff | latest | line-length=120, rules: E,F,I,N,W |

---

## Feature 1 — Automated Crawling via Asset Registry

### Summary

Crawl targets live in a Postgres table (`control.crawl_assets`). Airflow reads the registry every hour, crawls due assets, and updates their status. Users add new targets via a Streamlit UI — **no code deploy, no Airflow restart**.

### Key Data Flow

1. **User adds a target** via Streamlit UI (`assets/app.py`) or seed YAML (`assets/seeds/targets.yaml`)
2. **Airflow DAG** (`tokopedia_products`) runs `pipeline/load/crawl_assets.py` every hour
3. **`v_due_assets` view** returns only active assets whose `last_crawled_at` exceeds their `cadence_min`
4. **Crawler** iterates over due assets, calls the Tokopedia GraphQL API, and publishes JSON events to Kafka
5. **Status update** — successful crawls call `mark_success()`; failures call `mark_failure()`

### Circuit Breaker

- **Threshold:** 5 consecutive failures
- **Action:** `is_active` is set to `false` automatically
- **Recovery:** Operator reactivates the asset via Streamlit UI or SQL
- **Rationale:** Prevents the pipeline from hammering a broken target indefinitely

### Schema

```sql
CREATE TABLE control.crawl_assets (
    asset_id             BIGSERIAL PRIMARY KEY,
    platform             TEXT NOT NULL DEFAULT 'tokopedia',
    crawl_type           TEXT NOT NULL,  -- search-product | search-shop | product-detail | product-reviews
    payload              JSONB NOT NULL,  -- {"keyword": "poco f8"} | {"url": "..."} | {"product_id": "..."}
    label                TEXT,            -- Human-readable name
    category             TEXT,            -- elektronik | fashion | ...
    priority             SMALLINT DEFAULT 5 CHECK (1-9),
    cadence_min          INT DEFAULT 60 CHECK (>=15),
    is_active            BOOLEAN DEFAULT true,
    consecutive_failures SMALLINT DEFAULT 0,
    last_crawled_at      TIMESTAMPTZ,
    last_status          TEXT             -- success | failed | blocked
);
```

### Crawler Types

| `crawl_type` | Required payload field | GraphQL operation |
|---|---|---|
| `search-product` | `keyword` | `SearchProductV5Query` |
| `search-shop` | `keyword` | `AceSearchShopQuery` |
| `product-detail` | `url` or `product_key`+`shop_domain` | `PDPMainInfo` |
| `product-reviews` | `product_id` | `productReviewList` |

---

## Feature 2 — Medallion Pipeline (Bronze → Silver → Gold)

### Summary

The pipeline implements the **Databricks Medallion Architecture**. Each layer has a distinct purpose, schema, and failure mode — and each can be replayed independently.

### Bronze — Raw Ingestion

| Property | Value |
|---|---|
| **Input** | Kafka topic `tokopedia.products.raw` |
| **Processing** | Spark Structured Streaming, `trigger(availableNow=True)` |
| **Output** | Delta table `s3a://lakehouse/bronze/products` |
| **Schema** | `value_json` (string), `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `ingested_at` |
| **Idempotency** | Delta checkpoint on MinIO prevents re-reading Kafka offsets |

**Design rationale:** `availableNow` trigger means Airflow invokes bronze as a batch task — no 24/7 streaming daemon required. The checkpoint guarantees exactly-once semantics across DAG runs.

### Silver — Cleaned & Typed

| Property | Value |
|---|---|
| **Input** | Bronze Delta table |
| **Processing** | Spark batch: `from_json()` → flatten → deduplicate |
| **Output** | Delta table `s3a://lakehouse/silver/products` |
| **Schema** | `product_id`, `product_name`, `price_idr`, `discount_pct`, `rating`, `shop_id`, `shop_name`, `shop_city`, `shop_tier`, `crawled_at` |
| **Rejects** | Unparseable rows → `s3a://lakehouse/silver/products_rejects` |

**Deduplication:** Partition by `(product_id, kafka_timestamp)`, take the row with the highest `kafka_offset`. Prevents duplicate products from the same crawl batch.

**Rejects quarantine:** Silver **never fails the job** on malformed data. Rows that fail JSON parsing or have no `product_id` go to `_rejects` for later inspection. This is the anti-silent-failure pattern.

### Gold — Star Schema

| Property | Value |
|---|---|
| **Input** | Silver Delta table |
| **Processing** | dbt on DuckDB: staging view → dedup → star schema |
| **Output** | 3 tables: `dim_product`, `dim_shop`, `fct_product_snapshot` |
| **Tests** | 7 dbt data tests: `unique` + `not_null` on all primary keys |

| Model | Type | Grain | Key |
|---|---|---|---|
| `dim_product` | Dimension | One row per product (latest state) | `product_id` |
| `dim_shop` | Dimension | One row per shop (latest state) | `shop_id` |
| `fct_product_snapshot` | Fact | One row per product per crawl | `snapshot_id = md5(product_id \|\| crawled_at)` |

---

## Feature 3 — Quality Gate

### Summary

**Five automated checks** run after silver, before dbt. Any single check failure returns exit code 1 → Airflow marks the task as `FAILED` → the pipeline stops **before** bad data reaches the mart.

### Check Suite

| # | Check | Rule | Failure impact |
|---|---|---|---|
| 1 | `row_count` | Silver must have > 0 rows | Pipeline halted (empty crawl detected) |
| 2 | `null_pct` | `product_id`, `price_idr`, `shop_id`, `product_name` each < 5% null | Pipeline halted (schema drift detected) |
| 3 | `price_positive` | All `price_idr` values > 0 | Pipeline halted (corrupted pricing detected) |
| 4 | `rejects_ratio` | Rejects / (silver + rejects) < 10% | Pipeline halted (silent failure detected) |
| 5 | `freshness` | `max(crawled_at)` < 2 hours ago | Pipeline halted (crawler stuck or API down) |

### Implementation

- **Location:** `pipeline/quality/checks.py`
- **Runtime:** Spark (`build_session("quality_check")`)
- **Exit code:** `0` = all pass, `1` = ≥1 failure
- **DAG position:** `silver >> quality_check >> dbt_build`

### Negative Test Results (Verified 2026-07-15)

| Test | Injected | Detected |
|---|---|---|
| Price = 0 for one product | Manual overwrite of silver Parquet | `price_positive` → FAIL |
| 50 corrupt JSON rows in bronze | Manual append to bronze Delta | `rejects_ratio` 14% → FAIL |

---

## Feature 4 — Dual Serving + Dual BI

### Summary

Gold data is loaded into **two databases** with different strengths, each connected to a **different BI tool**. Both databases read from the same DuckDB gold source — data is always consistent.

### Serving Layer

| Database | Engine | Loader | Idempotency strategy |
|---|---|---|---|
| **Postgres** | Row-oriented (OLTP) | `load_to_postgres.py` | `DROP TABLE` + `CREATE TABLE AS SELECT` |
| **ClickHouse** | Column-oriented (OLAP) | `load_to_clickhouse.py` | `DROP PARTITION` + `INSERT` (facts), `ReplacingMergeTree` (dims) |

**Design rationale:** Postgres serves operational queries and Metabase dashboards. ClickHouse serves analytical queries and Superset — **3–5× faster** for aggregations on the same star schema.

### BI Tools

| Tool | Port | Backend | Strengths |
|---|---|---|---|
| **Metabase** | 3000 | Postgres | 30-second setup, drag-and-drop builder, embed-friendly |
| **Apache Superset** | 8088 | ClickHouse | 50+ chart types, SQL Lab, native ClickHouse driver |

### Pre-built Dashboards

| Dashboard | Tables used | SQL reference |
|---|---|---|
| **US-1: Price Trend 30 Days** | `fct_product_snapshot` | Average/min/max price per day (line chart) |
| **US-2: Top Price Drops Today** | `fct_product_snapshot` + `dim_product` | Cheapest products today (table) |
| **US-3: Shop/City Comparison** | `fct_product_snapshot` + `dim_shop` | Product count per city, avg price (bar chart) |
| **Pipeline Health** | `pipeline_runs` | Rows per layer, rejects, duration over time |
| **Asset Health** | `control.crawl_assets` | Active vs inactive, failure rate per category |

All SQL queries are documented in `dashboards/dashboards.sql` — dual dialect (Postgres + ClickHouse).

---

## Feature 5 — Self-Healing & Observability

### Summary

The pipeline recovers from transient failures, prevents cascading damage, and provides full visibility into every execution.

### Idempotency

| Layer | Strategy | Rollback-free |
|---|---|---|
| Bronze | Delta checkpoint (exactly-once) | ✅ |
| Silver | `mode("overwrite")` — full rebuild | ✅ |
| Gold | dbt `table` materialization (CREATE OR REPLACE) | ✅ |
| Postgres | `DROP TABLE` + `CREATE TABLE AS SELECT` | ✅ |
| ClickHouse facts | `DROP PARTITION` + `INSERT` | ✅ |
| ClickHouse dims | `ReplacingMergeTree` + `OPTIMIZE FINAL` | ✅ |

### Circuit Breaker

- **Scope:** Per-asset (column `consecutive_failures`)
- **Threshold:** 5 consecutive failures
- **Action:** `is_active` → `false`, asset skipped in future DAG runs
- **Recovery:** Manual reactivation via Streamlit UI or `UPDATE control.crawl_assets SET is_active=true`

### Audit Trail

- **Table:** `analytics.pipeline_runs` (ClickHouse, `ReplacingMergeTree`)
- **Schema:** `run_id`, `execution_date`, `status`, `rows_silver`, `rows_rejects`, `rows_gold`, `duration_sec`, `inserted_at`
- **Writer:** `pipeline/quality/audit.py` — called by the DAG's `write_audit` task with `trigger_rule="all_done"`
- **Consumers:** Pipeline Health dashboard, failure trend analysis

### Alerting

- **Trigger:** `on_failure_callback` on the `tokopedia_products` DAG
- **Implementation:** `pipeline/airflow/alerting.py` — standard library `urllib`, zero dependencies
- **Protocols:** Telegram bot, Discord webhook, Slack webhook, ntfy.sh
- **Configuration:** `ALERT_WEBHOOK_URL` environment variable (no-op if unset)

### Scheduled Maintenance

- **DAG:** `lakehouse_maintenance` (`@weekly`)
- **Actions:**
  - `OPTIMIZE` + `VACUUM` bronze Delta table
  - `OPTIMIZE` + `VACUUM` silver Delta table
  - `OPTIMIZE FINAL` on ClickHouse `dim_product` and `dim_shop` (ReplacingMergeTree deduplication)

---

## Data Model

### Star Schema (Gold / Mart)

```
dim_product (1) ──► (N) fct_product_snapshot (N) ◄── (1) dim_shop
```

**dim_product** — Latest product attributes

| Column | Type | Key | Description |
|---|---|---|---|
| `product_id` | VARCHAR | PK | Tokopedia product ID |
| `product_name` | VARCHAR | | Product display name |
| `product_url` | VARCHAR | | Product page URL |
| `shop_id` | VARCHAR | FK | Owning shop |
| `last_seen_at` | TIMESTAMP | | Most recent crawl time |

**dim_shop** — Latest shop attributes

| Column | Type | Key | Description |
|---|---|---|---|
| `shop_id` | VARCHAR | PK | Tokopedia shop ID |
| `shop_name` | VARCHAR | | Shop display name |
| `shop_city` | VARCHAR | | Shop location city |
| `shop_tier` | INT | | 1=Official Store, 2=Gold Merchant |
| `last_seen_at` | TIMESTAMP | | Most recent crawl time |

**fct_product_snapshot** — Price snapshot per crawl

| Column | Type | Key | Description |
|---|---|---|---|
| `snapshot_id` | VARCHAR | PK | `md5(product_id \|\| crawled_at)` |
| `product_id` | VARCHAR | FK | References `dim_product` |
| `shop_id` | VARCHAR | FK | References `dim_shop` |
| `price_idr` | BIGINT | | Price in Indonesian Rupiah |
| `discount_pct` | INT | | Discount percentage |
| `rating` | DOUBLE | | Product rating (0.0–5.0) |
| `crawled_at` | TIMESTAMP | | Crawl timestamp |

---

## Infrastructure & Resource Budget

### Docker Services

| Service | Image | Port | RAM | Role |
|---|---|---|---|---|
| Zookeeper | `confluentinc/cp-zookeeper:7.5.0` | 2181 | 80 MB | Kafka coordination |
| Kafka | `confluentinc/cp-kafka:7.5.0` | 9092 | 220 MB | Message queue |
| Elasticsearch | `elasticsearch:8.12.0` | 9200 | 970 MB | Optional real-time sink |
| Kibana | `kibana:8.12.0` | 5601 | 540 MB | ES visualization |
| MinIO | `minio/minio:latest` | 9000-9001 | 180 MB | S3-compatible lakehouse |
| Postgres | `postgres:16` | 5433 | 135 MB | Mart + asset registry |
| ClickHouse | `clickhouse/clickhouse-server:24.8` | 8123 | 690 MB | Columnar analytics |
| Airflow | Custom (`apache/airflow:2.10.4`) | 8080 | 1.4 GB | Orchestration |
| Metabase | `metabase/metabase:v0.53.5` | 3000 | 830 MB | BI (Postgres backend) |
| Superset | `apache/superset:latest` | 8088 | 225 MB | BI (ClickHouse backend) |
| **Total (11 services)** | | | **~5.3 GB / 7.6 GB** | |

### CI/CD

- **Platform:** GitHub Actions
- **Triggers:** Push to `main`, pull request to `main`
- **Steps:** ruff check → pytest (`source/tests/`)
- **Linter:** ruff (`line-length=120`, rules: E, F, I, N, W)

---

---

## BI Setup Quick Reference

### Metabase (Postgres mart)

| Item | Value |
|---|---|
| URL | `http://localhost:3000` |
| Login | `admin@tokocrawl.local` / `admin12345` |
| Database | Postgres Mart (host=`postgres`, port=5432, db=`mart`) |
| Guide | `dashboards/metabase_guide.md` — 5 dashboard step-by-step |

### Superset (ClickHouse)

| Item | Value |
|---|---|
| URL | `http://localhost:8088` |
| Login | `admin` / `admin` |
| Database | ClickHouse Analytics (`clickhousedb://ch_user:ch_pass@clickhouse:8123/analytics`) |
| Driver | `clickhouse-connect` (installed manually via `pip install` in venv) |
| Guide | `dashboards/superset_guide.md` — 5 dashboard step-by-step (SQL Lab) |

### Setup Script
```bash
# Run once from host machine after docker compose up
pip install requests && python dashboards/setup_all.py
```

---

## Production Readiness

### What's already production-grade
- Idempotent at every layer (re-run without duplicates)
- Config-driven (no hardcoded URLs/topics/credentials in code)
- Circuit breaker (auto-disable failing assets)
- Quality gate with 5 validations that fail the pipeline before bad data reaches consumers
- Audit trail (`pipeline_runs`) for every DAG execution
- Retry on transient failures (1 retry, 2 min delay)
- Weekly maintenance automation (OPTIMIZE + VACUUM)

### What's needed for production (see PRD_60)
| Item | Priority | Reference |
|---|---|---|
| Monitoring (Prometheus + Grafana) | P0 | FR-30..FR-32 |
| Secret management (Vault/Secrets Manager) | P0 | FR-33..FR-35 |
| CI/CD pipeline (build → test → deploy) | P0 | FR-36..FR-38 |
| Disaster recovery (backup + RTO 4h) | P0 | FR-48..FR-49 |
| Data retention policy | P0 | FR-39..FR-40 |
| Incremental silver processing | P1 | FR-42..FR-44 |
| TLS/SSL on all endpoints | P1 | FR-45..FR-47 |
| Kubernetes migration | P2 | FR-50 |

Full PRD: `prd-sharded/PRD_60_Production_Hardening.md`

---

## Non-Goals & Backlog

The following items were evaluated and **deferred** — they add complexity without proportional portfolio value at this stage.

| Item | Rationale for deferral |
|---|---|
| **AWS S3 migration** | Requires AWS account + billing setup. Config-driven architecture already supports it. |
| **Beanstalkd job queue** | MinIO checkpoint already provides exactly-once; Beanstalkd adds a second queue without clear benefit at <100 assets. |
| **SCD Type 2 for dim_product** | Current `ReplacingMergeTree` pattern serves the portfolio use case. SCD Type 2 adds ETL complexity without changing dashboard outcomes. |
| **Product-detail tracking (FR-11)** | Crawler already supports it; adding pipeline integration requires per-type silver schemas. Deferred until a second crawler type feeds the medallion. |
| **Elasticsearch full-text search (FR-12)** | ES driver is wired; the DAG doesn't route data to it. Deferred until a search demo is specifically needed. |
| **Price drop Telegram alert (feature)** | Infrastructure alerting is in place; feature-level alerts require business logic (thresholds, per-user config). |
