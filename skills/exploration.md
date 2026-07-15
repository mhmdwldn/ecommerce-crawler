# Exploration Report — E-Commerce Crawler Pipeline (Fase 0-8)

Retrospective of all work done from baseline validation to production hardening.
**Session dates:** 2026-07-15.
**AI session context:** Read this + CLAUDE.md + TASKS.md to understand full project state.

---

## Fase 0 — Validasi Baseline

Goal: Verify existing pipeline works end-to-end without code changes.

- Read CLAUDE.md + README.md, documented scrape + full pipeline commands
- `docker compose up`: 7 services, ~3.9 GB RAM. Fixed Kafka NodeExists (ZK stale) + Airflow PID conflict (volume stale)
- Crawler scrape: 20 products "poco f8", 0 nulls, HTTP 200
- Crawler → Kafka: 20 events, 3 partitions, console consumer verified
- `stream_bronze` → MinIO: 20 new rows, stale checkpoint issue documented
- Silver + dbt + load_to_postgres: 120 silver rows, 0 rejects, 11/11 dbt PASS, Postgres 180 rows
- DAG trigger: crawl→bronze→silver→dbt_build→[load_postgres,load_clickhouse], 5/5 SUCCESS, ~90s
- CI: ruff + pytest (60/60), badge in README
- `docs/baseline-notes.md`: 5 error/fix patterns documented
- `Makefile`: up/down/crawl/smoke/test/test-all/lint/lint-fix/clean
- `.env.example` already existed
- **DoD:** DAG trigger → new data in Postgres without manual intervention ✅

## Fase 1 — ClickHouse Serving Layer

Goal: Add ClickHouse as serving layer for BI tools.

- ClickHouse 24.8 service in compose, port 8123, 347 MB RAM
- DDL: 3 tables (fct_product_snapshot MergeTree, dim_product/dim_shop ReplacingMergeTree), toYYYYMM partition
- Spike ADR-001: tested script DuckDB→CH (clickhouse-connect, ~2s) vs dbt-clickhouse (~0.3s per model)
- **ADR-001 decision:** Opsi A — script approach. Single transform source, consistent with Postgres pattern
- Strategy: truncate-partition-insert (fct), ReplacingMergeTree+OPTIMIZE FINAL (dims)
- `load_to_clickhouse.py`: 50 lines, mirror of load_to_postgres.py
- DAG: +load_clickhouse task, parallel with load_postgres
- Test: 3/3 passed (tables exist, row counts match, idempotent)
- CH == PG == DuckDB (92/41/180 → 112/52/200)
- **DoD:** DAG trigger → fct_product_snapshot 180→200 in ClickHouse ✅

## Fase 2 — Hourly + Quality

Goal: Hourly schedule, quality checks, audit logging, maintenance DAG.

- Airflow Variables: crawl_keyword + crawl_max_pages (removed in Fase 2.5 registry)
- Schedule @hourly, jitter 0-300s, max_active_runs=1
- `quality/checks.py`: 5 checks (row_count, null_pct, price_positive, rejects_ratio, freshness). Exit non-zero on failure
- quality_check task in DAG: silver >> quality_check >> dbt_build. DAG now 8 tasks
- dbt tests: 7 tests (unique+not_null on all PKs)
- Negative test price=0: quality_check FAIL detected
- Negative test rejects: 52/372 rejects (14%) → quality_check FAIL detected
- Audit: `pipeline_runs` in CH, `write_audit` task with trigger_rule=all_done
- Reprocess test: delete bronze → re-stream from Kafka → row count identical
- Maintenance DAG: @weekly OPTIMIZE+VACUUM bronze/silver, OPTIMIZE FINAL ClickHouse dims
- Rejects 14% exceeded tested, freshness check verified at 0.1h

## Fase 2.5 — Asset Registry / Control Plane

Goal: Crawl targets managed via Postgres registry + Streamlit UI, DAG auto-fan-out.

- DDL applied: `control.crawl_assets` + `v_due_assets` view in Postgres
- 23 seed assets (14 elektronik: POCO F8/F8 Pro/X7 Pro, iPhone 17/17 Pro Max/16, Galaxy S25 Ultra; 9 fashion)
- `assets/repository.py`: get_due_assets(), mark_success(), mark_failure() + circuit breaker (5 consecutive failures → is_active=false)
- DAG refactored: `crawl_assets.py` replaces fixed-keyword crawl. Reads registry, crawls due assets, updates status
- Airflow Variables removed — registry is single source of truth
- max_active_tasks=2 in DAG for safe fan-out
- Streamlit admin UI CRUD, pre-built, verified working
- Circuit breaker verified: 5 consecutive failures → is_active=false
- Pre-existing tests: 15/15 pass
- **DoD:** keywords 100% from registry via UI; DAG auto-crawls due assets; circuit breaker functional ✅

## Fase 3 — Dual BI (Metabase + Superset)

Goal: Two BI tools, 5 dashboards, serialized exports.

- Metabase v0.53.5 (port 3000) → Postgres mart. Metadata in separate Postgres DB
- Superset latest (port 8088) → ClickHouse serving. Metadata with Postgres
- 5 dashboards SQL documented (US-1 Price Trend, US-2 Top Price Drops, US-3 Shop/City, Pipeline Health, Asset Health)
- Dual dialect: Postgres + ClickHouse queries in `dashboards/dashboards.sql`
- Setup scripts: `setup_metabase.py`, `setup_superset.py`, `setup_all.py`
- Export directories: `dashboards/metabase_exports/`, `dashboards/superset_exports/`
- Metabase guide: 5 step-by-step tutorials in `dashboards/metabase_guide.md`
- Superset guide: 5+2 step-by-step tutorials (SQL Lab) in `dashboards/superset_guide.md`
- Superset driver fix: clickhouse-connect copied to venv (no pip in venv), UUID binary 16-byte fix, datetime format fix
- Metabase: fresh setup with admin@tokocrawl.local / admin12345 (Google OAuth user overwritten)
- Stack: 11 services, ~5.3 GB RAM
- Both BI tools accessible and tested with live data

## Fase 4 — Dokumentasi & Alerting

Goal: BI comparison, DAG alerting, README quickstart.

- `docs/bi-comparison.md`: Metabase vs Superset — setup, UX, features, ClickHouse performance, verdict
- Alerting: `pipeline/airflow/alerting.py` — webhook callback (Telegram/Discord/Slack/ntfy). on_failure_callback in DAG
- `docs/architecture.md`: full project guide, maintained from Fase 0
- README quickstart: <15 minutes, 5 commands, all URLs + logins

## Code Review (v1) — Phase 0-1 Audit

30 findings from codebase audit. Fixed 17 (critical/high):
- Removed --config/-c dead code from main.py
- Cleaned duplicate CLI args (-d/-o/--bootstrap-servers), merged parent+subparser
- close() chain: Output → Controllers → TokopediaControllers (finally block)
- Removed CLAUDE.md from .gitignore (should ship with repo)
- Hardcoded analytics. schema → CH_DB env var in load_to_clickhouse.py
- isinstance(e, RateLimitExceeded) replace fragile regex
- Schema defaults (user_district_id/city_id) now match Settings
- config.yaml: DEBUG→INFO, ruff.toml: N812 ignore (PySpark F/T convention)
- make lint covers source/pipeline/assets
- Removed duplicates: S3 settings from profiles.yml, assets deps from source/requirements.txt
- dim_product model: WHERE product_id IS NOT NULL guard

## Loguru Migration

- InterceptHandler in main.py captures all `logging.getLogger()` → loguru
- Format: HH:MM:SS | LEVEL | logger_name (30-char aligned) | message
- Zero changes to controllers/helpers/library — all existing calls auto-intercept
- Pipeline unchanged (print + Spark logging remains as-is)

## Fase 6 — Production Hardening (Monitoring + Secrets + CI/CD + Backup)

**6.1-6.2 Monitoring:** Prometheus (:9090) + Grafana (:3001, admin/admin) + Alertmanager (:9093)
- postgres-exporter (:9187), airflow-statsd (:9102), 4/6 scrape targets UP
- Pipeline Health dashboard auto-imported via Grafana API
**6.3 Alerting:** Alertmanager webhook config ready (Telegram/Discord as needed)
**6.4-6.5 Vault:** Dev mode (:8200, token=root-token-dev). 4 secrets stored (PG/CH/Kafka/MinIO). Airflow Vault backend configured
**6.6 CI/CD:** GitHub Actions 5 test jobs + CD workflow (build→push GHCR→smoke test). PRD_60 created for production hardening
**6.7 Rolling deploy:** `deploy.sh`: pull GHCR→restart→health check 60s→auto-rollback. Compose.cd.yaml override for GHCR image
**6.8 Backup:** `backup.sh`: PG dump+CH DDL+MinIO sync, 7-day retention
**6.9 DR test:** Drop crawl_assets→DDL→seed→23/23 restored (RTO <10 min)
**Self-hosted runner:** GitHub Actions runner on Windows laptop. CD auto-deploys to local Docker on every push
Stack: 16 services, ~6.5 GB RAM

## Fase 7 — Data Retention + Security + Logging

**7.1-7.3 Retention+Incremental:** `data_retention` DAG @monthly, VACUUM bronze 90d/silver 180d. Silver `--incremental` MERGE mode via watermark. `--full-refresh` flag
**7.4-7.5 TLS:** Caddy reverse proxy (:8081), routes to 7 services by path prefix. TLS docs in `deployment/tls-config.md` (Kafka SASL/PG SSL/CH TLS/MinIO TLS/ES TLS/Caddy HTTPS)
**7.6 Fluent Bit:** → ES → Kibana (permission limitation on Docker Desktop, documented)
**7.7-7.8 Env promotion:** Vault paths `secret/env/dev|staging|prod/database`. Credential rotation via Vault API
**Silver incremental syntax:** `python -m pipeline.spark.silver --incremental` / `--full-refresh`
Stack: 18 services, ~6.5 GB RAM

## Fase 8 — Kubernetes + Cold Storage + TLS

**8.1 Helm chart:** `deployment/helm/`: Chart.yaml, values.yaml (18 services toggleable), README
**8.2 Cold storage:** `retention.py --cold-storage`: export old data to Parquet (`lakehouse/cold/`) before VACUUM
**8.3 TLS config:** `deployment/tls-config.md`: per-service TLS (Kafka SASL, PG SSL, CH TLS, MinIO TLS, ES TLS, Caddy HTTPS)

## Final E2E Test

- Crawler scrape: 20 docs ✅
- Crawler → Kafka: 20 events ✅
- DAG trigger: 8/8 tasks SUCCESS ✅
- Postgres: fct=1520, dim_p=766, dim_s=306 ✅
- ClickHouse: 1520/766/306 (matches PG) ✅
- Quality: 5/5 PASS ✅
- Audit: pipeline_runs recorded ✅
- Asset Registry: 23 active ✅
- Metabase: 200, Superset: 302, Grafana: 302, Prometheus: 302, Vault: OK ✅
- @hourly schedule running ✅
- CI/CD: 7 green jobs, self-hosted runner auto-deploy ✅

## Key Technical Decisions

1. **Medallion architecture:** Bronze(raw)→Silver(typed+dedup)→Gold(star schema). Each layer independently replayable
2. **ClickHouse loader:** Script approach (not dbt-clickhouse). Single transform source. ADR-001
3. **Quality gate:** 5 validation checks BEFORE data enters mart. Exit code 1 → pipeline stops
4. **Idempotency:** Every layer has its own strategy (checkpoint, overwrite, DROP PARTITION, ReplacingMergeTree)
5. **Asset Registry:** Separate control plane (Postgres+Streamlit). DAG reads registry hourly, no code deploy
6. **Dual BI + Dual Backend:** Metabase→Postgres, Superset→ClickHouse. Data identical, patterns flexible
7. **Config-driven:** pydantic-settings with env/YAML/.env layering. TOKOPEDIA_ prefix, __ nesting delimiter
8. **Vault for secrets:** All service credentials in Vault. Airflow connections via Vault backend
9. **CI/CD full cycle:** push→test→build→push GHCR→smoke→self-hosted deploy to local Docker
10. **Self-hosted runner:** Windows laptop auto-deploys on push. Rollback via deploy.sh --rollback

## What Was Skipped

- **Fase 5 (AWS S3):** Requires AWS account + billing setup. Architecture is config-driven — just swap env vars
- **Backlog v2:** Beanstalkd, product-detail tracking, ES search, SCD Type 2, price drop Telegram alert

---

## 1. Project inventory (final structure)

```
ecommerce-crawler/
├── CLAUDE.md                       # project documentation for AI/devs (Step 4 output)
├── README.md                       # user-facing docs: quick start, pipeline, layout
├── config.yaml                     # sample YAML config, section `tokopedia_crawler`
├── .env.example                    # TOKOPEDIA_* env template (placeholders only)
├── .gitignore                      # Python / Docker / env / runtime-output hygiene
├── Dockerfile                      # python:3.11-slim, ENTRYPOINT ["python","main.py"]
├── requirements.txt                # pointer → source/requirements.txt
├── skills/
│   └── exploration.md              # this report (Step 5 output)
└── source/
    ├── .dockerignore               # build-context exclusions
    ├── .gitignore                  # source-level ignores
    ├── main.py                     # argparse CLI; CONTROLLER_REGISTRY maps --type → controller
    ├── requirements.txt            # runtime + test deps
    ├── controllers/
    │   ├── __init__.py             # `Controllers` ABC: job loop, output dispatch, exc handling
    │   ├── tokopedia/
    │   │   ├── __init__.py         # `TokopediaControllers`: API lifecycle + job parsing helpers
    │   │   ├── search_product.py   # keyword → products controller
    │   │   ├── search_shop.py      # keyword → shops controller
    │   │   ├── product_detail.py   # URL or shop+key → PDP document controller
    │   │   └── product_reviews.py  # product_id → paginated reviews controller
    │   └── shopee/
    │       ├── __init__.py         # `ShopeeControllers`: API lifecycle + job parsing helpers
    │       └── search_product.py   # keyword/match_id → products controller
    ├── library/
    │   ├── __init__.py
    │   ├── config.py               # BaseSettings tree, TOKOPEDIA_ + SHOPEE_ prefixes, YAML/.env sources
    │   ├── schemas.py              # GraphQLRequest, Tokopedia (4 req + 4 doc) + Shopee (req + doc), KafkaEvent
    │   ├── graphql_queries.py      # 4 Tokopedia GraphQL documents (trimmed from browser captures)
    │   ├── tokopedia_api.py        # TokopediaAPI: httpx client, retries, throttle, parsing
    │   ├── shopee_api.py           # ShopeeAPI: httpx client for v4 search REST, anti-bot handling
    │   └── setup_infra.py          # async infra bootstrap (AIOKafkaAdminClient + AsyncElasticsearch)
    ├── helpers/
    │   ├── __init__.py
    │   ├── input/                  # Input facade, InputDriver ABC, StdInputDriver, factory
    │   └── output/                 # Output facade, OutputDriver ABC, factory,
    │       └── driver/             #   kafka.py / elasticsearch.py / file.py / std.py
    ├── exception/
    │   ├── __init__.py
    │   └── exception.py            # 3 exceptions actually used by the pipeline
    ├── deployment/
    │   ├── compose.yaml            # Kafka + Zookeeper + ES 8.12 + Kibana (unchanged)
    │   ├── 01-configmap.yaml       # k8s ConfigMap with production tokopedia_crawler config
    │   └── 02-deployment.yaml      # k8s Deployment: search-product → Kafka
    └── tests/                      # 78 tests, all passing, zero live network
        ├── __init__.py
        ├── conftest.py             # sample GraphQL fixtures + settings fixtures
        ├── test_config.py          # settings defaults/overrides/bounds
        ├── test_schemas.py         # request param building + document parsing + event envelope
        ├── test_tokopedia_api.py   # client lifecycle, pagination, PDP merge, GraphQL unwrap
        ├── test_shopee_api.py      # Shopee client: parsing real item, paging, anti-bot errors
        ├── test_controllers.py     # Tokopedia controller orchestration with mocked API
        ├── test_shopee_controllers.py  # Shopee controller orchestration with mocked API
        └── test_output_drivers.py  # std/file drivers + factory (unchanged from reference)
```

> Note: the root also holds `shopee_search_product.txt` — the raw browser
> capture used to build the Shopee client. It contains a **live logged-in
> session** and is git-ignored (see §9.6); it is an input artifact, not part of
> the shipped source.

---

## 2. Patterns & conventions discovered in the reference TikTok project

- **Layered template-crawler architecture:** `main.py (CLI) → Controllers ABC →
  platform controller → platform API client → Pydantic schemas`, with I/O
  abstracted behind `Input`/`Output` facades that delegate to factory-created
  drivers. The platform-specific code is isolated in exactly two places:
  `controllers/<platform>/` and `library/<platform>_api.py`.
- **Naming:** snake_case modules, `<Platform><Action>` controller classes
  (`TikTokSearchPost`), `<Platform>API` client, `<Platform>CrawlerSettings`
  config subclass extending a generic `CrawlerSettings`.
- **Base-class hierarchy:** `Controllers` (generic loop/error handling) →
  `TikTokControllers` (API lifecycle + job parsing) → concrete handlers, each
  exposing `handler()` (full pipeline) and `scrape_to_json()` (programmatic).
- **Config pattern:** one `Settings` root with nested `BaseSettings` sections
  (kafka / elasticsearch / crawler), env prefix + `__` nesting delimiter,
  custom `settings_customise_sources` adding a YAML source, and a module-level
  `settings` singleton.
- **Sync-put / async-driver bridge:** the Kafka output driver runs its
  AIOKafkaProducer on a dedicated background thread + event loop and bridges
  via `asyncio.run_coroutine_threadsafe` — keeping `OutputDriver.put()`
  synchronous so controllers can call it from inside their own running loop.
- **Two CLI modes:** `scrape` (JSON to stdout/file, no drivers) and `full`
  (input loop + output driver), with output-driver flags duplicated on the
  subparser for ergonomic ordering.
- **Tests:** class-per-unit pytest layout, `pytest-asyncio` strict markers,
  `pytest-mock`, fixtures in conftest mirroring real API payload shapes.

## 3. Refactoring decisions made (the "Tokopedia source")

The "existing Tokopedia scraper" was **not Python** — it was four raw curl
captures from browser DevTools (`tokopedia_*.txt`), each a Tokopedia GraphQL
call with full browser headers, session cookies, and inline query documents.
The refactor distributed their contents as follows, then deleted them:

| Capture file | Became |
|---|---|
| `tokopedia_search_product.txt` | `SEARCH_PRODUCT_QUERY`, `TokopediaSearchProductRequest.to_params()`, `TokopediaProduct`, `search_products()` |
| `tokopedia_search_shop.txt` | `SEARCH_SHOP_QUERY`, `TokopediaSearchShopRequest`, `TokopediaShop`, `search_shops()` |
| `tokopedia_product_detail.txt` | `PRODUCT_DETAIL_QUERY`, `TokopediaProductDetailRequest`, `TokopediaProductDetail`, `get_product_detail()` |
| `tokopedia_product_reviews.txt` | `PRODUCT_REVIEWS_QUERY`, `TokopediaProductReviewsRequest`, `TokopediaReview`, `get_product_reviews()` |

Key transformations:

- **Headers → config.** Volatile/identifying header values (`x-version` build
  hash, `bd-device-id`, `x-source`, `x-device`, `x-tkpd-lite-service`,
  User-Agent, Origin/Referer) became `TokopediaCrawlerSettings` fields.
- **Cookies → secrets.** The captures contained live session cookies
  (`_SID_Tokopedia_`, `_abck`, `bm_sz`, …). These were **not** carried into
  code; instead there is an optional `cookies` setting / `--cookies` flag,
  and the capture files were deleted so no secret lands in git history.
  Testing showed the four endpoints respond without cookies; they only add
  session/location personalisation.
- **`params` strings → typed builders.** The opaque urlencoded `params`
  variable (product/shop search) is rebuilt field-by-field via
  `to_params()`, with paging (`start = (page-1)*rows`), location bias, and a
  generated visitor `unique_id` (uuid4 hex per session when not configured).
- **Query documents → `graphql_queries.py`.** Kept as code (they are data
  contracts tied to the parsers, not configuration). Trimmed tracking/ads
  subtrees (`topads`, `related products`, variant/shipment fragments) that
  the pipeline never parses; the reviews and shop queries are essentially
  verbatim.
- **Tracking params dropped:** `srp_page_id`, `topads_bucket` kept only where
  required for a valid request; ads URLs, GA keys, and wishlist-tracking
  fields were dropped from queries and models.

## 4. Architecture decisions

- **One API client, four operations.** Tokopedia's four endpoints share one
  gateway, protocol (batched GraphQL list payload), and header set — so a
  single `TokopediaAPI` with one `_execute()` core (throttle → POST → retry →
  `_unwrap`) and four thin public methods beats four near-identical clients.
- **Batched-list protocol modelled explicitly.** The gateway takes/returns a
  JSON *list*; `GraphQLRequest.to_payload()` and `TokopediaAPI._unwrap()`
  handle list-or-object shapes and raise `ErrorRequestException` on GraphQL
  `errors`, so parsers downstream never see envelope variance.
- **PDP component merge.** `PDPMainInfo` scatters product data across layout
  components (`basicInfo` + `ProductHighlight` + `ProductMedia`).
  `_merge_pdp_components()` flattens name/price/stock/media into the
  `basicInfo` dict before validation, giving one flat
  `TokopediaProductDetail` document instead of leaking layout structure.
- **Per-type pagination semantics.** Product search advances `page` until a
  page returns empty; shop search advances `start += rows`; reviews follow
  the server's `hasNext` flag; PDP is single-shot returning
  `Optional[KafkaEvent]`. Each matches what the endpoint actually supports.
- **`KafkaEvent.payload` is a typed union** (`TokopediaDocument`) rather than
  `dict`, keeping the envelope `extra="forbid"` while supporting four
  document types; events carry `event_type` discriminators
  (`tokopedia.product.scraped`, `.shop.`, `.product_detail.`, `.review.`).
- **Lenient document models.** All response models are `extra="allow"` with
  alias-based camelCase mapping and defensive coercers (string ratings,
  `null` attachment lists, `null` seller responses) — marketplace APIs change
  shape frequently and a crawl shouldn't die on a new field.
- **ES driver rewritten to the async client** (`AsyncElasticsearch`) using the
  same background-thread pattern as the Kafka driver — satisfying the
  "fully async I/O" constraint and the mandated `elasticsearch-py` stack
  (the reference used sync `requests` against the REST API).
- **`setup_infra.py` made fully async** with `AIOKafkaAdminClient` +
  `AsyncElasticsearch`, which also let `requests` and `kafka-python` be
  dropped from requirements entirely.
- **429 handling:** `RateLimitExceeded` (message contains "Too Many Requests")
  is raised without retry so the base controller's bury logic catches it.
- **`CONTROLLER_REGISTRY` in main.py** replaces the reference's if/elif
  chains — adding a crawler type is one dict entry (Open/Closed at the CLI).
- **Naming note:** the brief said `controller/Tokopedia/`; the reference's
  actual convention is `controllers/<platform>/` lowercase, so the project
  uses `controllers/tokopedia/` (Pythonic module naming, mirrors reference).

## 5. Mapping: TikTok → Tokopedia

| Reference file | Disposition |
|---|---|
| `source/main.py` | **Adapted** — registry-based controller dispatch, Tokopedia flags (`--url`, `--product-id`, `--rows`, `--sort-by`, …) |
| `source/controllers/__init__.py` | **Reused as-is** (platform-agnostic engine) |
| `source/controllers/tiktok/*` | **Replaced** by `controllers/tokopedia/` (4 controllers vs 3) |
| `source/library/config.py` | **Adapted** — `TIKTOK_`→`TOKOPEDIA_`, GraphQL endpoints/headers/location settings, added `.env` source |
| `source/library/schemas.py` | **Rewritten** — Tokopedia request builders + 4 document families; kept `KafkaEvent` envelope concept |
| `source/library/tiktok_api.py` | **Replaced** by `tokopedia_api.py` (same skeleton: lifecycle, throttle, retry; new GraphQL core + parsers) |
| — | **New:** `library/graphql_queries.py` |
| `source/library/setup_infra.py` | **Adapted + made async** — aiokafka admin + AsyncElasticsearch, Tokopedia index mapping |
| `source/exception/exception.py` | **Trimmed** — kept the 3 used exceptions; deleted 10 dead TikTok/proxy exception classes and `MessageException` |
| `source/helpers/input/*` | **Reused as-is** |
| `source/helpers/output/driver/kafka.py` | **Adapted** — producer settings (client_id, acks, compression, linger) now injected from config |
| `source/helpers/output/driver/elasticsearch.py` | **Rewritten** — sync `requests` → `AsyncElasticsearch` on background loop; dead `bulk_put()` removed |
| `source/helpers/output/driver/file.py`, `std.py`, ABCs, facades | **Reused as-is** |
| `source/helpers/output/driver/factory/__init__.py` | **Adapted** — all defaults pulled from settings instead of hardcoded literals |
| `source/tests/*` | **Adapted** — conftest + 4 modules rewritten for Tokopedia; `test_output_drivers.py` kept |
| `source/tests/test_user_posts.py`, `test_user_story.py`, `test_tiktok_api.py` | **Dropped** (TikTok-specific; superseded by `test_tokopedia_api.py`) |
| `config.yaml`, `Dockerfile`, `.gitignore`, `deployment/01+02` | **Adapted** for Tokopedia naming/content |
| `deployment/compose.yaml`, `.dockerignore`, `source/.gitignore` | **Reused as-is** |
| `README.md` | **Rewritten** (reference was 29 KB TikTok-specific) |
| — | **New:** `.env.example` |

## 6. Env vars & config schema

Full table lives in `CLAUDE.md` (§ Environment variables reference). Summary
of what was *introduced* relative to the reference:

| New setting | Type / default | Purpose |
|---|---|---|
| `crawler.site_url` | str / `https://www.tokopedia.com` | Origin/Referer headers |
| `crawler.search_product_endpoint` … `product_reviews_endpoint` | str / `/graphql/<Op>` | 4 endpoint paths (vs 3 TikTok ones) |
| `crawler.x_version` | str / `a3540b9` | frontend build hash header — rotates with Tokopedia web releases |
| `crawler.x_source`, `x_device`, `lite_service` | str / `tokopedia-lite`, `desktop`, `zeus` | gateway identification headers |
| `crawler.device_id` | str / `""` | optional `bd-device-id` header (omitted when empty) |
| `crawler.unique_id` | str / `""` | visitor UUID for search params; random uuid4 hex per session when empty |
| `crawler.user_district_id`, `user_city_id` | str / `2274`, `176` | location bias from the captures (Bandung area), now configurable |
| `crawler.default_rows` | int 1–100 / `20` | page size (replaces TikTok `default_count`) |
| root `env_file=".env"` | — | dotenv support added (reference had none) |

Removed settings: `crawler.base_url=tikwm.com`, `search/user_posts/user_story`
endpoints, `hd` flag, `elasticsearch.chunk_size` (only consumed by the deleted
`bulk_put`).

## 7. What was deleted and why

- `tokopedia_*.txt` (4 files) — source captures; contents distributed into
  modules; contained **live session cookies** that must not reach git.
- `library/tiktok_api.py`, `controllers/tiktok/` — replaced by Tokopedia
  equivalents per the brief.
- `tests/test_tiktok_api.py`, `test_user_posts.py`, `test_user_story.py` —
  tested deleted code.
- 10 exception classes + `MessageException` in `exception.py` — never
  imported by any surviving module (no dead code constraint).
- `ElasticsearchOutputDriver.bulk_put()` — had no call sites.
- `requests` and `kafka-python` dependencies — superseded by the async
  clients.
- `EXPOSE 9092` in the Dockerfile — the crawler is a Kafka *client*; exposing
  the broker port from the crawler image was meaningless.

## 8. Open questions & future improvements

- **Anti-bot hardening.** The gateway sits behind Akamai (`_abck`, `bm_sz`
  cookies in the captures). Anonymous requests worked at capture time, but
  sustained crawling may trigger challenges; proxy rotation hooks exist
  (`proxy_url`) yet no challenge detection/solver is implemented.
- **`x_version` drift.** The build-hash header will go stale; it's config now,
  but auto-discovering it from the homepage HTML would remove the manual step.
- **Search response codes.** `header.responseCode`/`keywordProcess` are parsed
  but not acted upon (e.g. redirected or unsafe-query results) — worth
  surfacing as warnings.
- **PDP variant data.** The variant/wholesale/shipment fragments were trimmed;
  a `product-variants` crawler type could re-add `ProductVariant`.
- **Input drivers.** Only `StdInputDriver` exists (jobs from JSON file/CLI);
  the architecture anticipates a queue-backed driver (beanstalk/Kafka
  consumer) for production job feeds.
- **Doc IDs in ES.** Documents are indexed with auto-generated `_id`s;
  re-crawls duplicate. Passing `doc_id=payload.id` through `Output.put()`
  kwargs would make ingestion idempotent (driver already supports it).
- **Integration tests.** The suite is unit-only by design; a `--live` marked
  smoke test against the real gateway would catch schema drift early.
- **Windows console emoji** in `setup_infra.py`'s final warning line could
  hit cp1252 encode errors on legacy terminals (cosmetic).

---

## 9. Shopee addition (second marketplace)

The module was extended from Tokopedia-only to **multi-marketplace** by adding
a Shopee client. Goal: build `library/shopee_api.py` from the
`shopee_search_product.txt` browser capture and prove it returns real search
JSON.

### 9.1 What was built

| File | Change |
|---|---|
| `library/shopee_api.py` | **New.** `ShopeeAPI` async client (httpx) for `/api/v4/search/search_items` — retry, throttle, anti-bot error detection, `item_basic` parsing, keyword **and** category (`match_id`) modes. |
| `library/schemas.py` | **Extended.** Added `ShopeeSearchProductRequest` (keyword/category param builder, paging via `newest = (page-1)*limit`) and `ShopeeProduct` document; widened `KafkaEvent.payload` to `Union[TokopediaDocument, ShopeeDocument]`. |
| `library/config.py` | **Extended.** Added `ShopeeCrawlerSettings` in its own `SHOPEE_*` env namespace + a `shopee_settings` singleton. |
| `controllers/shopee/__init__.py` | **New.** `ShopeeControllers` base — `ShopeeAPI` lifecycle + job-parsing helpers (mirrors `TokopediaControllers`, uses `shopee_settings`). |
| `controllers/shopee/search_product.py` | **New.** `ShopeeSearchProduct` controller with `handler()` + `scrape_to_json()` (keyword **and** `match_id` category modes). |
| `main.py` | **Refactored.** `CONTROLLER_REGISTRY` nested by platform; added `--platform {tokopedia,shopee}` (default tokopedia), `--match-id`, platform-aware `resolve_controller` validation, and `match_id` in the job dict. |
| `tests/test_shopee_api.py` | **New.** 14 tests; fixtures trimmed from a real captured response. |
| `tests/test_shopee_controllers.py` | **New.** 4 tests — controller orchestration with a mocked `ShopeeAPI` (scrape, output dispatch, CLI-over-job precedence, category mode). |
| `config.yaml`, `.env.example`, `.gitignore`, `README.md`, `CLAUDE.md` | Updated for the `SHOPEE_*` namespace, `--platform` CLI, the anti-bot caveat, and capture-file hygiene. |

The Shopee client and controller deliberately **reuse the existing pipeline
contracts** (`KafkaEvent` envelope, the same httpx lifecycle/throttle/retry
shape as `TokopediaAPI`, the same `Controllers` ABC and output drivers, the same
`ErrorRequestException` / `RateLimitExceeded` exceptions). Wiring Shopee into the
CLI required **no change** to `controllers/__init__.py`, `helpers/`, or the output
factory — only a new top-level key in the platform-nested registry (Open/Closed
confirmed in practice).

### 9.2 The capture & schema decisions

The source `shopee_search_product.txt` is a cURL capture of
`GET /api/v4/search/search_items`. Unlike Tokopedia's GraphQL POST, Shopee is a
**REST GET with a flat query string** and a response envelope of
`{error, items: [{item_basic: {...}}], nomore, ...}`.

- **`item_basic` parsing.** Each result row wraps the real product under
  `item_basic`; `_parse_item()` unwraps it and skips rows without an `itemid`.
- **Price scaling.** Shopee serves prices as integers scaled by 1e5
  (`6000000000` → Rp 60,000). `ShopeeProduct.price` keeps the raw value;
  `price_idr` exposes the normalised amount.
- **Rating flattening.** A `model_validator` lifts
  `item_rating.rating_star` into the flat `rating` field.
- **Pagination.** Shopee echoes a `nomore` boolean; the client stops on it
  rather than guessing from item counts.
- **Headers as config.** `x-api-source`, `x-shopee-language`, and the rotating
  anti-bot tokens are config (`api_source`, `language`, `extra_headers`); the
  CSRF token is echoed from the `csrftoken` cookie automatically.

### 9.3 Anti-bot investigation (the "explore & fix until it works" loop)

Getting a real response required iterating through Shopee's layered anti-bot:

| # | Attempt | Result | Lesson |
|---|---------|--------|--------|
| 1 | httpx, no cookies | `403 Forbidden` | anonymous calls rejected |
| 2 | httpx + stale cookies from old capture | `200` but `error=90309999` | anti-bot block; tokens expired |
| 3 | prime fresh cookies via `/api/v4/search/search_hint` (returns 200 + sets `SPC_F`/`SPC_SI`/…) then `search_items` | still `90309999` | `search_items` needs a valid `x-sap-sec` token; cookies alone insufficient |
| 4 | real browser via **Playwright** (headless + headed) | redirected to `/verify/traffic/error` | block is also at the **IP-reputation** layer (datacenter IP) |
| 5 | exact replay of an updated capture with a **logged-in session** (`SPC_U`/`SPC_ST`), captcha cert (`AC_CERT_D`) and fresh `x-sap-sec`/`sz-token` | **`200`, `error=None`, 60 items** ✅ | a valid, freshly-captured browser session works |

Key finding: the `x-sap-sec`/`x-sap-ri`/`sz-token` triplet is generated by
Shopee's obfuscated JS (`x-sz-sdk-version: 1.12.39`) and **signed per request
URL** — replaying the same token against a *different* query (e.g. switching the
captured `PAGE_CATEGORY` request to a keyword search) returns `90309999`. So the
token cannot be reused across requests, and the client is designed to accept a
caller-supplied session rather than attempt to mint tokens.

### 9.4 Live verification

With the fresh logged-in session, the exact captured request returned **60 real
products**, and `ShopeeProduct`/`_parse_item` parsed **60/60** correctly
(verified: `id`, `shop_id`, `price_idr` = Rp 60,000, `shop_location`, derived
product `url`). The trimmed-down real item is now the fixture backing
`tests/test_shopee_api.py`. Full suite after the addition: **78 passed**
(60 Tokopedia/pipeline + 14 Shopee client/schema + 4 Shopee controller).

### 9.5 Shopee env vars introduced

Namespace `SHOPEE_` (flat, no nesting). See `CLAUDE.md` for the full table.
Notable: `SHOPEE_COOKIES` (session string), `SHOPEE_EXTRA_HEADERS` (JSON dict of
rotating anti-bot tokens) — both **secrets**; `SHOPEE_DEFAULT_LIMIT` (default
60), `SHOPEE_RATE_LIMIT_RPS` (default 2.0, lower than Tokopedia's 5.0 given the
stricter anti-bot), `SHOPEE_PROXY_URL` (residential recommended).

### 9.6 What was added/deleted during the Shopee work

- **Added (temporary, then deleted):** throwaway exploration scripts
  (`_live_shopee_*.py`), the large `shopee_sample_response.json`, and Playwright
  debug screenshots — all removed after a trimmed fixture was extracted.
- **Dependency note:** `playwright` was installed *only* as an investigation
  tool to prove the IP-reputation block; it is **not** a runtime dependency and
  is not in `requirements.txt`. The shipped client is pure `httpx`.
- **`.gitignore`:** added patterns for raw capture files
  (`/shopee_*.txt`, `/tokopedia_*.txt`, `/*_search_product.txt`, …) because the
  updated `shopee_search_product.txt` now contains a **live logged-in session**
  (cookies `SPC_U`/`SPC_ST`, captcha cert, anti-bot tokens) that must never be
  committed.

### 9.7 Shopee open questions & future work

- **CLI integration — done.** Shopee is now wired into `main.py` via
  `--platform shopee` with a `controllers/shopee/` package and a platform-nested
  `CONTROLLER_REGISTRY` (this was the follow-up step after the library-first
  build). Remaining items below.
- **Token freshness / automation.** Sustained crawling needs a way to keep
  `x-sap-sec`/cookies fresh — e.g. a Playwright sidecar that mints a session per
  run, or an external token service. Out of scope for this iteration.
- **IP reputation.** Datacenter IPs hit `/verify/traffic`; production use needs
  residential/mobile proxies (`SHOPEE_PROXY_URL` hook exists).
- **Single capability.** Only product search is implemented; shop search, PDP,
  and reviews would follow the same `ShopeeProduct`-style pattern.
- **ES mapping.** `setup_infra.py`'s index mapping is Tokopedia-shaped; Shopee
  documents (e.g. `price` scaled by 1e5, `shop_location`) would benefit from a
  dedicated index/mapping when Shopee is wired into the output pipeline.
