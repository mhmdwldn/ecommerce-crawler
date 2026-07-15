# CLAUDE.md — E-Commerce End-to-End Crawler

## Project overview

Async, config-driven **Tokopedia** crawler. It calls the same public storefront GraphQL gateway the web frontend uses and emits normalised JSON documents to **Kafka**, **Elasticsearch**, a **file**, or **stdout**.

**Tokopedia** (fully wired into the CLI/controller pipeline) — GraphQL gateway `https://gql.tokopedia.com`. Four crawler types:

| `--type`          | GraphQL operation      | Output document        |
|-------------------|------------------------|------------------------|
| `search-product`  | `SearchProductV5Query` | `TokopediaProduct`     |
| `search-shop`     | `AceSearchShopQuery`   | `TokopediaShop`        |
| `product-detail`  | `PDPMainInfo`          | `TokopediaProductDetail` |
| `product-reviews` | `productReviewList`    | `TokopediaReview`      |

The CLI selects the marketplace with `--platform {tokopedia}` (default `tokopedia`); `main.py` resolves the controller from a platform-nested `CONTROLLER_REGISTRY`.

Intended use: portfolio / data-engineering demos — a realistic scrape → validate → publish pipeline.

## Tech stack

| Layer            | Library                              | Role |
|------------------|--------------------------------------|------|
| Runtime          | Python 3.10+, `asyncio`              | async-first execution |
| Config           | `pydantic-settings` (`BaseSettings`) | env / YAML / .env layered config |
| Validation       | Pydantic v2 (`BaseModel`)            | request payloads, documents, Kafka event envelope |
| HTTP client      | `httpx` (AsyncClient)                | Tokopedia GraphQL POSTs, with retries + rate limiting |
| Message queue    | `aiokafka`                           | async producer + async admin (topic setup) |
| Search / storage | `elasticsearch[async]` 8.x           | AsyncElasticsearch indexing + index setup |
| Testing          | `pytest` + `pytest-asyncio` + `pytest-mock` | fully mocked unit tests |

## Project structure

```
ecommerce-crawler/
├── CLAUDE.md                  # this file
├── README.md                  # user-facing docs
├── config.yaml                # sample YAML config (no secrets)
├── .env.example               # env-var template (copy to .env)
├── .gitignore                 # Python / Docker / env hygiene
├── Dockerfile                 # python:3.11-slim image, ENTRYPOINT main.py
├── requirements.txt           # pointer to source/requirements.txt
├── skills/
│   └── exploration.md         # retrospective build report
└── source/
    ├── main.py                # argparse CLI entry point (controller registry)
    ├── requirements.txt       # runtime + test dependencies
    ├── .gitignore / .dockerignore
    ├── controllers/
    │   ├── __init__.py        # Controllers ABC: input loop, output dispatch, exc handling
    │   ├── tokopedia/
    │   │   ├── __init__.py    # TokopediaControllers base: API lifecycle, job parsing
    │   │   ├── search_product.py   # TokopediaSearchProduct controller
    │   │   ├── search_shop.py      # TokopediaSearchShop controller
    │   │   ├── product_detail.py   # TokopediaProductDetail controller
    │   │   └── product_reviews.py  # TokopediaProductReviews controller
    ├── library/
    │   ├── config.py          # BaseSettings tree (TOKOPEDIA_* prefix) + singleton
    │   ├── schemas.py         # Pydantic v2 request/document/event models (Tokopedia)
    │   ├── graphql_queries.py # Tokopedia GraphQL query documents (trimmed from captures)
    │   ├── tokopedia_api.py   # TokopediaAPI async client (httpx)
    │   └── setup_infra.py     # async Kafka topic + ES index bootstrap CLI
    ├── helpers/
    │   ├── input/             # Input facade + StdInputDriver (+ factory)
    │   └── output/            # Output facade + drivers: kafka, elasticsearch, file, std (+ factory)
    ├── exception/
    │   └── exception.py       # ErrorRequestException, RateLimitExceeded, OutputDriverNotRecognizeException
    ├── deployment/
    │   ├── compose.yaml       # local Kafka + Zookeeper + ES + Kibana stack
    │   ├── 01-configmap.yaml  # k8s ConfigMap (production config.yaml)
    │   └── 02-deployment.yaml # k8s Deployment (search-product → Kafka)
    └── tests/                 # pytest suite (60 tests, all network mocked)
```

## Architecture patterns

- **Config-driven:** zero hardcoded URLs/topics/indices/headers. Everything is a field in `library/config.py`, layered: init kwargs > env vars (`TOKOPEDIA_*`) > `config.yaml` > `.env` > defaults. The `x-version` build hash and location-bias IDs are config so they rotate without code changes.
- **Async-first:** `httpx.AsyncClient` for scraping, `aiokafka` for publishing, `AsyncElasticsearch` for indexing. Kafka/ES output drivers run their async clients on a dedicated background thread + event loop so the synchronous `OutputDriver.put()` contract never deadlocks the crawler's loop.
- **Open/Closed (controllers):** new crawler types subclass `TokopediaControllers` and implement `handler()` / `scrape_to_json()`; `main.py` finds them via `CONTROLLER_REGISTRY`. The pipeline engine (`Controllers` ABC, drivers, factories) is never modified.
- **Open/Closed (outputs):** new destinations subclass `OutputDriver` and register in the factory's `_DRIVERS` dict.
- **Single responsibility:** request building (schemas) / HTTP+parsing (`TokopediaAPI`) / orchestration (controllers) / delivery (output drivers) are separate layers.
- **Typed contracts:** every wire payload in and out is a Pydantic v2 model; the Kafka envelope (`KafkaEvent`) is `extra="forbid"`.

## How to run

```bash
# setup
python -m venv .venv && .venv\Scripts\activate     # Windows
pip install -r source/requirements.txt
cp .env.example .env                                # optional — defaults work

# scrape-only (JSON to stdout / file) — Tokopedia (default platform)
cd source
python main.py crawler --platform tokopedia --mode scrape --type search-product --keyword "poco f8" --pretty
python main.py crawler --platform tokopedia --mode scrape --type search-shop --keyword "xiaomi" -o shops.json
python main.py crawler --platform tokopedia --mode scrape --type product-detail --url "https://www.tokopedia.com/xiaomi/poco-f8-pro"
python main.py crawler --platform tokopedia --mode scrape --type product-reviews --product-id 102988772766 --max-pages 3

# full pipeline
bash start.sh                                         # startup berurutan + auto DDL/seed/infra
python main.py crawler --platform tokopedia --mode full --type search-product --keyword "poco f8" \
    -d kafka -o tokopedia.products.raw --bootstrap-servers localhost:9092
python main.py crawler --platform tokopedia --mode full --type search-product --keyword "poco f8" \
    -d elasticsearch -o tokopedia_products --elasticsearch-hosts http://localhost:9200
```

## How to test

```bash
cd source
pytest tests/ -v
```

- Runner: `pytest` with `pytest-asyncio` (strict mode — async tests are marked `@pytest.mark.asyncio`).
- Tests live in `source/tests/`; shared fixtures (sample GraphQL responses, settings) in `tests/conftest.py`.
- All HTTP/Kafka/ES calls are mocked via `pytest-mock` — the suite never touches the network.
- Current suite: **~60 tests** (Tokopedia/pipeline).

## Crawler extension guide (Open/Closed)

To add a new crawler type to an **existing** platform (e.g. a Tokopedia shop-products type) **without touching the pipeline engine**:

1. **Query** — add the GraphQL document to `library/graphql_queries.py` (Tokopedia) or the equivalent for the platform.
2. **Schemas** — in `library/schemas.py`, add a `<Platform><X>Request` (with `operation_name` ClassVar and `to_variables()`/`to_params()`) and a document model; extend the platform's document union.
3. **Config** — add the endpoint path as a field on the platform's `*CrawlerSettings`.
4. **API method** — add an async generator on the platform API client that builds the request, executes it, validates documents, and yields `KafkaEvent`s.
5. **Controller** — create `controllers/<platform>/<x>.py` subclassing the platform's controllers base with `handler()` and `scrape_to_json()` (copy an existing controller as the template).
6. **Register** — add one entry under the platform in `CONTROLLER_REGISTRY` in `main.py`.
7. **Test** — add fixtures + tests mirroring the existing `tests/test_*_api.py` / `tests/test_*controllers.py`.

To add a whole **new marketplace**: create `library/<name>_api.py` + a `*CrawlerSettings` (its own env namespace) + schemas, a `controllers/<name>/` package with a controllers base + handlers, then add a new top-level key to `CONTROLLER_REGISTRY`. The CLI exposes it via `--platform <name>` automatically (choices come from the registry).

Nothing in `controllers/__init__.py`, `helpers/`, or the output factory changes.

## Environment variables reference

Prefix `TOKOPEDIA_`, nesting delimiter `__`. All optional (sane defaults built in).

| Variable | Type | Description | Example |
|----------|------|-------------|---------|
| `TOKOPEDIA_ENVIRONMENT` | str | deployment environment | `production` |
| `TOKOPEDIA_LOG_LEVEL` | str | root log level | `INFO` |
| `TOKOPEDIA_KAFKA__BOOTSTRAP_SERVERS` | str | comma-separated brokers | `kafka01:9092,kafka02:9092` |
| `TOKOPEDIA_KAFKA__TOPIC` | str | default topic | `tokopedia.products.raw` |
| `TOKOPEDIA_KAFKA__CLIENT_ID` | str | producer client id | `tokopedia-crawler` |
| `TOKOPEDIA_KAFKA__ACKS` | str | producer acks | `all` |
| `TOKOPEDIA_KAFKA__COMPRESSION_TYPE` | str? | codec | `gzip` |
| `TOKOPEDIA_KAFKA__MAX_REQUEST_SIZE` | int | bytes | `1048576` |
| `TOKOPEDIA_KAFKA__LINGER_MS` | int | batch delay ms | `10` |
| `TOKOPEDIA_KAFKA__REQUEST_TIMEOUT_MS` | int | producer timeout ms | `30000` |
| `TOKOPEDIA_ELASTICSEARCH__HOSTS` | list[str] (JSON) | node URLs | `["http://localhost:9200"]` |
| `TOKOPEDIA_ELASTICSEARCH__INDEX_NAME` | str | default index | `tokopedia_products` |
| `TOKOPEDIA_ELASTICSEARCH__API_KEY` | str? | API-key auth | `b64key==` |
| `TOKOPEDIA_ELASTICSEARCH__USERNAME` | str? | basic auth user | `elastic` |
| `TOKOPEDIA_ELASTICSEARCH__PASSWORD` | str? | basic auth password | `changeme` |
| `TOKOPEDIA_ELASTICSEARCH__REQUEST_TIMEOUT` | int | seconds | `30` |
| `TOKOPEDIA_ELASTICSEARCH__MAX_RETRIES` | int | transient retries | `3` |
| `TOKOPEDIA_CRAWLER__BASE_URL` | str | GraphQL gateway | `https://gql.tokopedia.com` |
| `TOKOPEDIA_CRAWLER__SITE_URL` | str | Origin/Referer | `https://www.tokopedia.com` |
| `TOKOPEDIA_CRAWLER__SEARCH_PRODUCT_ENDPOINT` | str | endpoint path | `/graphql/SearchProductV5Query` |
| `TOKOPEDIA_CRAWLER__SEARCH_SHOP_ENDPOINT` | str | endpoint path | `/graphql/AceSearchShopQuery` |
| `TOKOPEDIA_CRAWLER__PRODUCT_DETAIL_ENDPOINT` | str | endpoint path | `/graphql/PDPMainInfo` |
| `TOKOPEDIA_CRAWLER__PRODUCT_REVIEWS_ENDPOINT` | str | endpoint path | `/graphql/productReviewList` |
| `TOKOPEDIA_CRAWLER__X_VERSION` | str | frontend build hash header | `a3540b9` |
| `TOKOPEDIA_CRAWLER__X_SOURCE` | str | x-source header | `tokopedia-lite` |
| `TOKOPEDIA_CRAWLER__X_DEVICE` | str | x-device header | `desktop` |
| `TOKOPEDIA_CRAWLER__LITE_SERVICE` | str | x-tkpd-lite-service header | `zeus` |
| `TOKOPEDIA_CRAWLER__DEVICE_ID` | str | bd-device-id header (empty = omit) | `7650121588951598612` |
| `TOKOPEDIA_CRAWLER__COOKIES` | str | session cookie string (secret!) | `_SID_Tokopedia_=...` |
| `TOKOPEDIA_CRAWLER__UNIQUE_ID` | str | visitor UUID (empty = random) | `5dd48c00...` |
| `TOKOPEDIA_CRAWLER__USER_DISTRICT_ID` | str | location bias | `2274` |
| `TOKOPEDIA_CRAWLER__USER_CITY_ID` | str | location bias | `176` |
| `TOKOPEDIA_CRAWLER__DEFAULT_ROWS` | int (1–100) | results per page | `20` |
| `TOKOPEDIA_CRAWLER__REQUEST_TIMEOUT` | float | HTTP timeout s | `30.0` |
| `TOKOPEDIA_CRAWLER__MAX_RETRIES` | int | HTTP retries | `3` |
| `TOKOPEDIA_CRAWLER__RETRY_BACKOFF` | float | exponential base | `2.0` |
| `TOKOPEDIA_CRAWLER__RATE_LIMIT_RPS` | float | client-side throttle | `5.0` |
| `TOKOPEDIA_CRAWLER__PROXY_URL` | str? | HTTP/SOCKS proxy | `http://user:pass@proxy:8080` |
| `TOKOPEDIA_CRAWLER__USER_AGENT` | str | UA header | `Mozilla/5.0 ...` |

## Git hygiene

**Never commit:** `.env` / `.env.local`, cookie strings, device IDs, or anti-bot tokens, raw browser captures (the `*_search_product.txt` etc. cURL/HAR dumps — they hold live login cookies and tokens), crawl outputs (`results*.json`, `output/`), virtualenvs, `*.local.yaml`.

`.gitignore` covers: Python bytecode/build artifacts, virtualenvs, IDE folders (`.idea/`, `.vscode/`, `.claude/`), pytest/mypy/ruff caches, logs, runtime outputs, env files, the raw marketplace capture files (`/tokopedia_*.txt`, `/*_search_product.txt`, …), and OS cruft. `.env.example` (placeholder-only) and `config.yaml` (no secrets — cookies/tokens commented out) are intentionally committed.



## Control plane: Asset Registry (module `assets/`)

Selain crawler engine (`source/`) di atas, repo ini punya **control plane** terpisah:
daftar target crawl (keyword/URL/product_id) yang harus dijalankan, disimpan di Postgres,
dikelola lewat Streamlit admin UI. Dokumen desain lengkap: sharded PRD di `docs/prd/` —
baca `PRD_50_Asset_Registry.md` sebelum menyentuh modul ini.

```
assets/
├── ddl/crawl_assets.sql   # schema Postgres (schema `control`)
├── seeds/targets.yaml     # daftar target awal, versioned — sumber kebenaran seed
├── seed.py                # upsert YAML → Postgres, idempotent (python -m assets.seed)
├── repository.py          # SATU-SATUNYA pintu tulis/baca ke control.crawl_assets
├── app.py                 # Streamlit admin CRUD (streamlit run assets/app.py)
└── tests/test_asset_registry.py
```

**Aturan keras:** semua akses ke tabel `control.crawl_assets` — dari DAG, dari script mana
pun — WAJIB lewat `assets/repository.py`. Jangan raw SQL di tempat lain. Ini mencegah logic
due/circuit-breaker punya dua sumber kebenaran.

**Kenapa terpisah dari `source/`:** `source/` = *bagaimana* cara crawl (engine, dipertahankan
Open/Closed seperti didokumentasikan di atas). `assets/` = *apa* yang di-crawl (data operasional,
berubah tiap hari tanpa deploy kode). Analogi: `source/` itu mesinnya, `assets/` itu daftar
tujuannya.

**Config:** ikut pola project ini — `pydantic-settings`, prefix `TOKOPEDIA_`, delimiter `__`.
```
TOKOPEDIA_CONTROL__DSN=host=localhost port=5433 dbname=mart user=mart password=mart
```
Saat ini `assets/repository.py` masih baca lewat `os.getenv` langsung (lihat `get_dsn()`
di file itu) sebagai jalan cepat. **TODO housekeeping:** pindahkan ke
`library/config.py` sebagai `ControlPlaneSettings` resmi (nested di settings tree yang
sudah ada), supaya satu mekanisme config untuk seluruh repo — bukan dua.

**Bootstrap:** belum diintegrasikan ke `library/setup_infra.py`. Untuk konsistensi dengan
pola bootstrap Kafka topic/ES index yang sudah ada, `assets/ddl/crawl_assets.sql` sebaiknya
dieksekusi dari situ juga (tambah satu fungsi `setup_control_plane_table()`), bukan
`psql -f` manual selamanya. Belum dikerjakan — lihat TASKS.md fase 2.5.

**Cara jalanin:**
```bash
psql <DSN> -f assets/ddl/crawl_assets.sql   # sekali di awal / setelah ubah schema
python -m assets.seed                       # sinkronkan targets.yaml → Postgres, aman diulang
streamlit run assets/app.py                 # UI CRUD (tambah/nonaktifkan keyword)
```

**Belum tersambung ke DAG.** `assets/repository.py:get_due_assets()` sudah siap dipakai
sebagai input `crawl.expand()` (Airflow dynamic task mapping), tapi
`pipeline/airflow/dags/tokopedia_products_dag.py` belum di-refactor untuk memanggilnya —
saat ini DAG masih pakai sumber keyword lama (Variable/hardcode). Lihat TASKS.md task 2.5.4.
