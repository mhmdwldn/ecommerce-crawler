# E-Commerce End-to-End Crawler

Production-ready, config-driven **Tokopedia** crawler. Scrapes the public storefront GraphQL gateway (`gql.tokopedia.com`) and streams normalised documents into **Kafka** or **Elasticsearch** — or just dumps JSON to a file/stdout.

```
CLI (main.py --platform tokopedia) ──> Controller ──> TokopediaAPI (httpx, async) ──> storefront API
                                   │
                                   └──> Output driver: kafka | elasticsearch | file | std
```

## Features

### Tokopedia crawler types

| Crawler type      | GraphQL operation      | What it scrapes                          |
|-------------------|------------------------|------------------------------------------|
| `search-product`  | `SearchProductV5Query` | Products matching a keyword (paginated)   |
| `search-shop`     | `AceSearchShopQuery`   | Shops matching a keyword (paginated)      |
| `product-detail`  | `PDPMainInfo`          | Full product detail page (price, stats)   |
| `product-reviews` | `productReviewList`    | Product reviews (paginated via `hasNext`) |

- **Fully async I/O** — `httpx` for HTTP, `aiokafka` for Kafka, `elasticsearch-py` async client for ES
- **Config-driven** — every URL, topic, index, and header is a `pydantic-settings` field; override via `TOKOPEDIA_*` env vars, `.env`, or `config.yaml`
- **Typed data contracts** — Pydantic v2 models for requests, documents, and the Kafka event envelope
- **Open/Closed** — add a new crawler type by dropping in a controller; the pipeline engine is untouched

## Quick start

```bash
# 1. Install
pip install -r source/requirements.txt

# 2. Configure (optional — sane defaults are built in)
cp .env.example .env

# 3. Scrape Tokopedia products to stdout (tokopedia is the default platform)
cd source
python main.py crawler --platform tokopedia --mode scrape --type search-product --keyword "poco f8" --pretty

# Shops, saved to a file
python main.py crawler --platform tokopedia --mode scrape --type search-shop --keyword "xiaomi" -o shops.json

# Product detail by URL
python main.py crawler --platform tokopedia --mode scrape --type product-detail \
    --url "https://www.tokopedia.com/xiaomi/poco-f8-pro-12-512gb"

# Reviews by product ID
python main.py crawler --platform tokopedia --mode scrape --type product-reviews --product-id 102988772766 --max-pages 3
```

## Full pipeline (Kafka / Elasticsearch)

```bash
# Start local infra (Kafka + ES + Kibana)
docker compose -f source/deployment/compose.yaml up -d

# Create topic + index
cd source
python library/setup_infra.py

# Crawl → Kafka
python main.py crawler --mode full --type search-product --keyword "poco f8" \
    -d kafka -o tokopedia.products.raw --bootstrap-servers localhost:9092

# Crawl → Elasticsearch
python main.py crawler --mode full --type search-product --keyword "poco f8" \
    -d elasticsearch -o tokopedia_products --elasticsearch-hosts http://localhost:9200
```

## Configuration

Priority: **CLI args > env vars > `config.yaml` > `.env` > defaults**.

All env vars are prefixed `TOKOPEDIA_` with `__` as the nesting delimiter, e.g.:

```bash
TOKOPEDIA_KAFKA__BOOTSTRAP_SERVERS=kafka01:9092
TOKOPEDIA_ELASTICSEARCH__INDEX_NAME=tokopedia_products
TOKOPEDIA_CRAWLER__RATE_LIMIT_RPS=2.0
TOKOPEDIA_CRAWLER__COOKIES="_SID_Tokopedia_=...; bm_sz=..."   # never commit!
```

See [.env.example](.env.example) and [config.yaml](config.yaml) for the full reference, and `CLAUDE.md` for a complete env-var table.

## Tests

```bash
cd source
pytest tests/ -v
```

Async tests run via `pytest-asyncio`; all network calls are mocked — no live traffic.

## Docker

```bash
docker build -t tokopedia-crawler .
docker run tokopedia-crawler crawler --mode scrape --type search-product --keyword "poco f8"
```

Kubernetes manifests live in [source/deployment/](source/deployment/).

## Project layout

```
├── config.yaml               # sample YAML config (no secrets)
├── .env.example              # env-var template
├── Dockerfile
└── source/
    ├── main.py               # CLI entry point
    ├── controllers/          # base controller + tokopedia/ handlers
    ├── library/              # config, schemas, GraphQL queries, tokopedia_api, infra setup
    ├── helpers/              # input/output driver framework (factory pattern)
    ├── exception/            # custom exceptions
    ├── deployment/           # compose + k8s manifests
    └── tests/                # pytest suite (Tokopedia)
```

## Legal note

This project is for **portfolio / educational** purposes. It only calls the same public endpoints a browser uses, applies client-side rate limiting, and stores no credentials in the repo (browser captures with live cookies/tokens are git-ignored). Respect Tokopedia's terms of service and robots policy when running it.
