# Exploration Report — Multi-Marketplace E-Commerce Crawler

Retrospective of the work done to (1) mirror the reference TikTok project into a
production-ready Tokopedia crawler (Steps 1–4 of the migration brief), and
(2) add a second marketplace, Shopee, as an async client library.
Dates: 2026-06-12 (Tokopedia migration), 2026-06-12 (Shopee addition).

Sections 1–8 cover the Tokopedia migration; **section 9 covers the Shopee
addition** (client, anti-bot investigation, and live verification).

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
