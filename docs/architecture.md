# Architecture & Project Guide

Panduan lengkap untuk memahami project ini dari nol: apa, kenapa, dan gimana aliran datanya.

---

## Ringkasan 30 Detik

Project ini **crawler Tokopedia + data pipeline end-to-end**. Lo kasih keyword, sistem:

1. **Crawl** produk dari Tokopedia (GraphQL API publik)
2. **Kirim** hasil crawl sebagai JSON event ke Kafka
3. **Tarik** dari Kafka ke Delta Lake di MinIO (bronze)
4. **Bersihin & rapikan** jadi data terstruktur (silver)
5. **Transformasi** jadi star schema data warehouse (gold, via dbt/DuckDB)
6. **Simpan** ke dua serving layer: Postgres dan ClickHouse
7. **Orkestrasi** semuanya pakai Airflow DAG

Output akhir: dashboard BI yang nampilin harga produk Tokopedia dari waktu ke waktu.

---

## Kenapa project ini ada

Ini project **portfolio data engineering**. Tujuannya mendemonstrasikan:

- Pipeline medallion (bronze → silver → gold) — arsitektur standar industri
- Streaming + batch processing (Spark Structured Streaming + batch)
- Orchestration (Airflow DAG)
- Data warehouse modeling (star schema: dim + fact)
- Multi-destination serving (Postgres + ClickHouse)
- DevOps basics (Docker Compose, CI/CD, Makefile)

Kalau lo interview DE, lo bisa jelasin project ini dari crawler sampe dashboard dan itu mencakup 90% skill yang ditanya.

---

## Aliran data — dari keyword sampai dashboard

### Arsitektur visual

```
                        ┌──────────────────────────┐
                        │     Airflow DAGs          │
                        │  tokopedia_products @hourly│
                        │  lakehouse_maintenance    │
                        └──────┬───────────────────┘
                               │ trigger + jitter
                               ▼
┌──────────────┐    ┌──────────┐    HTTP     ┌──────────┐    Kafka    ┌───────────────┐
│ Asset        │───►│ Crawler  │────────────►│ Tokopedia │────────────►│ Kafka Topic   │
│ Registry     │    │ (httpx)  │   POST     │ GraphQL   │   produce   │ tokopedia.    │
│ (Postgres)   │    │          │            │ Gateway   │             │ products.raw  │
└──────────────┘    └──────────┘            └──────────┘            └───────┬───────┘
                                                        │ consume
                                                        ▼
                                               ┌───────────────┐
                                               │ Spark Struct. │
                                               │ Streaming     │
                                               │ (bronze)      │
                                               └───────┬───────┘
                                                       │ write Delta
                                                       ▼
                                               ┌───────────────┐
                                               │ MinIO (S3)    │
                                               │ lakehouse/    │
                                               │   bronze/     │
                                               │   silver/     │
                                               └───────┬───────┘
                                                       │ read
                                                       ▼
                                               ┌───────────────┐
                                               │ Spark Batch   │
                                               │ (silver)      │
                                               └───────┬───────┘
                                                       │ write typed Delta
                                                       ▼
                                               ┌───────────────┐
                                               │ Quality Check │
                                               │ (5 validasi)  │
                                               └───────┬───────┘
                                                       │ PASS
                                                       ▼
                                               ┌───────────────┐
                                               │ dbt + DuckDB  │
                                               │ (gold)        │
                                               │ star schema   │
                                               └───────┬───────┘
                                                       │
                                          ┌────────────┴────────────┐
                                          ▼                        ▼
                                   ┌───────────┐          ┌───────────┐
                                   │ Postgres  │          │ClickHouse │
                                   │ (mart)    │          │(serving)  │
                                   └───────────┘          └─────┬─────┘
                                          │                      │
                                          │              ┌───────▼───────┐
                                          └──────────────┤  Audit Log    │
                                                         │ pipeline_runs │
                                                         └───────────────┘
                                          │                        │
                                          └────────────┬───────────┘
                                                       ▼
                                               ┌───────────────┐
                                               │ Metabase /    │
                                               │ Superset      │
                                               │ (BI Dashboard)│
                                               └───────────────┘
```

### Layer-by-layer

#### 1. Ingestion (Crawler)

```
CLI (main.py) → Controller → TokopediaAPI (httpx) → GraphQL endpoint
```

- Lo jalanin: `python main.py crawler --type search-product --keyword "poco f8"`
- Crawler kirim POST request ke `https://gql.tokopedia.com/graphql/SearchProductV5Query`
- Respons JSON divalidasi pake Pydantic schema
- Output bisa ke: **stdout** (scrape mode), **Kafka** (full mode), **file**, atau **Elasticsearch**
- Ada 4 type crawler: `search-product`, `search-shop`, `product-detail`, `product-reviews`

**Kenapa GraphQL, bukan scraping HTML?**
Tokopedia punya GraphQL gateway publik yang sama dengan yang dipakai web frontend mereka. Respons-nya sudah terstruktur (JSON), jadi tidak perlu parse HTML yang fragile.

#### 2. Kafka (Message Queue)

```
Crawler → KafkaOutputDriver → Kafka topic "tokopedia.products.raw"
```

- Setiap produk jadi satu JSON message di Kafka topic
- Topic punya 3 partisi (paralelisme)
- Kafka jadi buffer — kalau downstream lambat, data tetap antri aman

**Kenapa Kafka?**
Decouple crawler dari pipeline. Kalau Spark mati, data tidak hilang. Kalau mau nambah consumer baru (misal: real-time alerting), tinggal subscribe ke topic yang sama.

#### 3. Bronze (Raw Ingestion)

```
Kafka → Spark Structured Streaming → Delta Lake di MinIO
```

- Spark baca dari Kafka dengan `trigger(availableNow=True)` — baca semua yang baru, lalu berhenti
- Checkpoint di MinIO mencegah baca ulang offset yang sama
- Hasil: `value_json` (JSON mentah) + `kafka_offset`, `kafka_partition`, `kafka_timestamp`, `ingested_at`
- Format: Delta Lake (Parquet + transaction log) — open source, ACID, versioned

**Kenapa "availableNow" bukan "continuous"?**
Karena Airflow trigger pipeline secara periodik (daily/hourly). Lebih sederhana daripada maintain 24/7 streaming daemon.

#### 4. Silver (Cleaned & Typed)

```
Bronze Delta → Spark Batch → Silver Delta
```

- Parse JSON `value_json` jadi kolom terstruktur: `product_id`, `product_name`, `price_idr`, `rating`, `shop_id`, dll
- Deduplikasi: kalau ada produk yang sama di-crawl dua kali dalam timestamp yang sama, ambil satu
- Baris yang tidak bisa di-parse masuk ke `_rejects` table (tidak bikin job gagal)
- Output: Delta table dengan schema typed

**Kenapa pisah bronze dan silver?**
Bronze = raw backup (bisa replay dari sini kalau logic transform berubah). Silver = data bersih yang siap dianalisis.

#### 5. Gold (Star Schema)

```
Silver Delta → dbt (DuckDB) → Gold star schema
```

- dbt baca silver Delta lewat DuckDB (pakai extension `delta`)
- Transformasi ke star schema:
  - `dim_product` — dimensi produk (nama, URL, toko)
  - `dim_shop` — dimensi toko (nama, kota, tier)
  - `fct_product_snapshot` — fakta snapshot harga (harga, diskon, rating, timestamp)
- 7 dbt tests validasi unique + not_null keys
- Materialisasi: table (full rebuild setiap run, karena datanya masih kecil)

**Kenapa dbt?**
Transformasi SQL-based, documented, tested. dbt bikin data modeling jadi software engineering — ada version control, testing, lineage graph.

#### 6. Serving Layer (Mart + Analytics)

```
Gold (DuckDB) ──┬── load_to_postgres.py ──► Postgres (mart)
                 └── load_to_clickhouse.py ─► ClickHouse (serving)
```

- **Postgres:** full reload (`DROP TABLE` + `CREATE TABLE AS SELECT`). Sederhana, aman untuk data kecil.
- **ClickHouse:** kolomar database untuk query analytics cepat. Strategi idempotensi: truncate-partition-insert untuk fakta, ReplacingMergeTree untuk dimensi.

**Kenapa dua serving layer?**
- Postgres: sudah ada di arsitektur awal, low-cost untuk menyimpan data mart tradisional
- ClickHouse: di-desain untuk query analitik (time-series, agregasi) — jauh lebih cepat dari Postgres untuk dashboard
- Dua-duanya baca dari sumber yang sama (DuckDB gold), jadi data selalu konsisten

#### 7. Quality Checks (Data Validation)

Bronze → **quality_check** → dbt → load. Lima pemeriksaan sebelum data masuk gold:

| Check | Aturan | Kalau gagal |
|---|---|---|
| `row_count` | Silver harus > 0 baris | Pipeline berhenti |
| `null_pct` | Kolom kunci < 5% null | Pipeline berhenti |
| `price_positive` | Semua `price_idr` harus > 0 | Pipeline berhenti |
| `rejects_ratio` | Baris reject < 10% total (anti silent failure) | Pipeline berhenti |
| `freshness` | Data terakhir < 2 jam (mendeteksi crawler stuck) | Pipeline berhenti |

Kalau satu check gagal → exit code 1 → Airflow task FAIL → DAG berhenti sebelum data rusak masuk mart.

#### 8. Audit Logging

Setiap DAG run menulis satu baris ke `analytics.pipeline_runs` di ClickHouse:
- `run_id`, `execution_date`, `status` (success/failed)
- `rows_silver`, `rows_rejects`, `rows_gold` — jumlah baris per layer
- `duration_sec` — berapa lama pipeline berjalan

Berguna untuk monitoring tren (data growing? rejects naik? pipeline makin lambat?) dan alerting di dashboard Metabase/Superset.

#### 9. Orchestration (Airflow)

```
DAG tokopedia_products (@hourly):
  crawl_assets → bronze → silver → quality_check → dbt_build → [pg, ch] → write_audit
   │
   └── baca dari control.crawl_assets (Postgres), crawl max 10 asset per run
       circuit breaker: 5x gagal berturut-turut → is_active=false

DAG lakehouse_maintenance (@weekly):
  optimize_bronze → optimize_silver → optimize_clickhouse
```

- **@hourly** dengan jitter 0-120 detik (hindari semua run serempak)
- **max_active_runs=1** — tidak ada concurrent run
- **max_active_tasks=2** — batasi fan-out crawl
- Retry 1x tiap task, retry delay 2 menit
- Semua tahap idempotent — rerun aman

#### 10. Asset Registry (Control Plane)

```
assets/
├── ddl/crawl_assets.sql    # Postgres schema: control.crawl_assets + v_due_assets
├── seeds/targets.yaml      # 23 target keyword (elektronik + fashion)
├── seed.py                 # Upsert YAML → Postgres, idempotent
├── repository.py           # SATU-SATUNYA akses ke tabel control.crawl_assets
└── app.py                  # Streamlit admin CRUD (tambah/nonaktifkan keyword)
```

- **v_due_assets view** — hanya asset yang is_active + sudah lewat cadence_min
- **Circuit breaker** — 5x gagal berturut-turut → `is_active=false` otomatis
- **idempotent seed** — aman dijalankan berulang (ON CONFLICT upsert)
- **Tanpa deploy kode** — tambah keyword lewat UI Streamlit langsung muncul di antrian

#### 11. Logging (loguru)

Semua log dari crawler, httpx, aiokafka, dan controller menggunakan **loguru**.
- InterceptHandler di `main.py` menangkap semua `logging.getLogger()` → format loguru
- Warna di terminal, nama logger rata kiri 30 karakter
- Pipeline tetap pakai print + Spark internal logging

#### 12. BI Dashboard (Fase 3)

Dua BI tools untuk serving analytics:

| Tool | Port | Backend | Login |
|---|---|---|---|
| **Metabase** | 3000 | Postgres mart | `admin@local.com` (first-run setup) |
| **Superset** | 8088 | ClickHouse | `admin` / `admin` |

**5 Dashboard:**
1. **US-1 Price Trend** — rata-rata/min/max harga per hari, 30 hari (line chart)
2. **US-2 Top Price Drops** — produk termurah hari ini (table, sorted ASC)
3. **US-3 Shop/Kota** — jumlah produk per kota + avg price (bar chart)
4. **Pipeline Health** — rows/run, rejects trend, durasi dari `pipeline_runs` (time series)
5. **Asset Health** — asset aktif vs nonaktif per kategori dari `control.crawl_assets` (summary)

Setup otomatis: `dashboards/setup_metabase.py` + `dashboards/setup_superset.py`.
Semua query SQL terdokumentasi di `dashboards/dashboards.sql` — dual dialect (Postgres + ClickHouse).

---

## Struktur direktori

```
ecommerce-crawler/
│
├── source/                         # ⚙️ Crawler engine
│   ├── main.py                     #   CLI entry point (argparse)
│   ├── controllers/                #   Crawler logic
│   │   ├── __init__.py             #     Base Controllers (ABC: input → process → output)
│   │   └── tokopedia/              #     Tokopedia controllers
│   │       ├── __init__.py         #       TokopediaControllers base
│   │       ├── search_product.py   #       Product search handler
│   │       ├── search_shop.py      #       Shop search handler
│   │       ├── product_detail.py   #       Product detail handler
│   │       └── product_reviews.py  #       Review list handler
│   ├── library/                    #   Shared modules
│   │   ├── config.py               #     Pydantic-settings config tree
│   │   ├── schemas.py              #     Pydantic v2 data models
│   │   ├── graphql_queries.py      #     GraphQL query strings
│   │   ├── tokopedia_api.py        #     Async httpx HTTP client
│   │   └── setup_infra.py          #     Kafka topic + ES index bootstrap
│   ├── helpers/                    #   I/O framework
│   │   ├── input/                  #     Input facade (baca job list)
│   │   └── output/                 #     Output driver framework
│   │       ├── driver/             #       kafka.py, elasticsearch.py, file.py, std.py
│   │       └── factory/            #       Driver registry
│   ├── exception/                  #   Custom exceptions
│   ├── deployment/                 #   Docker Compose + K8s manifests
│   │   └── compose.yaml            #     8 services (ZK, Kafka, ES, Kibana, MinIO, PG, CH, Airflow)
│   └── tests/                      #   Crawler tests (60/60)
│
├── pipeline/                       # 🔄 Medallion pipeline
│   ├── spark/                      #   Spark jobs
│   │   ├── session.py              #     SparkSession builder (Delta + S3A)
│   │   ├── stream_bronze.py        #     Kafka → Delta (streaming, availableNow)
│   │   ├── silver.py               #     Bronze → typed + dedup + rejects
│   │   └── maintenance.py          #     OPTIMIZE + VACUUM bronze/silver
│   ├── dbt/                        #   dbt project (gold)
│   │   ├── dbt_project.yml         #     Project config
│   │   ├── profiles.yml            #     DuckDB connection profile
│   │   └── models/                 #     SQL transformation models
│   ├── quality/                    #   Data quality
│   │   ├── checks.py               #     5 quality checks (row/null/price/rejects/freshness)
│   │   └── audit.py                #     Write pipeline_runs audit to ClickHouse
│   ├── load/                       #   Serving layer loaders
│   │   ├── load_to_postgres.py     #     DuckDB → Postgres (DuckDB ATTACH)
│   │   ├── load_to_clickhouse.py   #     DuckDB → ClickHouse (clickhouse-connect)
│   │   ├── crawl_assets.py         #     Crawl due assets from registry
│   │   └── ch_client.py            #     Shared ClickHouse client builder
│   ├── airflow/                    #   Orchestration
│   │   ├── Dockerfile              #     Airflow image (Spark + dbt + DuckDB)
│   │   └── dags/                   #     tokopedia_products + lakehouse_maintenance
│   ├── tests/                      #   Pipeline tests (7)
│   └── requirements.txt            #   pyspark, dbt-duckdb, clickhouse-connect, psycopg2
│
├── assets/                         # 📋 Control plane (crawl target registry)
│   ├── ddl/                        #   Postgres DDL (schema `control`)
│   ├── seeds/                      #   Seed data (YAML → DB)
│   ├── app.py                      #   Streamlit admin UI
│   ├── repository.py               #   Single DB access point
│   └── tests/                      #   Registry tests (15)
│
├── warehouse/                      # 🏗️ Data warehouse DDL
│   └── clickhouse/ddl/             #   ClickHouse table definitions
│
├── dashboards/                     # 📊 BI Dashboard specs + setup scripts
│   ├── dashboards.sql              #   5 dashboard SQL (US-1/2/3 + health)
│   ├── setup_metabase.py           #   API-based Metabase connection setup
│   ├── setup_superset.py           #   API-based Superset ClickHouse setup
│   ├── metabase_exports/           #   Export directory
│   └── superset_exports/           #   Export directory
├── docs/                           # 📖 Documentation
│   ├── architecture.md             #   File ini — panduan arsitektur
│   ├── baseline-notes.md           #   Log verifikasi per fase
│   └── decisions/                  #   ADR (Architecture Decision Records)
│
├── .github/workflows/ci.yml        # CI: ruff + pytest
├── Makefile                        # up/down/crawl/smoke/test/lint
├── ruff.toml                       # Linter config
├── CLAUDE.md                       # AI assistant guide
├── README.md                       # User-facing docs
└── TASKS.md                        # Development roadmap
```

---

## Pola desain yang dipakai

### 1. Medallion Architecture (Bronze → Silver → Gold)

Standar industri yang dipopulerkan Databricks.

| Layer | Pertanyaan | Data |
|---|---|---|
| Bronze | "Apa yang terjadi?" | Raw JSON dari Kafka |
| Silver | "Apa yang valid?" | Typed, deduplicated, bersih |
| Gold | "Apa yang penting?" | Star schema siap query BI |

Keuntungan:
- **Replay:** kalau logic silver berubah, re-process dari bronze tanpa crawl ulang
- **Debug:** kalau data gold aneh, trace balik ke silver → bronze → Kafka offset
- **Audit:** bronze simpan semua yang pernah di-crawl sebagai historical record

### 2. Open/Closed Principle (Crawler)

Tambah crawler type baru **tanpa ubah engine**. Tambah marketplace baru **tanpa ubah framework**.

Caranya: subclass `Controllers`, daftarin di `CONTROLLER_REGISTRY` di `main.py`. File `controllers/__init__.py`, `helpers/`, dan output drivers tidak pernah disentuh.

### 3. Config-driven

Semua URL, topic, index, header, credential dikonfigurasi dari env vars (atau `config.yaml`). Tidak ada hardcode.

Priority: **CLI args > env vars > config.yaml > .env > defaults**

Prefix: `TOKOPEDIA_`, delimiter: `__` (contoh: `TOKOPEDIA_CRAWLER__RATE_LIMIT_RPS=2.0`)

### 4. Typed Contracts

Setiap data yang lewat antar sistem punya Pydantic schema. Request ke API, response dari API, event ke Kafka, semua validated.

```python
class TokopediaProduct(BaseModel):
    id: str
    name: str
    url: str
    price: TokopediaPrice
    shop: TokopediaShop
    rating: float | None
```

Kalau schema Tokopedia berubah, test langsung gagal, ketahuan sebelum production.

### 5. Idempotency

Pipeline bisa di-rerun kapan aja tanpa takut duplikat.

| Layer | Strategi |
|---|---|
| Bronze | Delta checkpoint (exactly-once dari Kafka) |
| Silver | `mode("overwrite")` — full rebuild |
| Gold | dbt `table` materialization (CREATE OR REPLACE) |
| Postgres | DROP TABLE + CREATE TABLE AS SELECT |
| ClickHouse dims | ReplacingMergeTree + OPTIMIZE FINAL |
| ClickHouse fct | DROP PARTITION → INSERT |

---

## Database dan datanya

### Tabel di setiap layer

#### Bronze (`lakehouse/bronze/products`)

| Kolom | Tipe | Keterangan |
|---|---|---|
| value_json | string | JSON mentah dari crawler |
| kafka_topic | string | Nama topic Kafka |
| kafka_partition | int | Partisi Kafka |
| kafka_offset | long | Offset dalam partisi |
| kafka_timestamp | timestamp | Timestamp Kafka |
| ingested_at | timestamp | Kapan data masuk bronze |

#### Silver (`lakehouse/silver/products`)

| Kolom | Tipe | Keterangan |
|---|---|---|
| product_id | string | ID produk Tokopedia |
| product_name | string | Nama produk |
| product_url | string | URL produk |
| rating | double | Rating (0.0–5.0) |
| price_idr | long | Harga dalam Rupiah |
| discount_pct | int | Persentase diskon |
| shop_id | string | ID toko |
| shop_name | string | Nama toko |
| shop_city | string | Kota toko |
| shop_tier | int | Tier toko (1=Official, 2=Gold, dst) |
| crawled_at | timestamp | Kapan produk di-crawl |

#### Gold (DuckDB) & Mart (Postgres/ClickHouse)

Tiga tabel star schema:

**dim_product** — Dimensi produk (latest state)

| Kolom | Key |
|---|---|
| product_id | PK |
| product_name | |
| product_url | |
| shop_id | FK → dim_shop |
| last_seen_at | |

**dim_shop** — Dimensi toko (latest state)

| Kolom | Key |
|---|---|
| shop_id | PK |
| shop_name | |
| shop_city | |
| shop_tier | |
| last_seen_at | |

**fct_product_snapshot** — Fakta snapshot harga per crawl

| Kolom | Key |
|---|---|
| snapshot_id | PK (`md5(product_id + crawled_at)`) |
| product_id | FK → dim_product |
| shop_id | FK → dim_shop |
| price_idr | |
| discount_pct | |
| rating | |
| crawled_at | |

---

## Cara menjalankan (berdasarkan situasi)

### "Gw cuma mau lihat crawler jalan"

```bash
cd source
python main.py crawler --mode scrape --type search-product --keyword "poco f8" --pretty
```

Output: 20 produk Tokopedia dalam JSON. Tidak butuh Docker, Kafka, atau infrastruktur apa pun.

### "Gw mau pipeline lengkap end-to-end"

```bash
make up                                    # start semua service
docker exec airflow cat /opt/airflow/standalone_admin_password.txt  # ambil password
make smoke KEYWORD="poco f8"              # setup + crawl → Kafka
# Buka Airflow UI http://localhost:8080, trigger DAG tokopedia_products
make test-all                             # verifikasi semua test
```

### "Gw mau tambah crawler baru (misal: shop-product)"

1. Tambah GraphQL query ke `library/graphql_queries.py`
2. Tambah schema di `library/schemas.py`
3. Tambah API method di `library/tokopedia_api.py`
4. Buat controller baru di `controllers/tokopedia/`
5. Register di `CONTROLLER_REGISTRY` di `main.py`
6. Bikin test

Lihat `CLAUDE.md` section "Crawler extension guide" untuk detail.

### "Gw mau production"

- Ganti `MINIO_ENDPOINT` ke AWS S3
- Ganti `KAFKA_BOOTSTRAP` ke MSK
- Ganti `POSTGRES_DSN` ke RDS
- Deploy Airflow ke MWAA
- Semua config-driven — ganti env vars, bukan kode.

---

## FAQ

### Kenapa DuckDB, bukan langsung Spark SQL?

Spark SQL bisa, tapi DuckDB lebih ringan untuk data kecil (embedded, no cluster overhead). dbt-DuckDB combo sangat cepat untuk development local. Kalau data gede (>10 GB), bisa switch ke Spark SQL atau Trino.

### Kenapa MinIO, bukan langsung S3?

MinIO = S3-compatible API, gratis, jalan local. Kalau pindah ke AWS S3, ganti endpoint URL aja — kode tidak berubah.

### Kenapa ada Postgres DAN ClickHouse?

Postgres = general-purpose, row-oriented. Cocok untuk operational query. ClickHouse = column-oriented, di-design untuk analytics (time-series, agregasi). Dashboard BI query ke ClickHouse jauh lebih cepat daripada ke Postgres.

### Apakah pipeline ini production-ready?

Untuk portfolio: ya. Untuk production skala besar: sudah cukup dekat. Yang perlu ditambah:
- Monitoring & alerting (DataDog/Prometheus)
- CI/CD deployment
- Security (secret management, bukan env vars)
- Data retention policy
- Incremental processing untuk data besar
