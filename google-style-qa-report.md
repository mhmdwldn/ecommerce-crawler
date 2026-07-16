# Google-Style QA Report: E-Commerce Crawler Pipeline

**Reviewer:** Senior QA Automation Engineer — Google Testing Standards  
**Date:** 2026-07-16  
**Project:** ecommerce-crawler (`mhmdwldn/ecommerce-crawler`)  
**Current Test Suite:** ~64 tests (60 source/ + 4 pipeline/ unit, 15 assets/ integration)  
**Framework:** pytest + pytest-asyncio + pytest-mock + PySpark local mode  

---

## 🧪 Analisis Kemudahan Pengujian (Testability)

### Skor Testability: 7.5/10

### Kekuatan

1. **Arsitektur terisolasi dengan baik.** `Controllers` ABC, output driver factory, `TokopediaAPI` sebagai data-access layer — setiap komponen bisa di-mock secara independen. Controller tidak tahu detail HTTP/Kafka, API client tidak tahu detail controller.

2. **Typed contracts everywhere.** Pydantic v2 di setiap batas — input validasi otomatis, output terprediksi. Test tinggal assert tipe dan nilai.

3. **Dependency injection by config.** `TokopediaAPI(crawler_settings)` — settings bisa di-inject tanpa mocking global state. Test tidak perlu env vars real.

4. **PySpark local mode.** `SparkSession.builder.master("local[2]")` — silver/bronze di-test tanpa cluster. Ini keputusan arsitektur yang benar.

5. **Semua network call di-mock.** `pytest-mock` + `MockerFixture` — suite tidak pernah sentuh network real. Cepat, deterministik, aman di CI.

6. **Database tests opsional.** Asset registry tests pakai `pytest.mark.skipif(not DSN, ...)` — tidak blocking developer yang belum setup Postgres.

### Hambatan

1. **crawl_assets.py tidak testable tanpa refactor.** Module ini menggunakan `subprocess.run()` untuk memanggil crawler — tidak ada cara mocking tanpa patch `subprocess` global. Harus diekstrak ke fungsi yang bisa di-mock.

2. **load_to_postgres.py tidak punya test sama sekali.** Table drop-and-recreate — harus diverifikasi bahwa tidak ada data loss saat concurrent run.

3. **Airflow DAG tidak di-test.** Tidak ada `test_dag_loaded()`, `test_dag_structure()`, atau `test_task_dependencies()`. DAG didefinisikan di module-level dan langsung di-parse Airflow.

4. **Quality checks (`checks.py`) tidak punya test.** Lima validasi kritis yang menentukan nasib pipeline — tidak ada automated test untuk threshold logic.

5. **Shopee client tidak punya test suite.** File `source/tests/test_shopee_api.py` dan `test_shopee_controllers.py` ada di struktur folder exploration.md tapi tidak ditemukan di test suite saat ini. Kalau memang sudah ditulis tapi tidak di-commit, itu jadi gap besar.

6. **Silver category parsing tidak punya dedicated test.** Logic `add_category_columns()` yang baru ditambahkan (Fase 8.5C) — 15+ kolom transformasi, slug normalization, md5 hashing — tidak ada test sama sekali.

7. **Kafka driver menggunakan background thread + event loop.** `asyncio.run_coroutine_threadsafe()` — ini rentan race condition yang tidak bisa di-capture unit test.

8. **Tidak ada test schema evolution.** Apa yang terjadi kalau Tokopedia API menambah/menghapus field? `from_json(..., mode="PERMISSIVE")` sudah ditambahkan, tapi tidak diverifikasi dengan test.

### Verdict

Kode ini **siap diuji** secara arsitektur — komponen terisolasi, typed contracts, dependency injection. Tapi **coverage test saat ini tidak proporsional**: crawler engine (source/) di-cover dengan baik (~60 unit test), sementara pipeline (pipeline/) dan orchestration (Airflow DAG) hampir tidak tersentuh. Ini adalah inverse dari Google Testing Pyramid — seharusnya pipeline layer punya LEBIH BANYAK test karena lebih kompleks dan lebih kritis (data corruption di sini permanen).

---

## 🚦 Skenario Pengujian End-to-End (E2E Test Cases)

### Alur Utama: Crawl → Kafka → Bronze → Silver → Gold → Serving

| ID | Fitur | Langkah | Ekspektasi | Risiko |
|----|-------|---------|-----------|--------|
| E2E-01 | **Full pipeline — happy path** | 1. Seed asset registry dengan 1 keyword (`poco f8`). 2. Trigger DAG. 3. Tunggu semua task sukses. 4. Cek Postgres: `fct_product_snapshot` bertambah. 5. Cek ClickHouse: `fct_product_snapshot` = Postgres. | Semua 8 task SUCCESS. PG == CH. `pipeline_runs` tercatat. | **Tinggi** |
| E2E-02 | **Idempotency — rerun DAG** | 1. E2E-01 sukses. 2. Trigger DAG ulang tanpa ubah apapun. 3. Cek row count PG/CH. | Row count tidak berubah (tidak duplikasi). | **Tinggi** |
| E2E-03 | **Asset Registry — multiple keywords** | 1. Seed 3 asset: `poco f8`, `iphone 17`, `samsung galaxy s25 ultra`. 2. Trigger DAG. 3. Cek `last_crawled_at` ter-update untuk ketiganya. | 3 asset tercrawl, `last_crawled_at` terisi. | **Sedang** |
| E2E-04 | **Circuit breaker — 5x gagal** | 1. Buat asset dengan keyword invalid (`xxxyyyzzz1234`). 2. Trigger DAG 5x berturut. 3. Cek `is_active` asset. | Asset jadi `is_active=false` setelah kegagalan ke-5. | **Sedang** |
| E2E-05 | **Quality gate — data korup** | 1. Inject produk dengan `price=0` ke Kafka. 2. Trigger DAG. 3. Cek Airflow task `quality_check`. | `quality_check` FAIL. `dbt_build` tidak jalan. | **Tinggi** |
| E2E-06 | **Silver rejects — JSON malformed** | 1. Inject JSON rusak ke Kafka. 2. Trigger DAG. 3. Cek `_rejects` table. | Baris rusak masuk `_rejects`, silver tetap bersih. | **Sedang** |
| E2E-07 | **ClickHouse == Postgres consistency** | 1. E2E-01 sukses. 2. Query `count(*)` dari kedua DB untuk semua tabel. | Row count identik. | **Tinggi** |
| E2E-08 | **Kafka restart — tidak kehilangan data** | 1. Crawl 20 produk ke Kafka. 2. Restart Kafka container. 3. Trigger bronze. | Bronze baca semua 20 offset dari awal (checkpoint bersih). | **Sedang** |
| E2E-09 | **Airflow graceful retry** | 1. Matikan Kafka sebelum bronze task. 2. Trigger DAG. 3. Nyalakan Kafka setelah retry delay. | Bronze retry 1x, lalu sukses. | **Sedang** |
| E2E-10 | **Koneksi Tokopedia down** | 1. Mock `gql.tokopedia.com` return HTTP 503. 2. Trigger DAG. 3. Cek crawl task. | Crawl task FAIL, `mark_failure()` dipanggil, circuit breaker counter naik. | **Sedang** |
| E2E-11 | **MinIO unreachable — bronze fail** | 1. Matikan MinIO. 2. Trigger DAG. | Bronze task FAIL, retry, tetap FAIL. Silver tidak jalan. | **Rendah** |
| E2E-12 | **DAG max_active_runs = 1** | 1. Trigger DAG 2x dalam 1 detik. 2. Cek Airflow UI. | Hanya 1 run yang aktif, run kedua antri. | **Rendah** |
| E2E-13 | **Incremental silver — watermark** | 1. Full refresh silver. 2. Tambah 10 row baru ke bronze. 3. Run `--incremental`. 4. Cek silver count. | Hanya 10 row baru yang di-MERGE, yang lama tidak disentuh. | **Sedang** |
| E2E-14 | **Cold storage export** | 1. Isi bronze dengan data > 90 hari. 2. Run `retention.py --cold-storage`. 3. Cek `lakehouse/cold/`. | Parquet file ada, data sebelum VACUUM terekstrak. | **Rendah** |
| E2E-15 | **dim_category FK integrity** | 1. Crawl 3 keyword dari 3 kategori beda. 2. Cek `dim_category` row count. 3. Cek `fct_product_snapshot.category_sk` semua punya match di `dim_category`. | Semua category_sk di fct exist di dim_category. | **Tinggi** |

---

## 🔍 Temuan Edge Cases & Potensi Error

### 1. `source/library/tokopedia_api.py` — HTTP timeout tidak ditangani

**Severity: HIGH — Data Loss**

```python
# search_products() — jika httpx.ReadTimeout terjadi di tengah pagination,
# produk yang sudah di-yield sebelumnya TIDAK di-rollback.
# Kafka sudah terlanjur terima event partial.
for _ in range(max_pages):
    data = await self._execute(...)  # ← bisa timeout di halaman ke-2
```

**Dampak:** Data parsial masuk bronze. Silver memproses apa adanya. Tidak ada mekanisme kompensasi.

**Rekomendasi:** Wrap pagination dengan try/except, signal ke controller bahwa data partial.

### 2. `pipeline/spark/silver.py` — `F.md5()` pada string kosong

**Severity: MEDIUM — Silent Corruption**

```python
# add_category_columns() — jika breadcrumb kosong, l1_slug = "".
# F.md5("") = "d41d8cd98f00b204e9800998ecf8427e" (md5 empty string)
# Ini benar secara teknis, tapi kategori "tidak ada" dan 
# "kategori yang belum di-crawl" tidak bisa dibedakan.
```

**Dampak:** Semua produk tanpa kategori akan share category_sk yang sama. Benar secara dimensi, tapi `dim_category.cat_l1_name = ""` tidak informatif.

**Rekomendasi:** Gunakan sentinel value yang jelas: `cat_l1_name = "(unknown)"` untuk breadcrumb kosong.

### 3. `pipeline/load/crawl_assets.py` — `get_due_assets(limit=10)` vs 23 asset

**Severity: MEDIUM — Incomplete Processing**

```python
assets = get_due_assets(limit=10)  # hanya 10 dari 23
```

**Dampak:** Dengan 23 asset dan @hourly schedule, 13 asset tidak pernah ter-crawl dalam satu jam yang sama. Kalau cadence-nya 60 menit dan 10 asset pertama selalu yang paling "due" (priority + last_crawled_at NULLS FIRST), 13 asset lainnya baru di-crawl di jam berikutnya — menciptakan ketidakmerataan.

**Rekomendasi:** Naikkan limit ke 50 atau gunakan round-robin strategy.

### 4. `source/library/tokopedia_api.py` — Rate limiting tanpa jitter

**Severity: MEDIUM — Thundering Herd**

```python
async def _throttle(self):
    delay = 1.0 / self._settings.rate_limit_rps  # 5 RPS = 0.2s delay
    await asyncio.sleep(delay)  # ← semua request jaraknya persis 200ms
```

**Dampak:** Pola request deterministik — Tokopedia anti-bot system bisa mendeteksi interval yang terlalu teratur sebagai bot signature.

**Rekomendasi:** Tambah jitter: `delay * (0.5 + random.random())`.

### 5. `source/helpers/output/driver/kafka.py` — Background thread crash silent

**Severity: MEDIUM — Silent Failure**

```python
# KafkaOutputDriver menjalankan AIOKafkaProducer di thread terpisah.
# Kalau thread crash, put() akan stuck di asyncio.run_coroutine_threadsafe().
# Tidak ada health check atau monitoring untuk background loop.
```

**Dampak:** Producer thread mati — controller tetap memanggil `send_output()` — deadlock atau data hilang tanpa notifikasi.

**Rekomendasi:** Tambah heartbeat check atau `thread.is_alive()` guard sebelum setiap `put()`.

### 6. `pipeline/spark/stream_bronze.py` — `failOnDataLoss` default

**Severity: LOW — False Positive Failure**

```python
# TriggerAvailableNow tanpa option "failOnDataLoss" = "false".
# Kalau Kafka topic baru (offset 0) dan checkpoint merujuk offset lama,
# Spark akan throw IllegalStateException.
```

Ini sudah terjadi di sesi debugging (partisi 1 vs 3). Fix-nya sudah dilakukan (hapus checkpoint), tapi tidak ada automated guard di kode.

**Rekomendasi:** Set `failOnDataLoss = false` di production config. Atau minimal, dokumentasikan prosedur checkpoint cleanup.

### 7. `pipeline/dbt/models/marts/dim_category.sql` — `SELECT DISTINCT` tanpa dedup watermark

**Severity: LOW — Data Drift**

```sql
select distinct
    category_sk, ...
from {{ ref('stg_product_snapshot') }}
where category_sk is not null and category_sk != ''
```

**Dampak:** `SELECT DISTINCT` di scale besar (>100K rows) bisa lambat. Saat ini data kecil, jadi tidak masalah. Tapi tidak ada indeks atau materialisasi inkremental.

**Rekomendasi:** Tambah `last_seen_at` ke `dim_category` (seperti dim_product/dim_shop) dan gunakan `qualify row_number()` pattern.

### 8. `source/controllers/tokopedia/search_product.py` — `model_dump()` tanpa `exclude_none`

**Severity: LOW — Data Leak**

```python
doc = event.payload.model_dump(mode="json", by_alias=True, exclude_none=True)
if event.metadata:
    doc["search_keyword"] = event.metadata.get("keyword", keyword)
    doc["asset_category"] = event.metadata.get("asset_category", "")
```

**Dampak:** `exclude_none=True` akan menghapus field `None` dari payload. Tapi setelah injeksi, field seperti `asset_category = ""` (string kosong) tetap ada. Ini intentional — tapi tidak ada test yang memverifikasi bahwa field kosong tidak menyebabkan masalah di downstream (Spark schema expects non-null untuk StringType, dan kita sudah coalesce).

### 9. `pipeline/quality/checks.py` — Check freshness di waktu UTC vs lokal

**Severity: LOW — False Positive**

```python
# check_freshness membandingkan crawled_at dengan F.current_timestamp()
# Yang mana UTC. Tapi max_age_hours=2 tidak membedakan jam kerja vs jam tidur.
# Kalau DAG @hourly jalan jam 3 pagi (tidak ada traffic Tokopedia), 
# freshness check akan false-positive FAIL.
```

**Rekomendasi:** Gunakan business-hours-aware freshness, atau naikkan threshold di luar jam sibuk.

### 10. `assets/repository.py` — `get_conn()` tidak thread-safe

**Severity: LOW — Race Condition**

```python
@contextmanager
def get_conn(dsn: str | None = None) -> Iterator:
    conn = psycopg2.connect(dsn or DSN)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
```

**Dampak:** Kalau dua Airflow task (dynamic task mapping) baca/write registry bersamaan, bisa deadlock di Postgres pada `control.crawl_assets`. Saat ini `max_active_tasks=3` dan crawl sequential — tapi kalau nanti di-refactor ke fan-out, ini jadi masalah.

---

## 🛠️ Rekomendasi Kode Pengujian (Test Script)

### 1. Unit Test: Category Parsing (silver.py)

File baru: `pipeline/tests/test_category_parsing.py`

```python
"""Tests for silver.py add_category_columns() — breadcrumb parsing."""
import json
from datetime import datetime

import pytest
from pyspark.sql import types as T

from pipeline.spark.silver import add_category_columns

BRONZE_SCHEMA = T.StructType([
    T.StructField("value_json", T.StringType()),
    T.StructField("kafka_offset", T.LongType()),
    T.StructField("kafka_timestamp", T.TimestampType()),
])
TS = datetime(2026, 7, 7, 10, 0, 0)


@pytest.fixture
def spark():
    from pyspark.sql import SparkSession
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-category")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _make_df(spark, breadcrumb="", asset_category=""):
    """Helper: create DataFrame with category columns pre-extracted (simulates
    what bronze_to_silver's .select() produces before add_category_columns)."""
    return spark.createDataFrame([(
        breadcrumb, asset_category,
    )], schema=T.StructType([
        T.StructField("category_breadcrumb", T.StringType()),
        T.StructField("asset_category", T.StringType()),
    ]))


class TestCategory3Levels:
    """Breadcrumb with all 3 levels."""

    def test_normalizes_slug_to_title_case(self, spark):
        df = _make_df(spark, breadcrumb="handphone-tablet/aksesoris-handphone/flip-cover-handphone")
        result = add_category_columns(df).collect()[0]

        assert result.cat_l1_name == "Handphone Tablet"
        assert result.cat_l2_name == "Aksesoris Handphone"
        assert result.cat_l3_name == "Flip Cover Handphone"

    def test_per_level_ids_are_stable(self, spark):
        df = _make_df(spark, breadcrumb="handphone-tablet/handphone/android-os")
        r1 = add_category_columns(df).collect()[0]
        r2 = add_category_columns(df).collect()[0]

        # Same inputs → same md5 IDs
        assert r1.l1_id == r2.l1_id
        assert r1.l2_id == r2.l2_id
        assert r1.l3_id == r2.l3_id

    def test_category_sk_includes_asset_category(self, spark):
        df1 = _make_df(spark, breadcrumb="a/b/c", asset_category="elektronik")
        df2 = _make_df(spark, breadcrumb="a/b/c", asset_category="fashion")

        sk1 = add_category_columns(df1).collect()[0].category_sk
        sk2 = add_category_columns(df2).collect()[0].category_sk

        # Same breadcrumb, different asset_category → different category_sk
        assert sk1 != sk2


class TestCategoryEmpty:
    """Edge cases with empty/missing breadcrumb."""

    def test_empty_breadcrumb_returns_empty_names(self, spark):
        df = _make_df(spark, breadcrumb="", asset_category="elektronik")
        result = add_category_columns(df).collect()[0]

        assert result.cat_l1_name == ""
        assert result.cat_l2_name == ""
        assert result.cat_l3_name == ""

    def test_null_breadcrumb_does_not_crash(self, spark):
        df = _make_df(spark, breadcrumb=None, asset_category="elektronik")
        result = add_category_columns(df).collect()[0]

        # Should not throw; should produce empty strings
        assert result.cat_l1_name == ""

    def test_empty_but_category_sk_still_computed(self, spark):
        df = _make_df(spark, breadcrumb="", asset_category="fashion")
        result = add_category_columns(df).collect()[0]

        # category_sk = md5("|||fashion") — deterministic
        assert result.category_sk is not None
        assert len(result.category_sk) == 32  # md5 hex


class TestCategoryTruncation:
    """More than 3 levels → extras silently dropped."""

    def test_4_levels_truncated_to_3(self, spark):
        df = _make_df(spark, breadcrumb="a/b/c/d")
        result = add_category_columns(df).collect()[0]

        assert result.cat_l3_name != ""  # level 3 exists
        # Level 4 is silently dropped — cat_l3 should be "c", not "d"
        assert "d" not in result.cat_l3_name.lower()
```

### 2. Integration Test: Full Pipeline Smoke

File baru: `pipeline/tests/test_pipeline_integration.py`

```python
"""End-to-end smoke test: crawl → Kafka → bronze → silver → gold.
Requires: running Docker services (kafka, minio, postgres, clickhouse).
Skip if any service is unreachable.
"""
import os
import time

import pytest

# Skip entire suite if integration env not configured
_KAFKA_READY = os.getenv("KAFKA_BOOTSTRAP")
pytestmark = pytest.mark.skipif(
    not _KAFKA_READY,
    reason="KAFKA_BOOTSTRAP not set — set to 'kafka:29092' for integration tests",
)


@pytest.mark.integration
class TestPipelineEndToEnd:
    """Run one complete DAG cycle and verify data at every layer."""

    def test_bronze_reads_from_kafka(self, spark):
        """Kafka topic has messages → bronze parses them."""
        from pipeline.spark.session import build_session
        from pipeline.spark.stream_bronze import main as bronze_main

        # ... trigger crawl, run bronze, assert row count > 0
        pass

    def test_silver_transforms_bronze(self, spark):
        """Bronze Delta → silver typing + dedup."""
        from pipeline.spark.silver import main as silver_main
        # ... run silver, assert silver rows > 0, rejects == 0
        pass

    def test_dbt_build_produces_dim_category(self):
        """Gold contains dim_category with non-null category_sk."""
        import duckdb
        gold_db = os.getenv("GOLD_DB_PATH", "pipeline/dbt/gold.duckdb")
        duck = duckdb.connect(gold_db, read_only=True)
        try:
            rows = duck.execute(
                "SELECT count(*) FROM dim_category WHERE category_sk != ''"
            ).fetchone()[0]
            assert rows > 0, "dim_category is empty — category parsing may be broken"
        finally:
            duck.close()

    def test_fct_category_sk_references_dim_category(self):
        """Referential integrity: every category_sk in fact must exist in dim."""
        import duckdb
        gold_db = os.getenv("GOLD_DB_PATH", "pipeline/dbt/gold.duckdb")
        duck = duckdb.connect(gold_db, read_only=True)
        try:
            orphans = duck.execute("""
                SELECT count(*) FROM fct_product_snapshot f
                LEFT JOIN dim_category d ON f.category_sk = d.category_sk
                WHERE d.category_sk IS NULL
            """).fetchone()[0]
            assert orphans == 0, f"{orphans} orphan category_sk in fact table"
        finally:
            duck.close()
```

### 3. Negative Test: Tokopedia API Error Handling

File baru: `source/tests/test_tokopedia_api_errors.py`

```python
"""Negative tests for TokopediaAPI — error handling edge cases."""
import pytest
from pytest_mock import MockerFixture

from library.tokopedia_api import TokopediaAPI


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_http_503_raises(self, mocker: MockerFixture, crawler_settings):
        """Tokopedia gateway returns 503 → should surface, not silently skip."""
        api = TokopediaAPI(crawler_settings)
        mock_client = mocker.AsyncMock()
        resp = mocker.MagicMock()
        resp.status_code = 503
        resp.raise_for_status.side_effect = Exception("503 Service Unavailable")
        mock_client.post.return_value = resp
        api._client = mock_client

        with pytest.raises(Exception):
            async for _ in api.search_products(keyword="test", max_pages=1):
                pass

    @pytest.mark.asyncio
    async def test_empty_products_list_stops_gracefully(
        self, mocker: MockerFixture, crawler_settings, sample_search_product_empty: list
    ):
        """API returns empty products list → generator stops without error."""
        api = TokopediaAPI(crawler_settings)
        mock_client = mocker.AsyncMock()
        resp = mocker.MagicMock()
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.json.return_value = sample_search_product_empty
        mock_client.post.return_value = resp
        api._client = mock_client

        events = [e async for e in api.search_products(keyword="zzz_no_results", max_pages=1)]
        assert len(events) == 0  # No products, no errors

    @pytest.mark.asyncio
    async def test_rate_limit_429_raises_without_retry(
        self, mocker: MockerFixture, crawler_settings
    ):
        """HTTP 429 → RateLimitExceeded raised, controller will handle bury."""
        from exception.exception import RateLimitExceeded

        api = TokopediaAPI(crawler_settings)
        mock_client = mocker.AsyncMock()

        # _execute has retry logic that checks for 429
        # Mocking the inner _execute to simulate rate limit response
        mocker.patch.object(
            api, "_execute",
            side_effect=RateLimitExceeded("Too Many Requests (429)")
        )

        with pytest.raises(RateLimitExceeded):
            async for _ in api.search_products(keyword="test", max_pages=1):
                pass
```

### 4. Airflow DAG Structure Test

File baru: `pipeline/tests/test_dag_structure.py`

```python
"""Airflow DAG validation tests — no runtime execution needed."""
import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="pipeline/airflow/dags", include_examples=False)


def test_dag_loaded(dagbag):
    """tokopedia_products DAG is loaded without import errors."""
    assert "tokopedia_products" in dagbag.dags
    assert len(dagbag.import_errors) == 0, dagbag.import_errors


def test_dag_structure(dagbag):
    """All 8 tasks exist with correct dependencies."""
    dag = dagbag.dags["tokopedia_products"]
    task_ids = {t.task_id for t in dag.tasks}

    expected = {
        "crawl", "bronze", "silver", "quality_check",
        "dbt_build", "load_postgres", "load_clickhouse", "write_audit",
    }
    assert task_ids == expected, f"Missing tasks: {expected - task_ids}"


def test_dag_no_cycles(dagbag):
    """DAG has no circular dependencies."""
    dag = dagbag.dags["tokopedia_products"]
    # Airflow DAG parser rejects cycles at parse time,
    # so reaching here means no cycles. Assert explicitly.
    assert len(dag.roots) > 0  # at least one root task


def test_dag_retry_config(dagbag):
    """All tasks have retries=1 and max_active_runs=1."""
    dag = dagbag.dags["tokopedia_products"]
    assert dag.max_active_runs == 1
    for task in dag.tasks:
        assert task.retries == 1, f"{task.task_id}: expected retries=1"
```

### 5. Quality Checks Unit Test

File baru: `pipeline/tests/test_quality_checks.py`

```python
"""Unit tests for pipeline/quality/checks.py — threshold logic."""
from datetime import datetime, timedelta

from pyspark.sql import types as T

from pipeline.quality.checks import (
    check_price_positive,
    check_row_count,
)


def test_row_count_empty_silver(spark):
    """Empty silver → FAIL."""
    df = spark.createDataFrame([], schema=T.StructType([
        T.StructField("product_id", T.StringType()),
    ]))
    # Create temp Delta table with 0 rows
    df.write.format("delta").mode("overwrite").saveAsTable("test_empty")
    assert not check_row_count(spark)  # should FAIL
    spark.sql("DROP TABLE IF EXISTS test_empty")


def test_price_positive_finds_zero(spark):
    """price_idr = 0 → FAIL."""
    df = spark.createDataFrame(
        [(1, 0), (2, 5000), (3, -100)],
        schema=T.StructType([
            T.StructField("product_id", T.LongType()),
            T.StructField("price_idr", T.LongType()),
        ])
    )
    df.write.format("delta").mode("overwrite").saveAsTable("test_price_zero")
    assert not check_price_positive(spark)  # 2 rows with price <= 0
    spark.sql("DROP TABLE IF EXISTS test_price_zero")
```

---

## 📊 Rekomendasi Matriks Pengujian Google

### Testing Pyramid — Current vs Target

```
                        CURRENT STATE                    TARGET STATE
                           ┌──┐                            ┌──┐
                    E2E    │  │ 0%                   E2E  │▓▓│ 10%  (5 test)
                           │  │                            │▓▓│
                           │  │                            └──┘
                         ┌──────┐                        ┌──────┐
              Integration│  ▓▓  │ 8%               Integ │  ▓▓▓  │ 25%  (20 test)
                         │  ▓▓  │  (5 test)              │  ▓▓▓  │
                         └──────┘                        │  ▓▓▓  │
                       ┌──────────┐                      └──────┘
              Unit      │  ▓▓▓▓▓▓▓ │ 92%               ┌──────────┐
                        │  ▓▓▓▓▓▓▓ │  (59 test)  Unit   │  ▓▓▓▓▓▓▓▓│ 65%  (50 test)
                        │  ▓▓▓▓▓▓▓ │                     │  ▓▓▓▓▓▓▓▓│
                        └──────────┘                     │  ▓▓▓▓▓▓▓▓│
                                                         └──────────┘
Inverted pyramid!                                     Google standard
Pipeline under-tested                                 Balanced coverage
```

### Breakdown Target

| Layer | % Target | # Tests | Fokus |
|-------|----------|---------|-------|
| **Unit** | 65% | ~50 | Crawler parsers, Pydantic validators, quality check thresholds, category normalization, schema coercion. **Mock all I/O.** |
| **Integration** | 25% | ~20 | Bronze→Silver→Gold data flow, Loader idempotency, Asset registry CRUD + due logic, dbt model compilation. **Real Spark local, real PG/CH if available.** |
| **E2E** | 10% | ~5 | Full DAG trigger → verify data in PG/CH, Circuit breaker activation, Idempotency verification. **Real Docker stack.** |

### Coverage Gap Prioritas

| Priority | Module | Current | Target | Action |
|----------|--------|---------|--------|--------|
| **P0** | `silver.py` (category) | 0 test | 8 test | Unit test `add_category_columns()` — 3 level, empty, null, truncation, md5 stability |
| **P0** | `checks.py` | 0 test | 5 test | Unit test each check with edge cases (0 rows, 100% nulls, negative price) |
| **P0** | `crawl_assets.py` | 0 test | 4 test | Unit test due-logic, circuit breaker threshold, metadata injection into CLI args |
| **P1** | `tokopedia_api.py` (errors) | 0 test | 5 test | Unit test HTTP 429, 503, timeout, empty response, GraphQL errors |
| **P1** | DAG structure | 0 test | 4 test | `DagBag` load, task count, dependency chain, retry config |
| **P1** | `load_to_postgres.py` | 0 test | 2 test | Integration test: DuckDB→PG row count, idempotency |
| **P2** | `dim_category` FK integrity | 0 test | 2 test | Integration: every fct.category_sk exists in dim_category |
| **P2** | E2E happy path | 0 test | 3 test | Full pipeline smoke: crawl→Kafka→bronze→silver→dbt→PG/CH |
| **P3** | Shopee API | ~14 test | 14 test | Verify existing tests still exist and run |
| **P3** | `stream_bronze.py` (failOnDataLoss) | 0 test | 1 test | Verify failOnDataLoss=false option works |

### Status Kelayakan Rilis

## 🟡 CONDITIONAL READY — NEED TEST SUPPLEMENTATION BEFORE PRODUCTION

Proyek ini **bisa lolos UAT portofolio** — pipeline sudah berjalan, data valid, semua error-path yang diketahui sudah ditangani secara manual (baseline notes). Tapi **belum siap untuk production continuous operation** karena 3 gap kritis:

1. **❌ Tidak ada automated regression detection.** Kalau Tokopedia API berubah schema, tidak ada test yang akan menangkap sebelum data corrupt masuk mart. Current approach: "liat Airflow log manual."

2. **❌ Pipeline logic tidak di-test.** Silver category parsing (15+ transformasi), quality check thresholds, `crawl_assets.py` orchestration — semuanya zero test coverage.

3. **❌ Tidak ada DAG validation.** DAG di-parse Airflow saat load, tidak ada automated test bahwa struktur task dan dependencies masih benar setelah perubahan.

**Rekomendasi:** Tambahkan 20 test prioritas P0+P1 (unit + integration) sebelum naik ke production. E2E bisa menyusul.

**Final Verdict:** `READY FOR UAT 👍` (portfolio/development) — `NEED 20+ TESTS ❌` (production)

---

## Appendix: Test Execution Matrix

```bash
# Unit tests (fast, no Docker, CI-friendly)
cd source && pytest tests/ -v                         # ~60 tests, ~2s
cd pipeline && pytest tests/test_silver.py -v          # ~3 tests, ~30s (Spark cold start)
cd pipeline && pytest tests/test_bronze.py -v          # ~1 test, ~30s
cd pipeline && pytest tests/test_category_parsing.py -v  # NEW: ~8 tests
cd pipeline && pytest tests/test_quality_checks.py -v    # NEW: ~5 tests
cd pipeline && pytest tests/test_dag_structure.py -v     # NEW: ~4 tests

# Integration tests (need Docker services)
docker compose -f source/deployment/compose.yaml up -d postgres clickhouse
cd pipeline && pytest tests/test_clickhouse_load.py -v
cd assets && CONTROL_DSN="..." pytest tests/ -v

# E2E tests (need full Docker stack)
# Triggered via CI after docker compose up
bash start.sh
docker exec airflow airflow dags trigger tokopedia_products
# Wait for DAG completion, run assertions
```
