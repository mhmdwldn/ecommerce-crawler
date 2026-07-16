# Google-Style Documentation — E-Commerce Crawler Pipeline

**Engineer:** Senior Python Engineer — Google Readability Certified  
**Date:** 2026-07-16  
**Scope:** All Python functions across `source/`, `pipeline/`, `assets/`  

All functions now follow **Google Python Style Guide**:
- **Docstrings:** Description, Args, Returns, Raises in `"""` blocks
- **Type Hints:** Explicit on all parameters and return values (PEP 484)
- **Inline Comments:** Only on non-obvious logic (`# why`, not `# what`)

---

## 1. `pipeline/quality/audit.py` — `main()`

```python
def main() -> None:
    """Write one audit row to ``analytics.pipeline_runs`` in ClickHouse.

    Reads pipeline state from env vars (set by Airflow DAG):
      - ``AIRFLOW_RUN_ID``: DAG run identifier (default ``"manual"``)
      - ``AIRFLOW_LOGICAL_DATE``: ISO 8601 execution date (default epoch)
      - ``AIRFLOW_TASK_STATE``: ``success`` / ``failed`` / ``unknown``

    Counts rows from silver Delta + gold DuckDB tables, computes duration,
    and inserts into ClickHouse. Connection cleanup in ``finally`` block.

    Raises:
        Does not raise — all errors are caught and logged.
    """
```

---

## 2. `pipeline/spark/stream_bronze.py` — `main()`

```python
def main() -> None:
    """Stream Kafka topic → Delta Lake bronze on MinIO (TriggerAvailableNow).

    Reads ``KAFKA_BOOTSTRAP``, ``KAFKA_TOPIC`` from env vars (defaults:
    ``localhost:9092`` / ``tokopedia.products.raw``). Uses ``failOnDataLoss=false``
    so the query does not crash when the topic is recreated with fewer partitions.
    Checkpoint at ``BRONZE_CHECKPOINT`` ensures exactly-once across runs.

    This is designed for Airflow: each DAG run drains whatever is new on the
    topic, then exits — no long-running streaming daemon needed.
    """
```

---

## 3. `pipeline/load/crawl_assets.py` — `main()`

```python
def main() -> None:
    """Crawl all due assets from the registry → Kafka (called by Airflow DAG).

    Workflow:
        1. Query ``control.v_due_assets`` for up to 50 due assets.
        2. For each asset, build CLI command with ``shlex.quote()``-safe
           arguments (including ``--asset-category`` and ``--asset-id``
           for registry metadata injection).
        3. Execute crawler via subprocess → crawl results → Kafka topic.
        4. Report success/failure back to registry (circuit breaker on
           ``MAX_CONSECUTIVE_FAILURES`` consecutive failures per asset).
        5. If no assets are due, fall back to ``CRAWL_KEYWORD`` default
           so the pipeline does not run dry.

    Raises:
        subprocess.CalledProcessError: if the fallback crawl command fails.
    """
```

---

## 4. `source/helpers/output/driver/kafka.py` — `put()` + `close()`

```python
def put(self, output: str, **kwargs) -> None:
    """Send *output* to the Kafka topic (thread-safe, synchronous).

    Includes background thread health check — if the producer loop died
    (e.g. broker unreachable), we detect it before silently dropping data.

    Args:
        output: JSON string to publish (encoded to UTF-8 bytes).
        **kwargs: Optional ``topic`` override.

    Raises:
        Does not raise — errors are logged, messages are dropped gracefully
        to avoid crashing the caller's event loop.
    """


def close(self) -> None:
    """Stop the background producer thread and cleanup the event loop.

    Blocks up to 10s for the producer to flush and stop, then 5s for
    the thread to join. Errors during shutdown are silently caught.
    """
```

---

## 5. `source/controllers/tokopedia/search_product.py` — `handler()` + `scrape_to_json()`

```python
async def handler(self, job: dict[str, Any]) -> None:
    """Execute the product search and pipe results to the output driver.

    Injects registry metadata (``asset_category``, ``asset_id``) into each
    product dict so it flows through Kafka → bronze → silver → dim_category.

    Args:
        job: dict with keys ``keyword``, ``rows``, ``max_pages``,
             ``asset_category``, ``asset_id``. CLI args take precedence.

    Raises:
        Does not raise — pagination errors are caught by the base controller.
    """


async def scrape_to_json(self, job: dict[str, Any]) -> list[dict[str, Any]]:
    """Scrape and return raw product dicts — no output driver involved.

    Args:
        job: dict with keys ``keyword``, ``rows``, ``max_pages``.

    Returns:
        List of raw product dicts (TokopediaProduct.model_dump).
    """
```

---

## 6. `pipeline/spark/silver.py` — `add_category_columns()` + `bronze_to_silver()` + `main()`

```python
def add_category_columns(df: DataFrame) -> DataFrame:
    """Parse Tokopedia breadcrumb slug into a 3-level category dimension.

    Args:
        df: DataFrame with columns ``category_breadcrumb`` (e.g.
            ``"handphone-tablet/handphone/android-os"``) and
            ``asset_category`` (e.g. ``"elektronik"`` from registry).

    Returns:
        DataFrame with added columns: ``cat_l1_name``, ``l1_id``,
        ``cat_l2_name``, ``l2_id``, ``cat_l3_name``, ``l3_id``,
        ``category_sk``. Empty breadcrumbs produce the sentinel label
        ``"(unknown)"``. Per-level IDs are ``md5(slug)``; composite
        surrogate key is ``md5(l1_id|l2_id|l3_id|asset_category)``.
        Extra breadcrumb levels beyond 3 are silently dropped.
    """


def bronze_to_silver(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Transform bronze rows into typed, deduplicated silver + rejects.

    Args:
        df: Bronze DataFrame with columns ``value_json``, ``kafka_offset``,
            ``kafka_timestamp``.

    Returns:
        Tuple of ``(silver_df, rejects_df)``. Silver has 20 typed columns
        including category dimension. Rejects contain unparseable rows (null
        id or malformed JSON). Deduplication: per ``(product_id, crawled_at)``,
        keep the row with the highest ``kafka_offset``.
    """


def main(incremental: bool = False) -> None:
    """Entry point for ``python -m pipeline.spark.silver``.

    Args:
        incremental: If True, use MERGE mode (watermark-based, only new rows).
            If False (default), full overwrite rebuild.
    """
```

---

## 7. `pipeline/quality/checks.py` — All 5 checks

```python
def check_row_count(spark, min_rows: int = _ROW_COUNT_MIN) -> bool:
    """Silver must have at least *min_rows* rows.

    Args:
        spark: Active SparkSession.
        min_rows: Minimum acceptable row count (env: ``QUALITY_ROW_COUNT_MIN``).

    Returns:
        True if the check passes.
    """


def check_null_pct(spark, max_null_pct: float = _NULL_PCT_MAX) -> bool:
    """Key columns (product_id, price_idr, shop_id, product_name) must have
    less than *max_null_pct* nulls.

    Args:
        spark: Active SparkSession.
        max_null_pct: Maximum acceptable null percentage per column
            (env: ``QUALITY_NULL_PCT_MAX``, default 5.0).

    Returns:
        True if all key columns pass.
    """


def check_price_positive(spark, min_price: int = _PRICE_MIN) -> bool:
    """All ``price_idr`` values must be >= *min_price*.

    Args:
        spark: Active SparkSession.
        min_price: Minimum acceptable price (env: ``QUALITY_PRICE_MIN``, default 1).

    Returns:
        True if no row has price below the threshold.
    """


def check_rejects_ratio(spark, max_reject_ratio: float = _REJECTS_RATIO_MAX) -> bool:
    """Rejects must be less than *max_reject_ratio* of total rows (anti silent failure).

    Args:
        spark: Active SparkSession.
        max_reject_ratio: Maximum acceptable reject proportion
            (env: ``QUALITY_REJECTS_RATIO_MAX``, default 0.10).

    Returns:
        True if the reject ratio is below the threshold.
    """


def check_freshness(spark, max_age_hours: float = _FRESHNESS_MAX_HOURS) -> bool:
    """Latest ``crawled_at`` must be within *max_age_hours* (data is not stale).

    Uses Python ``time.time()`` (UTC Unix epoch) to avoid Spark timezone confusion.

    Args:
        spark: Active SparkSession.
        max_age_hours: Maximum acceptable data age in hours
            (env: ``QUALITY_FRESHNESS_MAX_HOURS``, default 2.0).

    Returns:
        True if the newest row is within the freshness window.
    """
```

---

## 8. `source/library/tokopedia_api.py` — `search_products()` + `_throttle()` + `_build_metadata()`

```python
async def search_products(
    self,
    keyword: str,
    max_pages: int = 1,
    rows: int | None = None,
    page: int = 1,
    context_metadata: dict[str, Any] | None = None,
    **kwargs: Any,
) -> AsyncIterator[KafkaEvent]:
    """Paginate through product search results for *keyword*.

    Args:
        keyword: Search query string.
        max_pages: Maximum number of pages to fetch (1 → ~20 products).
        rows: Results per page (default from ``settings.crawler.default_rows``).
        page: Starting page number.
        context_metadata: Optional dict merged into each event's metadata
            (e.g. ``{"asset_category": "elektronik", "asset_id": "42"}``).
        **kwargs: Additional keyword arguments (``unique_id`` for visitor UUID).

    Yields:
        :class:`KafkaEvent` with a :class:`TokopediaProduct` payload and
        metadata containing ``keyword``, ``page``, plus any injected
        ``context_metadata``.

    Raises:
        ErrorRequestException: if the GraphQL response contains errors.
        RateLimitExceeded: if the gateway returns HTTP 429 (no retry).
    """


async def _throttle(self) -> None:
    """Enforce the configured requests-per-second rate limit with jitter.

    Jitter (±40%) prevents deterministic request patterns that anti-bot
    systems detect as bot signatures.
    """


@staticmethod
def _build_metadata(
    base: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge base + extra metadata. Single place for all API metadata injection.

    Args:
        base: Required metadata dict (e.g. ``{"keyword": "poco f8", "page": 1}``).
        extra: Optional additional metadata to merge (e.g. registry context).

    Returns:
        Merged metadata dict.
    """
```

---

## 9. `assets/repository.py` — All public functions

```python
def get_dsn() -> str:
    """Resolve DSN: pydantic ``ControlPlaneSettings`` > env override > default dev.

    Returns:
        Postgres connection string (``host=... port=... dbname=... user=... password=...``).
    """


def get_conn(dsn: str | None = None) -> Iterator[psycopg2.extensions.connection]:
    """Context manager for a Postgres connection with auto-commit/rollback.

    Args:
        dsn: Optional DSN override (uses module-level ``DSN`` if None).

    Yields:
        A psycopg2 connection. Commits on clean exit, rollbacks on exception.

    Raises:
        Propagates any exception after rollback.
    """


def get_due_assets(limit: int = 50, dsn: str | None = None) -> list[dict[str, Any]]:
    """Assets that are due for crawling now (the 'due' rule — PRD_50).

    Args:
        limit: Maximum number of assets to return.
        dsn: Optional DSN override.

    Returns:
        List of asset dicts, ordered by priority ASC then oldest crawl first.
    """


def mark_success(asset_id: int, dsn: str | None = None) -> None:
    """Record a successful crawl: set ``last_crawled_at = now()``,
    reset ``consecutive_failures = 0``, set ``last_status = 'success'``.

    Args:
        asset_id: The asset to update.
        dsn: Optional DSN override.
    """


def mark_failure(asset_id: int, status: str = "failed", dsn: str | None = None) -> bool:
    """Record a failed crawl: increment failure counter, check circuit breaker.

    Args:
        asset_id: The asset to update.
        status: ``"failed"`` or ``"blocked"``.
        dsn: Optional DSN override.

    Returns:
        True if the asset was just disabled by the circuit breaker
        (consecutive failures >= ``MAX_CONSECUTIVE_FAILURES``).

    Raises:
        ValueError: if *status* is not ``"failed"`` or ``"blocked"``.
    """


def reactivate(asset_id: int, dsn: str | None = None) -> None:
    """Reactivate an asset that was disabled by the circuit breaker.

    Resets ``consecutive_failures = 0`` and clears ``last_status``.

    Args:
        asset_id: The asset to reactivate.
        dsn: Optional DSN override.
    """
```

---

## 🧪 Verification

```bash
# Verify all docstrings render correctly
cd source && python -c "
import ast, sys
for mod in ['library.tokopedia_api', 'library.config', 'library.schemas']:
    m = __import__(mod, fromlist=['__all__'])
    for name, obj in vars(m).items():
        if callable(obj) and not name.startswith('_'):
            doc = getattr(obj, '__doc__', None)
            if doc:
                print(f'{mod}.{name}: DOCSTRING OK')
            else:
                print(f'{mod}.{name}: MISSING DOCSTRING')
"

# Lint should be clean
ruff check source/ pipeline/ assets/

# Tests should still pass
cd source && pytest tests/ -q
```
