"""Quality checks on the silver layer — run after silver, before dbt.

Each check prints PASS/FAIL. Exit code is non-zero if ANY check fails,
so Airflow marks the task as failed.

ponytail: one check per function, each reads the silver Delta table.
Add checks here as new failure modes are discovered.

Thresholds are configurable via env vars (with defaults):
  QUALITY_NULL_PCT_MAX, QUALITY_REJECTS_RATIO_MAX, QUALITY_FRESHNESS_MAX_HOURS,
  QUALITY_ROW_COUNT_MIN, QUALITY_PRICE_MIN
"""

import os
import sys

# Configurable thresholds — tune without code deploy
_NULL_PCT_MAX = float(os.getenv("QUALITY_NULL_PCT_MAX", "5.0"))
_REJECTS_RATIO_MAX = float(os.getenv("QUALITY_REJECTS_RATIO_MAX", "0.10"))
_FRESHNESS_MAX_HOURS = float(os.getenv("QUALITY_FRESHNESS_MAX_HOURS", "2.0"))
_ROW_COUNT_MIN = int(os.getenv("QUALITY_ROW_COUNT_MIN", "1"))
_PRICE_MIN = int(os.getenv("QUALITY_PRICE_MIN", "1"))


def _silver_path() -> str:
    """Return the Delta Lake path for the silver table (env-configurable)."""
    return os.getenv("SILVER_PATH", "s3a://lakehouse/silver/products")


def _rejects_path() -> str:
    """Return the Delta Lake path for the rejects quarantine table."""
    return os.getenv("REJECTS_PATH", "s3a://lakehouse/silver/products_rejects")


def check_row_count(spark, min_rows: int = _ROW_COUNT_MIN) -> bool:
    """Silver must have at least ``min_rows`` rows.

    Args:
        spark: Active ``SparkSession``.
        min_rows: Minimum expected row count (env: ``QUALITY_ROW_COUNT_MIN``).

    Returns:
        ``True`` if the check passes.
    """
    path = _silver_path()
    count = spark.read.format("delta").load(path).count()
    if count < min_rows:
        print(f"FAIL row_count: {count} rows < {min_rows} ({path})")
        return False
    print(f"PASS row_count: {count} rows")
    return True


def check_null_pct(spark, max_null_pct: float = _NULL_PCT_MAX) -> bool:
    """Key columns must have < ``max_null_pct`` percent nulls.

    Checks ``product_id``, ``price_idr``, ``shop_id``, ``product_name``.
    Skipped if silver has 0 rows (the ``row_count`` check will catch that).

    Args:
        spark: Active ``SparkSession``.
        max_null_pct: Maximum allowed null percentage (env: ``QUALITY_NULL_PCT_MAX``).

    Returns:
        ``True`` if all key columns pass.
    """
    path = _silver_path()
    df = spark.read.format("delta").load(path)
    total = df.count()
    if total == 0:
        print("PASS null_pct: skipped (0 rows — row_count check will fail)")
        return True

    failed = False
    for col in ["product_id", "price_idr", "shop_id", "product_name"]:
        nulls = df.filter(df[col].isNull()).count()
        pct = (nulls / total) * 100
        if pct > max_null_pct:
            print(f"FAIL null_pct: {col} = {pct:.1f}% null (max {max_null_pct}%)")
            failed = True
        else:
            print(f"PASS null_pct: {col} = {pct:.1f}% null")
    return not failed


def check_price_positive(spark, min_price: int = _PRICE_MIN) -> bool:
    """All ``price_idr`` values must be >= ``min_price``.

    Args:
        spark: Active ``SparkSession``.
        min_price: Minimum valid price in IDR (env: ``QUALITY_PRICE_MIN``).

    Returns:
        ``True`` if no row has a price below the threshold.
    """
    path = _silver_path()
    df = spark.read.format("delta").load(path)
    bad = df.filter(df.price_idr < min_price).count()
    if bad > 0:
        print(f"FAIL price_positive: {bad} rows with price_idr < {min_price}")
        return False
    print(f"PASS price_positive: all prices >= {min_price}")
    return True


def check_rejects_ratio(spark, max_reject_ratio: float = _REJECTS_RATIO_MAX) -> bool:
    """Rejects must be < ``max_reject_ratio`` of total rows (anti-silent-failure).

    Calculates ``rejects / (silver + rejects)``. If no rejects table exists yet,
    treats reject count as 0. Skipped if both tables are empty.

    Args:
        spark: Active ``SparkSession``.
        max_reject_ratio: Maximum allowed reject fraction (env: ``QUALITY_REJECTS_RATIO_MAX``).

    Returns:
        ``True`` if the reject ratio is below the threshold.
    """
    silver = spark.read.format("delta").load(_silver_path())
    silver_count = silver.count()

    rejects_path = _rejects_path()
    try:
        rejects = spark.read.format("delta").load(rejects_path)
        rejects_count = rejects.count()
    except Exception:
        rejects_count = 0  # no rejects table yet

    total = silver_count + rejects_count
    if total == 0:
        print("PASS rejects_ratio: no data (skipped)")
        return True

    ratio = rejects_count / total
    if ratio > max_reject_ratio:
        print(
            f"FAIL rejects_ratio: {rejects_count}/{total} = {ratio:.1%} "
            f"(max {max_reject_ratio:.0%})"
        )
        return False
    print(f"PASS rejects_ratio: {rejects_count}/{total} = {ratio:.1%}")
    return True


def check_freshness(spark, max_age_hours: float = _FRESHNESS_MAX_HOURS) -> bool:
    """Latest ``crawled_at`` must be within ``max_age_hours`` (data is not stale).

    Uses ``time.time()`` Unix epoch for age calculation to avoid timezone
    confusion across Docker hosts. Skipped if silver has no rows.

    Args:
        spark: Active ``SparkSession``.
        max_age_hours: Maximum allowed data age in hours
            (env: ``QUALITY_FRESHNESS_MAX_HOURS``).

    Returns:
        ``True`` if the most recent crawl is fresh enough.
    """
    from pyspark.sql import functions as F

    path = _silver_path()
    df = spark.read.format("delta").load(path)
    max_ts = df.agg(F.max("crawled_at")).collect()[0][0]
    if max_ts is None:
        print("PASS freshness: no data (skipped)")
        return True

    # Compute age in hours using Unix timestamps (avoids timezone confusion)
    import time
    now_ts = time.time()
    age_hours = (now_ts - max_ts.timestamp()) / 3600.0

    if age_hours > max_age_hours:
        print(
            f"FAIL freshness: last crawled_at = {max_ts}, "
            f"age = {age_hours:.1f}h (max {max_age_hours}h)"
        )
        return False
    print(f"PASS freshness: last crawled_at = {max_ts} ({age_hours:.1f}h ago)")
    return True


def main() -> int:
    """Run all five quality checks against the silver layer.

    Builds a Spark session, runs each check sequentially, and prints a
    PASS/FAIL summary. Any single failure returns exit code 1 so Airflow
    marks the task as ``FAILED`` and stops the pipeline before dbt.

    Returns:
        0 if all checks pass, 1 if any check fails.
    """
    from pipeline.spark.session import build_session

    spark = build_session("quality_check")
    try:
        checks = [
            ("row_count", check_row_count),
            ("null_pct", check_null_pct),
            ("price_positive", check_price_positive),
            ("rejects_ratio", check_rejects_ratio),
            ("freshness", check_freshness),
        ]

        results = {}
        for name, fn in checks:
            try:
                results[name] = fn(spark)
            except Exception as e:
                print(f"FAIL {name}: {e}")
                results[name] = False

        failed = [k for k, v in results.items() if not v]
        print(f"\n--- quality_check: {len(results) - len(failed)}/{len(results)} passed ---")
        if failed:
            print(f"FAILED: {', '.join(failed)}")
            return 1
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
