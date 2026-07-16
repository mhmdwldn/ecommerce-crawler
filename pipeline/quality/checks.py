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


def _silver_path():
    return os.getenv("SILVER_PATH", "s3a://lakehouse/silver/products")


def _rejects_path():
    return os.getenv("REJECTS_PATH", "s3a://lakehouse/silver/products_rejects")


def check_row_count(spark, min_rows=_ROW_COUNT_MIN):
    """Silver must have at least min_rows rows."""
    path = _silver_path()
    count = spark.read.format("delta").load(path).count()
    if count < min_rows:
        print(f"FAIL row_count: {count} rows < {min_rows} ({path})")
        return False
    print(f"PASS row_count: {count} rows")
    return True


def check_null_pct(spark, max_null_pct=_NULL_PCT_MAX):
    """Key columns must have < max_null_pct nulls."""
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


def check_price_positive(spark, min_price=_PRICE_MIN):
    """All price_idr values must be >= min_price (default 1)."""
    path = _silver_path()
    df = spark.read.format("delta").load(path)
    bad = df.filter(df.price_idr < min_price).count()
    if bad > 0:
        print(f"FAIL price_positive: {bad} rows with price_idr < {min_price}")
        return False
    print(f"PASS price_positive: all prices >= {min_price}")
    return True


def check_rejects_ratio(spark, max_reject_ratio=_REJECTS_RATIO_MAX):
    """Rejects must be < max_reject_ratio of total rows (anti silent failure)."""
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


def check_freshness(spark, max_age_hours=_FRESHNESS_MAX_HOURS):
    """Latest crawled_at must be within max_age_hours (data is not stale)."""
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
