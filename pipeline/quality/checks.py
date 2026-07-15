"""Quality checks on the silver layer — run after silver, before dbt.

Each check prints PASS/FAIL. Exit code is non-zero if ANY check fails,
so Airflow marks the task as failed.

ponytail: one check per function, each reads the silver Delta table.
Add checks here as new failure modes are discovered.
"""

import os
import sys


def _silver_path():
    return os.getenv("SILVER_PATH", "s3a://lakehouse/silver/products")


def _rejects_path():
    return os.getenv("REJECTS_PATH", "s3a://lakehouse/silver/products_rejects")


def check_row_count(spark):
    """Silver must have at least 1 row."""
    path = _silver_path()
    count = spark.read.format("delta").load(path).count()
    if count == 0:
        print(f"FAIL row_count: silver is empty ({path})")
        return False
    print(f"PASS row_count: {count} rows")
    return True


def check_null_pct(spark, max_null_pct=5.0):
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


def check_price_positive(spark):
    """All price_idr values must be > 0."""
    path = _silver_path()
    df = spark.read.format("delta").load(path)
    bad = df.filter(df.price_idr <= 0).count()
    if bad > 0:
        print(f"FAIL price_positive: {bad} rows with price_idr <= 0")
        return False
    print("PASS price_positive: all prices > 0")
    return True


def check_rejects_ratio(spark, max_reject_ratio=0.10):
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


def check_freshness(spark, max_age_hours=2):
    """Latest crawled_at must be within max_age_hours (data is not stale)."""
    from pyspark.sql import functions as F

    path = _silver_path()
    df = spark.read.format("delta").load(path)
    max_ts = df.agg(F.max("crawled_at")).collect()[0][0]
    if max_ts is None:
        print("PASS freshness: no data (skipped)")
        return True

    now = F.current_timestamp()
    age_seconds = df.select(
        (now.cast("long") - F.col("crawled_at").cast("long")).alias("age")
    ).agg(F.min("age")).collect()[0][0]

    age_hours = age_seconds / 3600.0 if age_seconds is not None else 0
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
