"""Data retention enforcement — prunes Delta tables beyond retention windows.

ponytail: VACUUM with explicit RETAIN HOURS. Bronze=90 days, Silver=180 days.
Cold storage: export data older than retention to Parquet before VACUUM.
Run monthly via data_retention DAG or standalone.
"""

import argparse
import os

from pyspark.sql import functions as F

from pipeline.spark.session import build_session

BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://lakehouse/bronze/products")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://lakehouse/silver/products")
REJECTS_PATH = os.getenv("REJECTS_PATH", "s3a://lakehouse/silver/products_rejects")
COLD_PATH = os.getenv("COLD_PATH", "s3a://lakehouse/cold")

# Default: 90 days bronze, 180 days silver, 180 days rejects
DEFAULTS = {"bronze": 2160, "silver": 4320, "rejects": 4320}


def vacuum_table(path: str, retain_hours: int, label: str) -> None:
    spark = build_session(f"retention_{label}")
    try:
        spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")
        print(f"  {label}: VACUUM done (retain {retain_hours}h = {retain_hours//24}d)")
    finally:
        spark.stop()


def export_to_cold(path: str, retain_hours: int, label: str) -> None:
    """Export data older than retain_hours to cold storage (Parquet) before VACUUM."""
    spark = build_session(f"cold_{label}")
    try:
        df = spark.read.format("delta").load(path)
        cutoff = F.current_timestamp() - F.expr(f"INTERVAL {retain_hours} HOURS")

        # Determine timestamp column
        ts_col = "kafka_timestamp" if "bronze" in label else "crawled_at"
        try:
            old_data = df.filter(F.col(ts_col) < cutoff)
            count = old_data.count()
            if count > 0:
                export_path = f"{COLD_PATH}/{label}/{F.current_timestamp().cast('string').substr(1, 10)}"
                old_data.write.mode("overwrite").parquet(export_path)
                print(f"  {label}: exported {count} rows to {export_path}")
            else:
                print(f"  {label}: no data to export (all within retention)")
        except Exception as e:
            print(f"  {label}: export skipped ({e})")
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Data retention enforcement")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be pruned")
    parser.add_argument("--cold-storage", action="store_true", help="Export old data to Parquet before VACUUM")
    parser.add_argument("--bronze-hours", type=int, default=DEFAULTS["bronze"])
    parser.add_argument("--silver-hours", type=int, default=DEFAULTS["silver"])
    args = parser.parse_args()

    if args.dry_run:
        print("DRY RUN — would prune:")
        print(f"  Bronze: data older than {args.bronze_hours}h ({args.bronze_hours//24}d)")
        print(f"  Silver: data older than {args.silver_hours}h ({args.silver_hours//24}d)")
        if args.cold_storage:
            print("  Cold storage: export to Parquet before VACUUM")
        return

    print("Data Retention — pruning old files...")

    if args.cold_storage:
        print("Step 1/2: Exporting cold data to Parquet...")
        export_to_cold(BRONZE_PATH, args.bronze_hours, "bronze")
        export_to_cold(SILVER_PATH, args.silver_hours, "silver")

    print("Step 2/2: VACUUM...")
    vacuum_table(BRONZE_PATH, args.bronze_hours, "bronze")
    vacuum_table(SILVER_PATH, args.silver_hours, "silver")
    vacuum_table(REJECTS_PATH, DEFAULTS["rejects"], "rejects")
    print("Retention enforcement complete.")


if __name__ == "__main__":
    main()
