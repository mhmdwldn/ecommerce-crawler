"""Data retention enforcement — prunes Delta tables beyond retention windows.

ponytail: VACUUM with explicit RETAIN HOURS. Bronze=90 days, Silver=180 days.
Run monthly via lakehouse_maintenance DAG or standalone.
"""

import argparse
import os

from pipeline.spark.session import build_session

BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://lakehouse/bronze/products")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://lakehouse/silver/products")
REJECTS_PATH = os.getenv("REJECTS_PATH", "s3a://lakehouse/silver/products_rejects")

# Default: 90 days bronze, 180 days silver, 180 days rejects
DEFAULTS = {"bronze": 2160, "silver": 4320, "rejects": 4320}


def vacuum_table(path: str, retain_hours: int, label: str) -> None:
    spark = build_session(f"retention_{label}")
    try:
        spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")
        print(f"  {label}: VACUUM done (retain {retain_hours}h = {retain_hours//24}d)")
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Data retention enforcement")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be pruned")
    parser.add_argument("--bronze-hours", type=int, default=DEFAULTS["bronze"])
    parser.add_argument("--silver-hours", type=int, default=DEFAULTS["silver"])
    args = parser.parse_args()

    if args.dry_run:
        print("DRY RUN — would prune:")
        print(f"  Bronze: data older than {args.bronze_hours}h ({args.bronze_hours//24}d)")
        print(f"  Silver: data older than {args.silver_hours}h ({args.silver_hours//24}d)")
        return

    print("Data Retention — pruning old files...")
    vacuum_table(BRONZE_PATH, args.bronze_hours, "bronze")
    vacuum_table(SILVER_PATH, args.silver_hours, "silver")
    vacuum_table(REJECTS_PATH, DEFAULTS["rejects"], "rejects")
    print("Retention enforcement complete.")


if __name__ == "__main__":
    main()
