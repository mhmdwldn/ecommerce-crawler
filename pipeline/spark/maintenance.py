"""Lakehouse maintenance — OPTIMIZE + VACUUM bronze & silver Delta tables."""

import argparse

from pipeline.spark.session import build_session


def optimize_table(path: str, label: str) -> None:
    """Compact small Parquet files in a Delta table to improve read performance.

    Args:
        path: Delta table path (e.g. ``s3a://lakehouse/bronze/products``).
        label: Human-readable label for logging and Spark app name.
    """
    spark = build_session(f"maintenance_opt_{label}")
    try:
        spark.sql(f"OPTIMIZE delta.`{path}`")
        print(f"{label} OPTIMIZE done")
    finally:
        spark.stop()


def vacuum_table(path: str, retain_hours: int = 168, label: str = "") -> None:
    """Remove stale Parquet files from a Delta table older than the retention period.

    Args:
        path: Delta table path.
        retain_hours: Files needed for time-travel up to this many hours are kept.
            Default 168 (7 days).
        label: Human-readable label for logging and Spark app name.
    """
    spark = build_session(f"maintenance_vac_{label}")
    try:
        spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")
        print(f"{label} VACUUM done")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lakehouse maintenance")
    parser.add_argument(
        "action", nargs="?", default="all",
        choices=["all", "bronze", "silver"],
        help="What to maintain (default: all)",
    )
    args = parser.parse_args()
    bronze = "s3a://lakehouse/bronze/products"
    silver = "s3a://lakehouse/silver/products"

    if args.action in ("all", "bronze"):
        optimize_table(bronze, "bronze")
        vacuum_table(bronze, label="bronze")
    if args.action in ("all", "silver"):
        optimize_table(silver, "silver")
        vacuum_table(silver, label="silver")
