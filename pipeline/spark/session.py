"""Shared SparkSession builder — Delta + S3A (MinIO) wiring."""

import os

from pyspark.sql import SparkSession

# Compatibility set for pyspark 3.5.4 — bump together or not at all.
PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4",
    "io.delta:delta-spark_2.12:3.3.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
])


def build_session(app_name: str) -> SparkSession:
    """Build a local PySpark session wired to MinIO (S3A), Delta Lake, and Kafka.

    Args:
        app_name: Spark application name (e.g. ``"silver"``, ``"stream_bronze"``).

    Returns:
        A configured ``SparkSession`` with Delta Lake extension, S3A filesystem
        pointed at MinIO (via ``MINIO_ENDPOINT`` / ``MINIO_ACCESS_KEY`` /
        ``MINIO_SECRET_KEY`` env vars), and 4 shuffle partitions.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint",
                os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
