"""Bronze ingestion — Spark Structured Streaming from Kafka into Delta on MinIO.

Runs with trigger(availableNow=True): drains everything new on the topic,
then exits — so Airflow can run it as a normal task. Checkpoint on MinIO
guarantees no message is read twice across runs.
"""

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://lakehouse/bronze/products")
CHECKPOINT_PATH = os.getenv("BRONZE_CHECKPOINT", "s3a://lakehouse/_checkpoints/bronze_products")


def kafka_to_bronze(df: DataFrame) -> DataFrame:
    """Map raw Kafka columns to the bronze schema (works on batch or stream)."""
    return df.select(
        F.col("value").cast("string").alias("value_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
    )


def main() -> None:
    """Stream Kafka topic → Delta Lake bronze on MinIO (TriggerAvailableNow).

    Reads ``KAFKA_BOOTSTRAP``, ``KAFKA_TOPIC`` from env vars (defaults:
    ``localhost:9092`` / ``tokopedia.products.raw``). Uses ``failOnDataLoss=false``
    so the query does not crash when the topic is recreated with fewer partitions.
    Checkpoint at ``BRONZE_CHECKPOINT`` ensures exactly-once across runs.

    This is designed for Airflow: each DAG run drains whatever is new on the
    topic, then exits — no long-running streaming daemon needed.
    """
    from pipeline.spark.session import build_session

    spark = build_session("stream_bronze")
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
        .option("subscribe", os.getenv("KAFKA_TOPIC", "tokopedia.products.raw"))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    query = (
        kafka_to_bronze(stream)
        .writeStream.format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start(BRONZE_PATH)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
