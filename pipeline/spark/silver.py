"""Silver layer — parse bronze JSON into a typed, deduplicated product table.

ponytail: full rebuild of silver from all of bronze on every run;
switch to incremental MERGE when bronze grows past what a laptop chews.
"""

import os

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://lakehouse/bronze/products")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://lakehouse/silver/products")
REJECTS_PATH = os.getenv("REJECTS_PATH", "s3a://lakehouse/silver/products_rejects")

PRODUCT_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("name", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("rating", T.DoubleType()),
    T.StructField("price", T.StructType([
        T.StructField("number", T.LongType()),
        T.StructField("discountPercentage", T.IntegerType()),
    ])),
    T.StructField("shop", T.StructType([
        T.StructField("id", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("city", T.StringType()),
        T.StructField("tier", T.IntegerType()),
    ])),
])


def bronze_to_silver(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split bronze into (silver, rejects). A row is a reject when its JSON
    cannot be parsed or has no product id."""
    parsed = df.withColumn("doc", F.from_json("value_json", PRODUCT_SCHEMA))

    rejects = (
        parsed.filter(F.col("doc").isNull() | F.col("doc.id").isNull())
        .select("value_json", "kafka_offset", "kafka_timestamp",
                F.current_timestamp().alias("rejected_at"))
    )

    dedup = Window.partitionBy("doc.id", "kafka_timestamp").orderBy(F.col("kafka_offset").desc())
    silver = (
        parsed.filter(F.col("doc.id").isNotNull())
        .withColumn("rn", F.row_number().over(dedup))
        .filter(F.col("rn") == 1)
        .select(
            F.col("doc.id").alias("product_id"),
            F.col("doc.name").alias("product_name"),
            F.col("doc.url").alias("product_url"),
            F.col("doc.rating").alias("rating"),
            F.col("doc.price.number").alias("price_idr"),
            F.col("doc.price.discountPercentage").alias("discount_pct"),
            F.col("doc.shop.id").alias("shop_id"),
            F.col("doc.shop.name").alias("shop_name"),
            F.col("doc.shop.city").alias("shop_city"),
            F.col("doc.shop.tier").alias("shop_tier"),
            F.col("kafka_timestamp").alias("crawled_at"),
        )
    )
    return silver, rejects


def main() -> None:
    from pipeline.spark.session import build_session

    spark = build_session("silver")
    bronze = spark.read.format("delta").load(BRONZE_PATH)
    silver, rejects = bronze_to_silver(bronze)
    silver.write.format("delta").mode("overwrite").save(SILVER_PATH)
    rejects.write.format("delta").mode("overwrite").save(REJECTS_PATH)
    print(f"silver rows: {spark.read.format('delta').load(SILVER_PATH).count()}, "
          f"rejects: {spark.read.format('delta').load(REJECTS_PATH).count()}")


if __name__ == "__main__":
    main()
