"""Silver layer — parse bronze JSON into a typed, deduplicated product table.

Modes:
  python -m pipeline.spark.silver                # full rebuild (default)
  python -m pipeline.spark.silver --incremental   # MERGE new rows only
  python -m pipeline.spark.silver --full-refresh  # explicit full rebuild (same as default)
"""

import os
import sys

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
    T.StructField("category", T.StructType([
        T.StructField("id", T.IntegerType()),
        T.StructField("name", T.StringType()),
        T.StructField("breadcrumb", T.StringType()),
    ])),
    # Registry metadata (injected by crawl_assets.py via CLI -> Kafka -> bronze)
    T.StructField("search_keyword", T.StringType()),
    T.StructField("asset_category", T.StringType()),
    T.StructField("asset_id", T.StringType()),
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
            # --- new: registry metadata ---
            F.coalesce(F.col("doc.search_keyword"), F.lit("")).alias("search_keyword"),
            F.coalesce(F.col("doc.asset_category"), F.lit("")).alias("asset_category"),
            F.coalesce(F.col("doc.asset_id"), F.lit("")).alias("asset_id"),
            # --- new: Tokopedia category ---
            F.col("doc.category.id").cast(T.IntegerType()).alias("tokopedia_category_id"),
            F.col("doc.category.name").alias("tokopedia_category_name"),
            F.coalesce(F.col("doc.category.breadcrumb"), F.lit("")).alias("category_breadcrumb"),
        )
        # Parse breadcrumb slug into 3 normalized levels
        .withColumn("_parts", F.split(F.col("category_breadcrumb"), "/"))
        .withColumn("l1_slug", F.element_at(F.col("_parts"), 1))
        .withColumn("l2_slug",
            F.when(F.size(F.col("_parts")) >= 2, F.element_at(F.col("_parts"), 2)).otherwise(F.lit("")))
        .withColumn("l3_slug",
            F.when(F.size(F.col("_parts")) >= 3, F.element_at(F.col("_parts"), 3)).otherwise(F.lit("")))
        # Normalize slug -> Title Case: replace "-" with " ", then initcap
        .withColumn("cat_l1_name",
            F.initcap(F.regexp_replace(F.col("l1_slug"), "-", " ")))
        .withColumn("cat_l2_name",
            F.when(F.col("l2_slug") != "",
                F.initcap(F.regexp_replace(F.col("l2_slug"), "-", " "))).otherwise(F.lit("")))
        .withColumn("cat_l3_name",
            F.when(F.col("l3_slug") != "",
                F.initcap(F.regexp_replace(F.col("l3_slug"), "-", " "))).otherwise(F.lit("")))
        # Per-level md5 IDs (stable, language-independent)
        .withColumn("l1_id", F.md5(F.col("l1_slug")))
        .withColumn("l2_id", F.md5(F.col("l2_slug")))
        .withColumn("l3_id", F.md5(F.col("l3_slug")))
        # Composite category_sk = md5(l1_id|l2_id|l3_id|asset_category)
        .withColumn("category_sk",
            F.md5(F.concat_ws("|",
                F.coalesce(F.col("l1_id"), F.lit("")),
                F.coalesce(F.col("l2_id"), F.lit("")),
                F.coalesce(F.col("l3_id"), F.lit("")),
                F.coalesce(F.col("asset_category"), F.lit("")))))
        # Drop intermediate columns
        .drop("_parts", "l1_slug", "l2_slug", "l3_slug")
    )
    return silver, rejects


def main(incremental: bool = False) -> None:
    from pipeline.spark.session import build_session

    spark = build_session("silver")

    if incremental:
        _main_incremental(spark)
    else:
        _main_full(spark)


def _main_full(spark) -> None:
    bronze = spark.read.format("delta").load(BRONZE_PATH)
    silver, rejects = bronze_to_silver(bronze)
    silver.write.format("delta").mode("overwrite").save(SILVER_PATH)
    rejects.write.format("delta").mode("overwrite").save(REJECTS_PATH)
    print(f"silver rows: {spark.read.format('delta').load(SILVER_PATH).count()}, "
          f"rejects: {spark.read.format('delta').load(REJECTS_PATH).count()}")


def _main_incremental(spark) -> None:
    # Read bronze since last silver run
    bronze = spark.read.format("delta").load(BRONZE_PATH)

    # Find watermark: max kafka_timestamp already in silver
    try:
        existing_silver = spark.read.format("delta").load(SILVER_PATH)
        last_crawled = existing_silver.agg(F.max("crawled_at")).collect()[0][0]
    except Exception:
        last_crawled = None

    if last_crawled is not None:
        new_bronze = bronze.filter(F.col("kafka_timestamp") > last_crawled)
    else:
        new_bronze = bronze

    new_count = new_bronze.count()
    if new_count == 0:
        print("silver incremental: no new rows (up to date)")
        spark.stop()
        return

    new_silver, new_rejects = bronze_to_silver(new_bronze)

    # MERGE into silver: update existing product_ids, insert new ones
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        new_silver.write.format("delta").save(SILVER_PATH)
    else:
        silver_table = DeltaTable.forPath(spark, SILVER_PATH)
        silver_table.alias("target").merge(
            new_silver.alias("source"),
            "target.product_id = source.product_id AND target.crawled_at = source.crawled_at"
        ).whenNotMatchedInsertAll().execute()

    # Append rejects
    new_rejects.write.format("delta").mode("append").save(REJECTS_PATH)

    total = spark.read.format("delta").load(SILVER_PATH).count()
    rejects_total = spark.read.format("delta").load(REJECTS_PATH).count()
    print(f"silver incremental: {new_count} new bronze rows -> {total} silver rows, "
          f"rejects: {rejects_total}")


if __name__ == "__main__":
    inc = "--incremental" in sys.argv or "-i" in sys.argv
    main(incremental=inc)
