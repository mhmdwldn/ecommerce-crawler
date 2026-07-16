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

# Core fields — required, reject the row if any is missing/null.
CORE_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("name", T.StringType()),
    T.StructField("url", T.StringType()),
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

# Optional fields — volatile, may change shape. Missing fields = null, not reject.
OPTIONAL_SCHEMA = T.StructType([
    T.StructField("rating", T.DoubleType()),
    T.StructField("category", T.StructType([
        T.StructField("id", T.IntegerType()),
        T.StructField("name", T.StringType()),
        T.StructField("breadcrumb", T.StringType()),
    ])),
    T.StructField("search_keyword", T.StringType()),
    T.StructField("asset_category", T.StringType()),
    T.StructField("asset_id", T.StringType()),
])

# Combined schema for from_json — PERMISSIVE mode: mismatched fields → null, not reject.
_PRODUCT_SCHEMA = T.StructType([
    *[f for f in CORE_SCHEMA.fields],
    *[f for f in OPTIONAL_SCHEMA.fields],
])


def add_category_columns(df: DataFrame) -> DataFrame:
    """Parse Tokopedia breadcrumb slug into a 3-level category dimension.

    Input requires these columns from bronze parsing:
      - ``category_breadcrumb``: e.g. "handphone-tablet/handphone/android-os"
      - ``asset_category``: e.g. "elektronik" (registry metadata)

    Transformations per level (max 3, extra levels silently dropped):
      1. Split breadcrumb by ``/`` → slugs
      2. Normalize slug → Title Case name (``initcap(regexp_replace(slug, "-", " "))``)
      3. Per-level md5 ID (stable, language-independent)
      4. Composite surrogate key: ``md5(l1_id|l2_id|l3_id|asset_category)``

    Returns the input DataFrame with added columns:
      cat_l1_name, cat_l2_name, cat_l3_name,
      l1_id, l2_id, l3_id, category_sk.
    Intermediate slug columns are dropped.
    """
    parts = F.split(F.col("category_breadcrumb"), "/")

    # Extract up to 3 level slugs
    df = (
        df
        .withColumn("_l1_slug", F.element_at(parts, 1))
        .withColumn("_l2_slug",
            F.when(F.size(parts) >= 2, F.element_at(parts, 2)).otherwise(F.lit("")))
        .withColumn("_l3_slug",
            F.when(F.size(parts) >= 3, F.element_at(parts, 3)).otherwise(F.lit("")))
    )

    # Normalize slug → name + md5 ID per level
    for i in range(1, 4):
        slug_col = f"_l{i}_slug"
        df = (
            df
            .withColumn(
                f"cat_l{i}_name",
                F.when(F.col(slug_col) != "",
                    F.initcap(F.regexp_replace(F.col(slug_col), "-", " "))
                ).otherwise(F.lit(""))
            )
            .withColumn(f"l{i}_id", F.md5(F.col(slug_col)))
        )

    # Composite surrogate key
    df = df.withColumn(
        "category_sk",
        F.md5(F.concat_ws("|",
            F.coalesce(F.col("l1_id"), F.lit("")),
            F.coalesce(F.col("l2_id"), F.lit("")),
            F.coalesce(F.col("l3_id"), F.lit("")),
            F.coalesce(F.col("asset_category"), F.lit("")),
        ))
    )

    return df.drop("_l1_slug", "_l2_slug", "_l3_slug")


def bronze_to_silver(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split bronze into (silver, rejects). A row is a reject when its JSON
    cannot be parsed or has no product id."""
    # PERMISSIVE mode: individual field mismatches → null in that field, not entire-row reject.
    parsed = df.withColumn(
        "doc",
        F.from_json("value_json", _PRODUCT_SCHEMA, options={"mode": "PERMISSIVE"}),
    )

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
            # --- core product fields ---
            F.col("doc.id").alias("product_id"),
            F.col("doc.name").alias("product_name"),
            F.col("doc.url").alias("product_url"),
            F.col("doc.rating").alias("rating"),
            F.col("doc.price.number").alias("price_idr"),
            F.col("doc.price.discountPercentage").alias("discount_pct"),
            # --- shop ---
            F.col("doc.shop.id").alias("shop_id"),
            F.col("doc.shop.name").alias("shop_name"),
            F.col("doc.shop.city").alias("shop_city"),
            F.col("doc.shop.tier").alias("shop_tier"),
            F.col("kafka_timestamp").alias("crawled_at"),
            # --- registry metadata ---
            F.coalesce(F.col("doc.search_keyword"), F.lit("")).alias("search_keyword"),
            F.coalesce(F.col("doc.asset_category"), F.lit("")).alias("asset_category"),
            F.coalesce(F.col("doc.asset_id"), F.lit("")).alias("asset_id"),
            # --- Tokopedia category ---
            F.col("doc.category.id").cast(T.IntegerType()).alias("tokopedia_category_id"),
            F.col("doc.category.name").alias("tokopedia_category_name"),
            F.coalesce(F.col("doc.category.breadcrumb"), F.lit("")).alias("category_breadcrumb"),
        )
    )
    silver = add_category_columns(silver)
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
    bronze = spark.read.format("delta").load(BRONZE_PATH)

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

    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        new_silver.write.format("delta").save(SILVER_PATH)
    else:
        silver_table = DeltaTable.forPath(spark, SILVER_PATH)
        silver_table.alias("target").merge(
            new_silver.alias("source"),
            "target.product_id = source.product_id AND target.crawled_at = source.crawled_at"
        ).whenNotMatchedInsertAll().execute()

    # Append rejects — mergeSchema handles column additions from code changes
    new_rejects.write.format("delta").mode("append") \
        .option("mergeSchema", "true").save(REJECTS_PATH)

    total = spark.read.format("delta").load(SILVER_PATH).count()
    rejects_total = spark.read.format("delta").load(REJECTS_PATH).count()
    print(f"silver incremental: {new_count} new bronze rows -> {total} silver rows, "
          f"rejects: {rejects_total}")


if __name__ == "__main__":
    inc = "--incremental" in sys.argv or "-i" in sys.argv
    main(incremental=inc)
