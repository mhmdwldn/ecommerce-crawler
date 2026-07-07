import json
from datetime import datetime

from pyspark.sql import types as T

from pipeline.spark.silver import bronze_to_silver

BRONZE_SCHEMA = T.StructType([
    T.StructField("value_json", T.StringType()),
    T.StructField("kafka_offset", T.LongType()),
    T.StructField("kafka_timestamp", T.TimestampType()),
])

TS = datetime(2026, 7, 7, 10, 0, 0)


def product_json(pid="123", name="Poco F8", price=4_999_000, shop_id="77"):
    return json.dumps({
        "id": pid,
        "name": name,
        "url": f"https://www.tokopedia.com/shop/{pid}",
        "rating": 4.9,
        "price": {"text": "Rp4.999.000", "number": price, "discountPercentage": 10},
        "shop": {"id": shop_id, "name": "Xiaomi Official", "city": "Jakarta", "tier": 2},
    })


def bronze_df(spark, rows):
    return spark.createDataFrame(rows, schema=BRONZE_SCHEMA)


def test_happy_path_flattens_fields(spark):
    df = bronze_df(spark, [(product_json(), 1, TS)])

    silver, rejects = bronze_to_silver(df)
    row = silver.collect()[0]

    assert row.product_id == "123"
    assert row.product_name == "Poco F8"
    assert row.price_idr == 4_999_000
    assert row.discount_pct == 10
    assert row.shop_id == "77"
    assert row.shop_city == "Jakarta"
    assert row.crawled_at == TS
    assert rejects.count() == 0


def test_exact_duplicates_are_deduped(spark):
    df = bronze_df(spark, [
        (product_json(pid="123"), 1, TS),
        (product_json(pid="123"), 2, TS),   # same product, same crawl ts (reprocess)
    ])

    silver, _ = bronze_to_silver(df)

    assert silver.count() == 1


def test_unparseable_rows_go_to_rejects(spark):
    df = bronze_df(spark, [
        (product_json(), 1, TS),
        ("not json at all {{{", 2, TS),
        (json.dumps({"name": "no id field"}), 3, TS),
    ])

    silver, rejects = bronze_to_silver(df)

    assert silver.count() == 1
    assert rejects.count() == 2
    assert set(rejects.columns) == {"value_json", "kafka_offset", "kafka_timestamp", "rejected_at"}
