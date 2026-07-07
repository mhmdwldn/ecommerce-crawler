from datetime import datetime

from pyspark.sql import types as T

from pipeline.spark.stream_bronze import kafka_to_bronze

KAFKA_SCHEMA = T.StructType([
    T.StructField("value", T.BinaryType()),
    T.StructField("topic", T.StringType()),
    T.StructField("partition", T.IntegerType()),
    T.StructField("offset", T.LongType()),
    T.StructField("timestamp", T.TimestampType()),
])


def test_kafka_to_bronze_maps_columns(spark):
    row = (b'{"id": "123"}', "tokopedia.products.raw", 0, 42, datetime(2026, 7, 7, 10, 0, 0))
    df = spark.createDataFrame([row], schema=KAFKA_SCHEMA)

    out = kafka_to_bronze(df).collect()[0]

    assert out.value_json == '{"id": "123"}'
    assert out.kafka_topic == "tokopedia.products.raw"
    assert out.kafka_offset == 42
    assert out.kafka_timestamp == datetime(2026, 7, 7, 10, 0, 0)
    assert out.ingested_at is not None
