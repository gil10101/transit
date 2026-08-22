"""Bronze: Kafka transit.* -> lake.bronze.envelopes (Iceberg, append, checkpointed).

Decoded envelopes with the payload kept as JSON text; no dedup beyond Kafka
exactly-once-per-offset semantics. Partitioned by feed / city / service_date,
where service_date is derived from fetched_at via the noon rule in city-local time.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.session import checkpoint_root, city_timezones, ensure_table, kafka_bootstrap

TABLE = "lake.bronze.envelopes"

DDL = """
    topic string, kafka_partition int, kafka_offset bigint, kafka_ts timestamp,
    city string, agency string, feed string, endpoint string,
    source_format string, schema_version int, feed_ts bigint,
    fetched_at timestamp, envelope_json string, service_date date
"""


def tz_map_expr():
    pairs = []
    for city, tz in city_timezones().items():
        pairs.extend([F.lit(city), F.lit(tz)])
    return F.create_map(*pairs)


def build_stream(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap())
        .option("subscribePattern", "transit\\..*")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "5000")
        .load()
    )
    value = F.col("value").cast("string")
    fetched_at = F.to_timestamp(F.get_json_object(value, "$.fetched_at"))
    city = F.col("key").cast("string")
    tz = F.coalesce(tz_map_expr()[city], F.lit("UTC"))
    return raw.select(
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        city.alias("city"),
        F.get_json_object(value, "$.agency").alias("agency"),
        F.get_json_object(value, "$.feed").alias("feed"),
        F.get_json_object(value, "$.endpoint").alias("endpoint"),
        F.get_json_object(value, "$.source_format").alias("source_format"),
        F.get_json_object(value, "$.schema_version").cast("int").alias("schema_version"),
        F.get_json_object(value, "$.feed_ts").cast("bigint").alias("feed_ts"),
        fetched_at.alias("fetched_at"),
        value.alias("envelope_json"),
        F.to_date(F.from_utc_timestamp(fetched_at, tz) - F.expr("INTERVAL 12 HOURS")).alias(
            "service_date"
        ),
    )


def start(spark: SparkSession):
    ensure_table(spark, TABLE, DDL, "feed, city, service_date")
    return (
        build_stream(spark)
        .writeStream.queryName("bronze_envelopes")
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", f"{checkpoint_root()}/bronze_envelopes")
        .toTable(TABLE)
    )
