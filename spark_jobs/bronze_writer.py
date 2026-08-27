"""Bronze: Kafka transit.* -> lake.bronze.envelopes (Iceberg, append, checkpointed).

Decoded envelopes with the payload kept as JSON text; no dedup beyond Kafka
exactly-once-per-offset semantics. Partitioned by feed / city / service_date,
where service_date is derived from fetched_at via the noon rule in city-local time.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.session import (
    checkpoint_root,
    city_timezones,
    ensure_table,
    kafka_bootstrap,
    trigger_kwargs,
)
from spark_jobs.timeutils import (
    CITY_FALLBACK_CUTOVER_HOURS,
    DEFAULT_FALLBACK_CUTOVER_HOURS,
)

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


def cutover_expr():
    """city -> service-date cutover hours, as a Spark map literal."""
    pairs: list = []
    for city, hours in CITY_FALLBACK_CUTOVER_HOURS.items():
        pairs += [F.lit(city), F.lit(hours)]
    return F.create_map(*pairs) if pairs else F.create_map()


def service_date_expr(fetched_at, tz, city_col=None):
    """The canonical service-date fallback, in ONE place.

    [rev 2026-08-25] This rule had three Spark implementations — here, in
    silver_normalize.with_common, and in silver_normalize.alerts — and two of them
    hardcoded -12h, so Toronto (which rolls at 04:00 and sets start_date on no trips)
    came out a day early in both. 73 of 297 live toronto alert rows were misdated.
    Constants live in spark_jobs.timeutils; every caller goes through here.
    """
    city_col = F.col("city") if city_col is None else city_col
    cutover = F.coalesce(cutover_expr()[city_col], F.lit(DEFAULT_FALLBACK_CUTOVER_HOURS))
    return F.to_date(F.from_utc_timestamp(fetched_at, tz) - F.make_dt_interval(hours=cutover))


def build_stream(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap())
        .option("subscribePattern", "transit\\..*")
        .option("startingOffsets", os.environ.get("TP_STARTING_OFFSETS", "latest"))
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "1000")  # messages, not bytes: one SF
        # trip-update chunk is ~900 KB while an NYC one is ~50 KB, so 5000
        # could pull GBs into a single batch and OOM the executor
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
        service_date_expr(fetched_at, tz, city).alias("service_date"),
    )


def start(spark: SparkSession):
    ensure_table(spark, TABLE, DDL, "feed, city, service_date")
    return (
        build_stream(spark)
        .writeStream.queryName("bronze_envelopes")
        .format("iceberg")
        .outputMode("append")
        .trigger(**trigger_kwargs())
        .option("checkpointLocation", f"{checkpoint_root()}/bronze_envelopes")
        .toTable(TABLE)
    )
