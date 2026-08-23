"""Silver: Kafka transit.* -> typed, deduplicated Iceberg tables.

- canonical typing per docs/01-data-dictionary.md §A
- exact-duplicate drop (identical re-polled predictions/pings) within a 2h watermark
- trip_uid = sha256(city | service_date | COALESCE(trip_id, route-dir-start_time))
- service_date: trip.start_date when the feed provides it, else noon rule on fetched_at
- sched_arr_ts_utc stays null here; the schedule join lives in dbt (needs trip matching)

Partitioned by city / service_date.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_jobs.bronze_writer import tz_map_expr
from spark_jobs.session import checkpoint_root, ensure_table, kafka_bootstrap, trigger_kwargs

STOP_TIME_EVENT = T.StructType(
    [
        T.StructField("time", T.LongType()),
        T.StructField("delay", T.IntegerType()),
        T.StructField("uncertainty", T.IntegerType()),
    ]
)

TRIP_FIELDS = [
    T.StructField("entity_id", T.StringType()),
    T.StructField("trip_id", T.StringType()),
    T.StructField("route_id", T.StringType()),
    T.StructField("direction_id", T.IntegerType()),
    T.StructField("start_date", T.StringType()),
    T.StructField("start_time", T.StringType()),
    T.StructField("schedule_relationship", T.StringType()),
    T.StructField("vehicle_id", T.StringType()),
    T.StructField("vehicle_label", T.StringType()),
    T.StructField("timestamp", T.LongType()),
]

TRIP_UPDATE_RECORD = T.StructType(
    [
        *TRIP_FIELDS,
        T.StructField(
            "stop_time_updates",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("stop_id", T.StringType()),
                        T.StructField("stop_sequence", T.IntegerType()),
                        T.StructField("arrival", STOP_TIME_EVENT),
                        T.StructField("departure", STOP_TIME_EVENT),
                        T.StructField("schedule_relationship", T.StringType()),
                    ]
                )
            ),
        ),
    ]
)

VEHICLE_POSITION_RECORD = T.StructType(
    [
        *TRIP_FIELDS,
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("bearing", T.DoubleType()),
        T.StructField("speed", T.DoubleType()),
        T.StructField("current_stop_sequence", T.IntegerType()),
        T.StructField("stop_id", T.StringType()),
        T.StructField("current_status", T.StringType()),
        T.StructField("occupancy_status", T.StringType()),
    ]
)

ALERT_RECORD = T.StructType(
    [
        T.StructField("alert_id", T.StringType()),
        T.StructField(
            "active_periods",
            T.ArrayType(
                T.StructType(
                    [T.StructField("start", T.LongType()), T.StructField("end", T.LongType())]
                )
            ),
        ),
        T.StructField(
            "informed_entities",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("agency_id", T.StringType()),
                        T.StructField("route_id", T.StringType()),
                        T.StructField("route_type", T.IntegerType()),
                        T.StructField("stop_id", T.StringType()),
                        T.StructField("trip_id", T.StringType()),
                    ]
                )
            ),
        ),
        T.StructField("cause", T.StringType()),
        T.StructField("effect", T.StringType()),
        T.StructField("severity_level", T.StringType()),
        T.StructField("header_text", T.StringType()),
        T.StructField("description_text", T.StringType()),
    ]
)


def envelope_schema(record: T.StructType) -> T.StructType:
    return T.StructType(
        [
            T.StructField("city", T.StringType()),
            T.StructField("agency", T.StringType()),
            T.StructField("feed", T.StringType()),
            T.StructField("fetched_at", T.StringType()),
            T.StructField("source_format", T.StringType()),
            T.StructField("schema_version", T.IntegerType()),
            T.StructField("endpoint", T.StringType()),
            T.StructField("feed_ts", T.LongType()),
            T.StructField("payload", T.ArrayType(record)),
        ]
    )


def read_topic(spark: SparkSession, topic: str, record: T.StructType) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap())
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "5000")
        .load()
    )
    env = F.from_json(F.col("value").cast("string"), envelope_schema(record)).alias("env")
    return raw.select(env).select("env.*")


# Cutover hours for the fetched_at service-date fallback, per city. The GTFS
# noon rule (-12h) is right for feeds whose records may reference yesterday's
# overnight trips; Toronto's feed sets start_date on NO trips (fixture-verified
# 2026-08-23), so with -12h every record fetched between local midnight and noon
# would be misdated to the previous service day. TTC service day rolls ~04:00
# local -> -4h. Amendment recorded in docs/01 §F and CLAUDE.md canonical rules.
CITY_FALLBACK_CUTOVER_HOURS = {"toronto": 4}
DEFAULT_FALLBACK_CUTOVER_HOURS = 12


def cutover_expr():
    pairs: list = []
    for city, hours in CITY_FALLBACK_CUTOVER_HOURS.items():
        pairs += [F.lit(city), F.lit(hours)]
    return F.create_map(*pairs) if pairs else F.create_map()


def with_common(df: DataFrame) -> DataFrame:
    """fetched_at typing, city tz, service_date (start_date else cutover rule)."""
    tz = F.coalesce(tz_map_expr()[F.col("city")], F.lit("UTC"))
    cutover = F.coalesce(cutover_expr()[F.col("city")], F.lit(DEFAULT_FALLBACK_CUTOVER_HOURS))
    df = df.withColumn("fetched_at", F.to_timestamp("fetched_at")).withColumn(
        "service_date",
        F.coalesce(
            F.to_date(F.col("rec.start_date"), "yyyyMMdd"),
            F.to_date(
                F.from_utc_timestamp(F.col("fetched_at"), tz) - F.make_dt_interval(hours=cutover)
            ),
        ),
    )
    return df.withColumn(
        "trip_uid",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("city"),
                F.date_format("service_date", "yyyy-MM-dd"),
                F.coalesce(
                    F.col("rec.trip_id"),
                    F.concat_ws(
                        "-",
                        F.col("rec.route_id"),
                        F.col("rec.direction_id"),
                        F.col("rec.start_time"),
                    ),
                ),
            ),
            256,
        ),
    )


def trip_cols():
    return [
        F.col("rec.trip_id").alias("trip_id"),
        F.col("rec.route_id").alias("route_id"),
        F.col("rec.direction_id").alias("direction_id"),
        F.col("rec.start_date").alias("start_date"),
        F.col("rec.start_time").alias("start_time"),
        F.col("rec.schedule_relationship").alias("schedule_relationship"),
        F.col("rec.vehicle_id").alias("vehicle_id"),
        F.col("rec.vehicle_label").alias("vehicle_label"),
    ]


def meta_cols():
    return [
        F.col("city"),
        F.col("agency"),
        F.col("endpoint"),
        F.col("source_format"),
        F.col("schema_version"),
        F.col("feed_ts"),
        F.col("fetched_at"),
        F.col("service_date"),
        F.col("trip_uid"),
    ]


def stop_time_predictions(spark: SparkSession) -> DataFrame:
    df = read_topic(spark, "transit.trip_updates", TRIP_UPDATE_RECORD)
    df = df.select("*", F.explode_outer("payload").alias("rec")).drop("payload")
    df = with_common(df)
    # explode_outer keeps trips with no stop_time_updates (e.g. CANCELED)
    df = df.select("*", F.explode_outer("rec.stop_time_updates").alias("stu"))
    out = df.select(
        *meta_cols(),
        *trip_cols(),
        F.col("rec.timestamp").alias("trip_timestamp"),
        F.col("stu.stop_id").alias("stop_id"),
        F.col("stu.stop_sequence").alias("stop_sequence"),
        F.to_timestamp(F.col("stu.arrival.time")).alias("arr_pred_ts_utc"),
        F.col("stu.arrival.delay").alias("arr_delay_sec"),
        F.col("stu.arrival.uncertainty").alias("arr_uncertainty"),
        F.to_timestamp(F.col("stu.departure.time")).alias("dep_pred_ts_utc"),
        F.col("stu.departure.delay").alias("dep_delay_sec"),
        F.col("stu.schedule_relationship").alias("stu_schedule_relationship"),
        F.lit(None).cast("timestamp").alias("sched_arr_ts_utc"),
    )
    return out.withWatermark("fetched_at", "2 hours").dropDuplicatesWithinWatermark(
        [
            "city",
            "trip_uid",
            "stop_id",
            "stop_sequence",
            "arr_pred_ts_utc",
            "dep_pred_ts_utc",
            "arr_delay_sec",
            "dep_delay_sec",
            "schedule_relationship",
            "stu_schedule_relationship",
        ]
    )


def vehicle_positions(spark: SparkSession) -> DataFrame:
    df = read_topic(spark, "transit.vehicle_positions", VEHICLE_POSITION_RECORD)
    df = df.select("*", F.explode_outer("payload").alias("rec")).drop("payload")
    df = with_common(df)
    out = df.select(
        *meta_cols(),
        *trip_cols(),
        F.coalesce(F.to_timestamp(F.col("rec.timestamp")), F.col("fetched_at")).alias("ts_utc"),
        F.col("rec.lat").alias("lat"),
        F.col("rec.lon").alias("lon"),
        F.col("rec.bearing").alias("bearing"),
        F.col("rec.speed").alias("speed"),
        F.col("rec.current_stop_sequence").alias("current_stop_sequence"),
        F.col("rec.stop_id").alias("stop_id"),
        F.col("rec.current_status").alias("current_status"),
        F.col("rec.occupancy_status").alias("occupancy_status"),
    )
    return out.withWatermark("fetched_at", "2 hours").dropDuplicatesWithinWatermark(
        ["city", "trip_uid", "vehicle_id", "ts_utc", "current_status", "stop_id"]
    )


def alerts(spark: SparkSession) -> DataFrame:
    df = read_topic(spark, "transit.alerts", ALERT_RECORD)
    df = df.select("*", F.explode_outer("payload").alias("rec")).drop("payload")
    tz = F.coalesce(tz_map_expr()[F.col("city")], F.lit("UTC"))
    df = df.withColumn("fetched_at", F.to_timestamp("fetched_at")).withColumn(
        "service_date",
        F.to_date(F.from_utc_timestamp(F.col("fetched_at"), tz) - F.expr("INTERVAL 12 HOURS")),
    )
    out = df.select(
        F.col("city"),
        F.col("agency"),
        F.col("endpoint"),
        F.col("source_format"),
        F.col("schema_version"),
        F.col("feed_ts"),
        F.col("fetched_at"),
        F.col("service_date"),
        F.col("rec.alert_id").alias("alert_id"),
        F.col("rec.cause").alias("cause"),
        F.col("rec.effect").alias("effect"),
        F.col("rec.severity_level").alias("severity_level"),
        F.col("rec.header_text").alias("header_text"),
        F.col("rec.description_text").alias("description_text"),
        F.to_json(F.col("rec.active_periods")).alias("active_periods_json"),
        F.to_json(F.col("rec.informed_entities")).alias("informed_entities_json"),
    )
    out = out.withColumn(
        "content_hash",
        F.sha2(
            F.concat_ws(
                "|",
                "alert_id",
                "cause",
                "effect",
                "severity_level",
                F.coalesce("header_text", F.lit("")),
                F.coalesce("active_periods_json", F.lit("")),
                F.coalesce("informed_entities_json", F.lit("")),
            ),
            256,
        ),
    )
    return out.withWatermark("fetched_at", "2 hours").dropDuplicatesWithinWatermark(
        ["city", "alert_id", "content_hash"]
    )


PREDICTIONS_DDL = """
    city string, agency string, endpoint string, source_format string, schema_version int,
    feed_ts bigint, fetched_at timestamp, service_date date, trip_uid string,
    trip_id string, route_id string, direction_id int, start_date string, start_time string,
    schedule_relationship string, vehicle_id string, vehicle_label string, trip_timestamp bigint,
    stop_id string, stop_sequence int,
    arr_pred_ts_utc timestamp, arr_delay_sec int, arr_uncertainty int,
    dep_pred_ts_utc timestamp, dep_delay_sec int, stu_schedule_relationship string,
    sched_arr_ts_utc timestamp
"""

POSITIONS_DDL = """
    city string, agency string, endpoint string, source_format string, schema_version int,
    feed_ts bigint, fetched_at timestamp, service_date date, trip_uid string,
    trip_id string, route_id string, direction_id int, start_date string, start_time string,
    schedule_relationship string, vehicle_id string, vehicle_label string,
    ts_utc timestamp, lat double, lon double, bearing double, speed double,
    current_stop_sequence int, stop_id string, current_status string, occupancy_status string
"""

ALERTS_DDL = """
    city string, agency string, endpoint string, source_format string, schema_version int,
    feed_ts bigint, fetched_at timestamp, service_date date,
    alert_id string, cause string, effect string, severity_level string,
    header_text string, description_text string,
    active_periods_json string, informed_entities_json string, content_hash string
"""

TABLES = {
    "lake.silver.stop_time_predictions": (stop_time_predictions, PREDICTIONS_DDL),
    "lake.silver.vehicle_positions": (vehicle_positions, POSITIONS_DDL),
    "lake.silver.alerts": (alerts, ALERTS_DDL),
}


def start(spark: SparkSession):
    queries = []
    for table, (builder, ddl) in TABLES.items():
        ensure_table(spark, table, ddl, "city, service_date")
        name = table.split(".")[-1]
        queries.append(
            builder(spark)
            .writeStream.queryName(f"silver_{name}")
            .format("iceberg")
            .outputMode("append")
            .trigger(**trigger_kwargs())
            .option("checkpointLocation", f"{checkpoint_root()}/silver_{name}")
            .toTable(table)
        )
    return queries
