"""Batch backfill: replay bronze envelopes through the silver builders for windows the
streaming drain dropped.

[2026-09-10] Silver's exact-duplicate drop runs inside a 2-hour event-time watermark,
which also discards any row more than 2h behind the newest event the query has seen.
When the chain moved to a 12-hour cadence, single drains chewed through 7-13h of Kafka
backlog with partitions progressing unevenly, the watermark ran ahead of the slow ones,
and whole hours were thrown away as "late": Helsinki lost 06-07Z and 15-21Z on
2026-09-10, NYC/DC/SF/Zurich 06Z — while bronze, which has no watermark, kept every
fetch (docs/08). Bronze holds the envelope exactly as Kafka carried it, so this feeds it
through the SAME builders (parse, allow-list, trip_uid, dedup) rather than a second
implementation that could drift.

Idempotent by construction: a row whose dedup key already exists in silver — looked up
from 2h before the window, the stream's own dedup horizon — is dropped before the
append, so re-running a window appends nothing.

Usage (EMR, launched through the Dagster silver_backfill job):
    silver_backfill.py <city>,<start_utc>,<end_utc> [...]
    e.g. helsinki,2026-09-10T14:00,2026-09-10T22:30
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from pyspark.sql import functions as F

from spark_jobs import silver_normalize as sn
from spark_jobs.session import build_spark

BRONZE = "lake.bronze.envelopes"
DEDUP_HORIZON = timedelta(hours=2)  # the stream's watermark


def parse_windows(args: list[str]) -> list[tuple[str, datetime, datetime]]:
    if not args:
        raise SystemExit("usage: silver_backfill.py <city>,<start_utc>,<end_utc> ...")
    windows = []
    for arg in args:
        city, start, end = arg.split(",")
        lo, hi = datetime.fromisoformat(start), datetime.fromisoformat(end)
        if hi <= lo:
            raise ValueError(f"window ends before it starts: {arg}")
        windows.append((city, lo, hi))
    return windows


def in_windows(windows, lead: timedelta = timedelta(0)):
    cond = F.lit(False)
    for city, lo, hi in windows:
        cond = cond | ((F.col("city") == city) & F.col("fetched_at").between(lo - lead, hi))
    return cond


def bronze_reader(windows):
    def read(spark, topic, record):
        raw = (
            spark.table(BRONZE)
            .where((F.col("topic") == topic) & in_windows(windows))
            .select(F.col("envelope_json").alias("value"))
        )
        return sn.parse_envelopes(raw, record)

    return read


def main() -> None:
    windows = parse_windows(sys.argv[1:])
    spark = build_spark("silver-backfill")
    spark.sparkContext.setLogLevel("WARN")
    read = bronze_reader(windows)
    for table, (builder, _ddl) in sn.TABLES.items():
        name = table.split(".")[-1]
        keys = sn.DEDUP_KEYS[name]
        existing = (
            spark.table(table)
            .where(in_windows(windows, DEDUP_HORIZON))
            .select(*[F.col(k).alias(f"_have_{k}") for k in keys])
            .distinct()
        )
        fresh = builder(spark, read=read)
        on = [fresh[k].eqNullSafe(F.col(f"_have_{k}")) for k in keys]
        missing = fresh.join(existing, on, "left_anti").cache()
        n = missing.count()
        if n:
            missing.writeTo(table).append()
        print(f"{name}: appended {n} rows", flush=True)
        missing.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
