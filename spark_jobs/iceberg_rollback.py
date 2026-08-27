"""One-shot Iceberg rollback: restore bronze+silver tables to their last snapshot
BEFORE a given UTC timestamp. Written for the 2026-08-27 checkpoint-v2 incident
(startingOffsets=earliest on a fresh checkpoint replayed the whole Kafka retention
window as duplicates); kept because the shape is general.

Usage (EMR or local spark-submit):
    iceberg_rollback.py <utc-timestamp e.g. 2026-08-27T00:30:00>

Rolls back exactly these tables, skipping any with no snapshot older than the cut:
bronze.envelopes, silver.stop_time_predictions, silver.vehicle_positions,
silver.alerts. Prints per-table row counts before/after; exits nonzero on failure.
"""

import sys

from pyspark.sql import SparkSession

TABLES = [
    "lake.bronze.envelopes",
    "lake.silver.stop_time_predictions",
    "lake.silver.vehicle_positions",
    "lake.silver.alerts",
]


def main() -> None:
    cut = sys.argv[1]
    spark = SparkSession.builder.appName("iceberg-rollback").getOrCreate()
    failures = []
    for table in TABLES:
        before = spark.sql(f"select count(*) c from {table}").collect()[0].c
        snap = spark.sql(
            f"select snapshot_id, committed_at from {table}.snapshots "
            f"where committed_at < TIMESTAMP '{cut}' "
            "order by committed_at desc limit 1"
        ).collect()
        if not snap:
            print(f"{table}: NO snapshot before {cut}; skipping")
            continue
        sid, at = snap[0].snapshot_id, snap[0].committed_at
        spark.sql(f"CALL lake.system.rollback_to_snapshot('{table}', {sid})")
        after = spark.sql(f"select count(*) c from {table}").collect()[0].c
        print(f"{table}: rolled back to {sid} ({at}); rows {before} -> {after}")
        if after > before:
            failures.append(table)
    if failures:
        print(f"FAILED sanity (rows grew): {failures}")
        sys.exit(1)


if __name__ == "__main__":
    main()
