"""Register/refresh Snowflake Iceberg tables over the cloud silver layer.

Hadoop-catalog tables on S3 expose their latest snapshot via metadata/version-hint.text.
Object-store catalog tables in Snowflake pin a metadata file, so this script:
  create catalog integration (once) ->
  per table: CREATE ICEBERG TABLE IF NOT EXISTS pinned to the latest metadata json,
             then ALTER ... REFRESH to that json (no-op if already there).
Re-run any time (Dagster owns the cadence in P5).

Usage: uv run python scripts/snowflake_register_iceberg.py
"""

from __future__ import annotations

import subprocess
import tempfile

import boto3

BUCKET = "transit-pulse-622221238588-lakehouse"
VOLUME = "TRANSIT_LAKEHOUSE"
CATALOG_INTEGRATION = "TRANSIT_OBJ_STORE"
CONNECTION = "transit-svc"

TABLES = [
    "stop_time_predictions",
    "vehicle_positions",
    "alerts",
    "gtfs_static_routes",
    "gtfs_static_trips",
    "gtfs_static_stops",
    "gtfs_static_stop_times",
    "gtfs_static_calendar",
    "gtfs_static_calendar_dates",
    "gtfs_static_shapes",
]


def latest_metadata(s3, table: str) -> str | None:
    key = f"iceberg/silver/{table}/metadata/version-hint.text"
    try:
        version = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode().strip()
    except s3.exceptions.NoSuchKey:
        return None
    # path relative to the external volume base (s3://bucket/iceberg/)
    return f"silver/{table}/metadata/v{version}.metadata.json"


def main() -> None:
    s3 = boto3.client("s3")
    stmts = [
        "use role accountadmin;",
        "use warehouse TRANSFORM_XS;",
        f"""create catalog integration if not exists {CATALOG_INTEGRATION}
              catalog_source = object_store table_format = iceberg enabled = true;""",
    ]
    for table in TABLES:
        meta = latest_metadata(s3, table)
        if meta is None:
            print(f"skip {table}: no version-hint yet")
            continue
        name = f"TRANSIT.SILVER.{table.upper()}"
        stmts.append(
            f"""create iceberg table if not exists {name}
                  external_volume = '{VOLUME}'
                  catalog = '{CATALOG_INTEGRATION}'
                  metadata_file_path = '{meta}';"""
        )
        stmts.append(f"alter iceberg table {name} refresh '{meta}';")
        print(f"{table}: {meta}")
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
        f.write("\n".join(stmts))
        path = f.name
    subprocess.run(["snow", "sql", "-f", path, "--connection", CONNECTION], check=True)


if __name__ == "__main__":
    main()
