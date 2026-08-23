"""Static GTFS: download, version, archive raw zip, parse with Spark into
lake.silver.gtfs_static_* (partitioned city / gtfs_version_id, idempotent per version).

Usage:
    python -m spark_jobs.gtfs_static_parse <city>                # local: download + parse
    python -m spark_jobs.gtfs_static_parse <city> <version_id>   # EMR: parse staged zip

Two-arg mode reads the zip the Dagster asset already archived at
raw/static/<city>/<version_id>/gtfs.zip. EMR Serverless base Python has boto3
but not requests/yaml/confluent_kafka, so this module must not import
ingestion.adapters.* — the download path (local dev only) lazy-imports its deps.

gtfs_version_id = <city>-<download date>-<sha256[:8] of zip>, so re-running on an
unchanged upstream file overwrites the same partition. GTFS times may exceed
24:00:00 — stop_times gains arrival_seconds/departure_seconds (int, >86400 allowed);
turning those into UTC timestamps per service_date happens downstream with the
noon-minus-12h rule (spark_jobs.timeutils / dbt macro).
"""

from __future__ import annotations

import hashlib
import io
import os
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.session import build_spark

UTC = timezone.utc  # noqa: UP017 — EMR Serverless runs py3.9; datetime.UTC needs 3.11

# Canonical projection per table: dictionary §D "key fields we use", column -> type.
# Every city's CSV is conformed to exactly this schema before writing (missing
# columns become typed nulls, extras are dropped) — per-city header drift is real
# (MBTA adds route_fare_class, TTC/HSL lack route_sort_order, HSL ships a BOM)
# and Iceberg by-name writes reject any mismatch.
CANONICAL = {
    "routes": {
        "route_id": "string",
        "agency_id": "string",
        "route_short_name": "string",
        "route_long_name": "string",
        "route_type": "int",
        "route_color": "string",
    },
    "trips": {
        "trip_id": "string",
        "route_id": "string",
        "service_id": "string",
        "direction_id": "int",
        "trip_headsign": "string",
        "shape_id": "string",
        "block_id": "string",
    },
    "stops": {
        "stop_id": "string",
        "stop_name": "string",
        "stop_lat": "double",
        "stop_lon": "double",
        "parent_station": "string",
        "location_type": "int",
        "zone_id": "string",
    },
    "stop_times": {
        "trip_id": "string",
        "stop_id": "string",
        "stop_sequence": "int",
        "arrival_time": "string",
        "departure_time": "string",
        "timepoint": "int",
    },
    "calendar": {
        "service_id": "string",
        **{
            d: "int"
            for d in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
        },
        "start_date": "string",
        "end_date": "string",
    },
    "calendar_dates": {"service_id": "string", "date": "string", "exception_type": "int"},
    "shapes": {
        "shape_id": "string",
        "shape_pt_lat": "double",
        "shape_pt_lon": "double",
        "shape_pt_sequence": "int",
        "shape_dist_traveled": "double",
    },
}


def conform(df: DataFrame, schema: dict[str, str]) -> DataFrame:
    """Project to the canonical column list: strip BOM from header names,
    cast present columns, add absent ones as typed nulls, drop extras."""
    df = df.toDF(*[c.lstrip("﻿").strip() for c in df.columns])
    cols = [
        F.col(name).cast(dtype).alias(name)
        if name in df.columns
        else F.lit(None).cast(dtype).alias(name)
        for name, dtype in schema.items()
    ]
    return df.select(*cols)


def gtfs_seconds(col: str):
    """'25:30:00' -> 91800, as a Spark expression (handles >24h)."""
    parts = F.split(F.col(col), ":")
    return (
        parts.getItem(0).cast("int") * 3600
        + parts.getItem(1).cast("int") * 60
        + parts.getItem(2).cast("int")
    )


def _s3():
    # same env contract as ingestion.adapters.base.s3_client, duplicated here
    # because that module needs requests/yaml/confluent_kafka — absent on EMR
    endpoint = os.environ.get("MINIO_ENDPOINT")
    if endpoint:
        return boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
            region_name="us-east-1",
        )
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-2"))


def static_prefix(city: str, version_id: str) -> str:
    return f"static/{city}/{version_id}"


def zip_key(city: str, version_id: str) -> str:
    return f"{static_prefix(city, version_id)}/gtfs.zip"


def raw_base_uri() -> str:
    """Spark-readable URI for the raw bucket: EMR's native committer speaks
    s3://, the local MinIO session config only maps s3a://."""
    scheme = "s3" if os.environ.get("TP_CLOUD") == "1" else "s3a"
    return f"{scheme}://{os.environ.get('RAW_BUCKET', 'raw')}"


def download(url: str) -> bytes:
    import requests  # local-dev only; absent on EMR Serverless base Python

    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    return resp.content


def archive_zip(city: str, version_id: str, blob: bytes) -> None:
    _s3().put_object(
        Bucket=os.environ.get("RAW_BUCKET", "raw"),
        Key=zip_key(city, version_id),
        Body=blob,
    )


def fetch_archived(city: str, version_id: str) -> bytes:
    obj = _s3().get_object(
        Bucket=os.environ.get("RAW_BUCKET", "raw"), Key=zip_key(city, version_id)
    )
    return obj["Body"].read()


def write_table(spark: SparkSession, df: DataFrame, name: str) -> None:
    table = f"lake.silver.gtfs_static_{name}"
    if spark.catalog.tableExists(table):
        existing = set(spark.table(table).columns)
        if existing != set(df.columns):
            # pre-canonical table (schema fixed by whichever city loaded first);
            # rebuild on the canonical projection — versioned partitions mean
            # every city repopulates on its next parse run
            print(f"{table}: schema drift {sorted(existing ^ set(df.columns))}; recreating")
            spark.sql(f"drop table {table}")
    writer = df.writeTo(table).partitionedBy("city", "gtfs_version_id")
    if spark.catalog.tableExists(table):
        writer.overwritePartitions()
    else:
        writer.create()


def stage_texts(city: str, version_id: str, extract_dir: Path) -> None:
    """Upload the canonical .txt members to raw. Executors run in separate
    containers and cannot see the driver's tempdir — reading
    file:/tmp/... fails on EMR with SparkFileNotFoundException."""
    s3 = _s3()
    bucket = os.environ.get("RAW_BUCKET", "raw")
    for name in CANONICAL:
        src = extract_dir / f"{name}.txt"
        if src.exists():
            s3.upload_file(str(src), bucket, f"{static_prefix(city, version_id)}/txt/{name}.txt")


def parse(spark: SparkSession, city: str, version_id: str, extract_dir: Path) -> None:
    base = f"{raw_base_uri()}/{static_prefix(city, version_id)}/txt"
    for name, schema in CANONICAL.items():
        src = extract_dir / f"{name}.txt"
        if not src.exists():
            print(f"{name}.txt absent in feed; skipped")
            continue
        df = conform(spark.read.csv(f"{base}/{name}.txt", header=True), schema)
        if name == "stop_times":
            df = df.withColumn("arrival_seconds", gtfs_seconds("arrival_time")).withColumn(
                "departure_seconds", gtfs_seconds("departure_time")
            )
        df = df.withColumn("city", F.lit(city)).withColumn("gtfs_version_id", F.lit(version_id))
        write_table(spark, df, name)
        print(f"gtfs_static_{name}: {df.count()} rows @ {version_id}")


def main() -> None:
    city = sys.argv[1] if len(sys.argv) > 1 else "nyc"
    version_id = sys.argv[2] if len(sys.argv) > 2 else None
    if version_id:  # EMR mode: the Dagster asset staged the zip already
        blob = fetch_archived(city, version_id)
    else:  # local dev: download + version + archive here
        from dotenv import load_dotenv

        from ingestion.adapters.base import load_city_config

        load_dotenv()
        cfg = load_city_config(city)
        if not cfg.static_gtfs:
            raise SystemExit(f"{city}.yaml has no static_gtfs URL")
        blob = download(cfg.static_gtfs)
        sha8 = hashlib.sha256(blob).hexdigest()[:8]
        version_id = f"{city}-{datetime.now(UTC):%Y%m%d}-{sha8}"
        archive_zip(city, version_id, blob)
    spark = build_spark(f"gtfs-static-{city}")
    spark.sparkContext.setLogLevel("WARN")
    with tempfile.TemporaryDirectory() as tmp:
        zipfile.ZipFile(io.BytesIO(blob)).extractall(tmp)
        stage_texts(city, version_id, Path(tmp))
        parse(spark, city, version_id, Path(tmp))
    spark.stop()


if __name__ == "__main__":
    main()
