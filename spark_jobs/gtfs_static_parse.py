"""Static GTFS: download, version, archive raw zip, parse with Spark into
lake.silver.gtfs_static_* (partitioned city / gtfs_version_id, idempotent per version).

Usage: python -m spark_jobs.gtfs_static_parse <city>

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
from datetime import UTC, datetime
from pathlib import Path

import boto3
import requests
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingestion.adapters.base import load_city_config
from spark_jobs.session import build_spark

# file name -> columns cast from string
CASTS = {
    "routes": {"route_type": "int"},
    "trips": {"direction_id": "int"},
    "stops": {"stop_lat": "double", "stop_lon": "double", "location_type": "int"},
    "stop_times": {"stop_sequence": "int", "timepoint": "int"},
    "calendar": {
        d: "int"
        for d in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
    },
    "calendar_dates": {"exception_type": "int"},
    "shapes": {
        "shape_pt_lat": "double",
        "shape_pt_sequence": "int",
        "shape_pt_lon": "double",
        "shape_dist_traveled": "double",
    },
}


def gtfs_seconds(col: str):
    """'25:30:00' -> 91800, as a Spark expression (handles >24h)."""
    parts = F.split(F.col(col), ":")
    return (
        parts.getItem(0).cast("int") * 3600
        + parts.getItem(1).cast("int") * 60
        + parts.getItem(2).cast("int")
    )


def download(url: str) -> bytes:
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    return resp.content


def archive_zip(city: str, version_id: str, blob: bytes) -> None:
    boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        region_name="us-east-1",
    ).put_object(
        Bucket=os.environ.get("RAW_BUCKET", "raw"),
        Key=f"static/{city}/{version_id}/gtfs.zip",
        Body=blob,
    )


def write_table(spark: SparkSession, df: DataFrame, name: str) -> None:
    table = f"lake.silver.gtfs_static_{name}"
    writer = df.writeTo(table).partitionedBy("city", "gtfs_version_id")
    if spark.catalog.tableExists(table):
        writer.overwritePartitions()
    else:
        writer.create()


def parse(spark: SparkSession, city: str, version_id: str, extract_dir: Path) -> None:
    for name, casts in CASTS.items():
        src = extract_dir / f"{name}.txt"
        if not src.exists():
            print(f"{name}.txt absent in feed; skipped")
            continue
        df = spark.read.csv(str(src), header=True)
        for col, dtype in casts.items():
            if col in df.columns:
                df = df.withColumn(col, F.col(col).cast(dtype))
        if name == "stop_times":
            df = df.withColumn("arrival_seconds", gtfs_seconds("arrival_time")).withColumn(
                "departure_seconds", gtfs_seconds("departure_time")
            )
        df = df.withColumn("city", F.lit(city)).withColumn("gtfs_version_id", F.lit(version_id))
        write_table(spark, df, name)
        print(f"gtfs_static_{name}: {df.count()} rows @ {version_id}")


def main() -> None:
    load_dotenv()
    city = sys.argv[1] if len(sys.argv) > 1 else "nyc"
    cfg = load_city_config(city)
    if not cfg.static_gtfs:
        raise SystemExit(f"{city}.yaml has no static_gtfs URL")
    blob = download(cfg.static_gtfs)
    version_id = f"{city}-{datetime.now(UTC):%Y%m%d}-{hashlib.sha256(blob).hexdigest()[:8]}"
    archive_zip(city, version_id, blob)
    spark = build_spark(f"gtfs-static-{city}")
    spark.sparkContext.setLogLevel("WARN")
    with tempfile.TemporaryDirectory() as tmp:
        zipfile.ZipFile(io.BytesIO(blob)).extractall(tmp)
        parse(spark, city, version_id, Path(tmp))
    spark.stop()


if __name__ == "__main__":
    main()
