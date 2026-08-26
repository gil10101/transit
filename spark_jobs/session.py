"""SparkSession factory for local runs: Kafka source + Iceberg on MinIO (s3a).

Catalog `lake` is a Hadoop catalog rooted at s3a://$LAKE_BUCKET/iceberg, so table
paths are stable for downstream readers (duckdb iceberg_scan in dbt).
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark import __version__ as spark_version
from pyspark.sql import SparkSession

ICEBERG_VERSION = "1.6.1"
HADOOP_AWS_VERSION = "3.3.4"
AWS_SDK_VERSION = "1.12.262"

CITY_CONFIG_DIR = Path(__file__).resolve().parent.parent / "ingestion" / "config" / "cities"


def city_timezones() -> dict[str, str]:
    # cloud: injected via env (EMR python has no yaml); local: read from city configs
    injected = os.environ.get("TP_CITY_TZS")
    if injected:
        return dict(pair.split("=", 1) for pair in injected.split(","))
    import yaml

    out = {}
    for path in CITY_CONFIG_DIR.glob("*.yaml"):
        cfg = yaml.safe_load(path.read_text())
        out[cfg["city"]] = cfg["timezone"]
    return out


def lake_base() -> str:
    """s3a://<bucket> against MinIO locally; TP_LAKE_URI (s3://...) in cloud."""
    return os.environ.get("TP_LAKE_URI", f"s3a://{os.environ.get('LAKE_BUCKET', 'lakehouse')}")


def build_spark(app_name: str) -> SparkSession:
    if os.environ.get("TP_CLOUD") == "1":
        # EMR: catalog, jars, and S3 access arrive via job-submission conf / instance role
        return (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    from dotenv import load_dotenv

    load_dotenv()
    packages = ",".join(
        [
            f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}",
            f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
            f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VERSION}",
        ]
    )
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", packages)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", f"{lake_base()}/iceberg")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
    )
    # MinIO when TP_LAKE_URI is unset (pure local); AWS default credential chain when
    # the laptop targets the cloud lake directly (one-off batch jobs like static parse)
    if os.environ.get("TP_LAKE_URI"):
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
    else:
        builder = (
            builder.config(
                "spark.hadoop.fs.s3a.endpoint",
                os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
            )
            .config(
                "spark.hadoop.fs.s3a.access.key",
                os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            )
            .config(
                "spark.hadoop.fs.s3a.secret.key",
                os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
            )
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )
    return builder.getOrCreate()


def kafka_bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")


def checkpoint_root() -> str:
    # TP_CHECKPOINT_ROOT exists so shuffle-partition changes are POSSIBLE at all: a
    # stateful streaming query cannot change spark.sql.shuffle.partitions on a live
    # checkpoint, so the partition cut (200 -> 16, 2026-08-26 S3-request cost fix)
    # ships together with a fresh checkpoint root. Swap procedure and its ~minutes
    # of accepted gap: docs/08 "Checkpoint v2 migration".
    return os.environ.get("TP_CHECKPOINT_ROOT", f"{lake_base()}/checkpoints")


def ensure_table(spark: SparkSession, name: str, ddl_columns: str, partition_by: str) -> None:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {name} ({ddl_columns}) "
        f"USING iceberg PARTITIONED BY ({partition_by})"
    )


def trigger_kwargs() -> dict:
    """availableNow (drain Kafka, commit, exit) when TP_TRIGGER=available_now —
    the scheduled cloud mode; continuous 30s micro-batches otherwise (local)."""
    if os.environ.get("TP_TRIGGER") == "available_now":
        return {"availableNow": True}
    return {"processingTime": "30 seconds"}
