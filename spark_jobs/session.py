"""SparkSession factory for local runs: Kafka source + Iceberg on MinIO (s3a).

Catalog `lake` is a Hadoop catalog rooted at s3a://$LAKE_BUCKET/iceberg, so table
paths are stable for downstream readers (duckdb iceberg_scan in dbt).
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv
from pyspark import __version__ as spark_version
from pyspark.sql import SparkSession

ICEBERG_VERSION = "1.6.1"
HADOOP_AWS_VERSION = "3.3.4"
AWS_SDK_VERSION = "1.12.262"

CITY_CONFIG_DIR = Path(__file__).resolve().parent.parent / "ingestion" / "config" / "cities"


def city_timezones() -> dict[str, str]:
    out = {}
    for path in CITY_CONFIG_DIR.glob("*.yaml"):
        cfg = yaml.safe_load(path.read_text())
        out[cfg["city"]] = cfg["timezone"]
    return out


def build_spark(app_name: str) -> SparkSession:
    load_dotenv()
    lake_bucket = os.environ.get("LAKE_BUCKET", "lakehouse")
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    access = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    packages = ",".join(
        [
            f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}",
            f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
            f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VERSION}",
        ]
    )
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", packages)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", f"s3a://{lake_bucket}/iceberg")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access)
        .config("spark.hadoop.fs.s3a.secret.key", secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def kafka_bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")


def checkpoint_root() -> str:
    lake_bucket = os.environ.get("LAKE_BUCKET", "lakehouse")
    return f"s3a://{lake_bucket}/checkpoints"


def ensure_table(spark: SparkSession, name: str, ddl_columns: str, partition_by: str) -> None:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {name} ({ddl_columns}) "
        f"USING iceberg PARTITIONED BY ({partition_by})"
    )
