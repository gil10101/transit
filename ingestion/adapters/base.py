"""Shared ingestion plumbing: city config, HTTP fetch, Kafka emit, raw-byte archive.

Every adapter emits the canonical envelope (docs/01-data-dictionary.md §F) to the three
transit.* topics, keyed by city, and archives the exact fetched bytes to the raw bucket
under hourly prefixes for replay.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import boto3
import requests
import yaml
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "cities"
POLL_FLOOR_SECONDS = 30

TOPICS = {
    "trip_updates": "transit.trip_updates",
    "vehicle_positions": "transit.vehicle_positions",
    "alerts": "transit.alerts",
}


@dataclass
class CityConfig:
    city: str
    agency: str
    timezone: str
    adapter: str
    poll_seconds: int
    # endpoint name -> URL. For combined feeds (NYC) names are line groups and each URL
    # yields all three entity types; for per-type feeds names are the feed types.
    endpoints: dict[str, str]
    static_gtfs: str | None = None
    auth: dict = field(default_factory=dict)

    @property
    def effective_poll_seconds(self) -> int:
        return max(POLL_FLOOR_SECONDS, self.poll_seconds)


def load_city_config(city: str) -> CityConfig:
    raw = yaml.safe_load((CONFIG_DIR / f"{city}.yaml").read_text())
    endpoints = raw.get("feed_groups") or raw.get("feeds")
    if not endpoints:
        raise ValueError(f"{city}.yaml must define feed_groups or feeds")
    return CityConfig(
        city=raw["city"],
        agency=raw["agency"],
        timezone=raw["timezone"],
        adapter=raw["adapter"],
        poll_seconds=int(raw.get("poll_seconds", POLL_FLOOR_SECONDS)),
        endpoints=dict(endpoints),
        static_gtfs=raw.get("static_gtfs"),
        auth=raw.get("auth") or {},
    )


def build_session(cfg: CityConfig) -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = "transit-pulse/0.1"
    auth = cfg.auth
    if auth.get("type") == "header":
        session.headers[auth["name"]] = os.environ[auth["env"]]
    return session


def fetch_feed(session: requests.Session, cfg: CityConfig, url: str, timeout: int = 15) -> bytes:
    params = {}
    if cfg.auth.get("type") == "query":
        params[cfg.auth["name"]] = os.environ[cfg.auth["env"]]
    resp = session.get(url, params=params or None, timeout=timeout)
    resp.raise_for_status()
    return resp.content


def build_envelope(
    *,
    city: str,
    agency: str,
    feed: str,
    fetched_at: datetime,
    source_format: str,
    payload: list[dict],
    endpoint: str,
    feed_ts: int | None,
) -> dict:
    """Canonical envelope per §F. `endpoint` and `feed_ts` are additive provenance keys."""
    return {
        "city": city,
        "agency": agency,
        "feed": feed,
        "fetched_at": fetched_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_format": source_format,
        "schema_version": 2,
        "endpoint": endpoint,
        "feed_ts": feed_ts,
        "payload": payload,
    }


class KafkaEmitter:
    def __init__(self, bootstrap: str | None = None):
        self.bootstrap = bootstrap or os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
        self.producer = Producer(
            {
                "bootstrap.servers": self.bootstrap,
                "compression.type": "zstd",
                "linger.ms": 50,
                "message.max.bytes": 10_000_000,
            }
        )

    def ensure_topics(self) -> None:
        admin = AdminClient({"bootstrap.servers": self.bootstrap})
        existing = admin.list_topics(timeout=10).topics
        wanted = [t for t in TOPICS.values() if t not in existing]
        if wanted:
            futures = admin.create_topics(
                [NewTopic(t, num_partitions=3, replication_factor=1) for t in wanted]
            )
            for topic, fut in futures.items():
                fut.result()
                print(f"created topic {topic}")

    def emit(self, envelope: dict) -> None:
        topic = TOPICS[envelope["feed"]]
        self.producer.produce(
            topic,
            key=envelope["city"].encode(),
            value=json.dumps(envelope, separators=(",", ":")).encode(),
        )

    def flush(self) -> None:
        self.producer.flush(10)


def s3_client():
    """MinIO when MINIO_ENDPOINT is set (local dev); default AWS chain otherwise
    (instance role / ~/.aws in cloud)."""
    endpoint = os.environ.get("MINIO_ENDPOINT")
    if endpoint:
        return boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
            region_name="us-east-1",
        )
    return boto3.client("s3")


class RawArchiver:
    """Writes fetched bytes to s3://$RAW_BUCKET/<city>/<endpoint>/<date>/<hour>/<ts>.pb."""

    def __init__(self):
        self.bucket = os.environ.get("RAW_BUCKET", "raw")
        self.client = s3_client()

    def archive(self, city: str, endpoint: str, fetched_at: datetime, raw: bytes) -> str:
        key = (
            f"{city}/{endpoint}/{fetched_at:%Y-%m-%d}/{fetched_at:%H}/"
            f"{fetched_at:%Y%m%dT%H%M%S}Z.pb"
        )
        self.client.put_object(Bucket=self.bucket, Key=key, Body=raw)
        return key


def utcnow() -> datetime:
    return datetime.now(UTC)
