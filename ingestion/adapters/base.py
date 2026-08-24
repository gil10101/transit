"""Shared ingestion plumbing: city config, HTTP fetch, Kafka emit, raw-byte archive.

Every adapter emits the canonical envelope (docs/01-data-dictionary.md §F) to the three
transit.* topics, keyed by city, and archives the exact fetched bytes to the raw bucket
under hourly prefixes for replay.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
import yaml
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from ingestion.city_static import StaticSource, auth_secret, resolve_auth, static_sources

UTC = timezone.utc  # noqa: UP017 — EMR Serverless runs py3.9; datetime.UTC needs 3.11

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "cities"
POLL_FLOOR_SECONDS = 30
# Under the broker's default 1 MiB message.max.bytes, with room for the JSON
# framing and Kafka's own record overhead.
MAX_MESSAGE_BYTES = 900_000

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
    # raw `static_gtfs` value: a URL string, or {name: {url, auth}} for agencies
    # that split the schedule across zips / gate it behind a key. Read it through
    # `static_sources` — never directly.
    static_gtfs: str | dict | None = None
    # City-level auth: {type: header|query, name: <header/param name>, env: <ENV VAR>}.
    # The secret NEVER appears in yaml or code — only the env var NAME does, resolved
    # per request in fetch_feed (P3 batch 2: WMATA header, 511 query param).
    auth: dict = field(default_factory=dict)
    # Per-endpoint override, same shape, for cities issuing one token per API product
    # (Zurich: gtfs-rt vs gtfs-sa tokens differ). Falls back to city-level `auth`.
    endpoint_auth: dict[str, dict] = field(default_factory=dict)

    @property
    def effective_poll_seconds(self) -> int:
        return max(POLL_FLOOR_SECONDS, self.poll_seconds)

    def auth_for(self, endpoint: str | None) -> dict:
        return self.endpoint_auth.get(endpoint) or self.auth

    @property
    def static_sources(self) -> list[StaticSource]:
        return static_sources(self.static_gtfs)


def load_city_config(city: str) -> CityConfig:
    raw = yaml.safe_load((CONFIG_DIR / f"{city}.yaml").read_text())
    groups = raw.get("feed_groups") or raw.get("feeds")
    if not groups:
        raise ValueError(f"{city}.yaml must define feed_groups or feeds")
    # feed_groups values are either a bare URL string or {url: ..., auth: {...}}
    endpoints: dict[str, str] = {}
    endpoint_auth: dict[str, dict] = {}
    for name, value in dict(groups).items():
        if isinstance(value, dict):
            endpoints[name] = value["url"]
            if value.get("auth"):
                endpoint_auth[name] = dict(value["auth"])
        else:
            endpoints[name] = value
    return CityConfig(
        city=raw["city"],
        agency=raw["agency"],
        timezone=raw["timezone"],
        adapter=raw["adapter"],
        poll_seconds=int(raw.get("poll_seconds", POLL_FLOOR_SECONDS)),
        endpoints=endpoints,
        static_gtfs=raw.get("static_gtfs"),
        auth=raw.get("auth") or {},
        endpoint_auth=endpoint_auth,
    )


def build_session(cfg: CityConfig) -> requests.Session:
    """UA-tagged session. Auth is applied per request in fetch_feed (never baked into
    session headers) so per-endpoint tokens can differ within one city."""
    session = requests.Session()
    session.headers["User-Agent"] = "transit-pulse/0.1"
    return session


def _auth_secret(auth: dict) -> str:
    """Kept as the module's historical name; the implementation is shared with
    the Dagster static stager in ingestion/city_static.py."""
    return auth_secret(auth)


def fetch_feed(
    session: requests.Session,
    cfg: CityConfig,
    url: str,
    timeout: int = 15,
    endpoint: str | None = None,
) -> bytes:
    """GET one feed URL with the endpoint's auth applied.

    - header auth (WMATA `api_key`; Swiss OTD raw `Authorization`, no Bearer prefix)
      goes on the request, not the session. Swiss endpoints 302 cross-host to a
      pre-signed largeapi.opentransportdata.swiss URL: requests strips Authorization
      on the cross-host hop (its documented behavior) and the signed target needs no
      auth — verified live 2026-08-23, so default redirect handling is correct.
    - query auth (511 `api_key`) is merged by requests with params already in the
      URL (511 keeps `agency=RG` in the URL itself).
    """
    headers, params = resolve_auth(cfg.auth_for(endpoint))
    resp = session.get(url, params=params or None, headers=headers or None, timeout=timeout)
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
        """Emit one envelope, splitting the payload when the encoded message
        would exceed the broker's per-message limit.

        511's regional feed carries every Bay Area operator in one response
        (~3,800 trip updates, 2.4 MB encoded) and the broker rejects that with
        MSG_SIZE_TOO_LARGE. Chunks keep the envelope's metadata identical and
        only slice `payload`, so downstream is unchanged: silver explodes the
        payload array either way, and dedup is per record."""
        topic = TOPICS[envelope["feed"]]
        for chunk in self._size_bounded(envelope):
            self.producer.produce(topic, key=envelope["city"].encode(), value=chunk)

    def _size_bounded(self, envelope: dict) -> list[bytes]:
        encoded = json.dumps(envelope, separators=(",", ":")).encode()
        payload = envelope.get("payload") or []
        if len(encoded) <= MAX_MESSAGE_BYTES or len(payload) <= 1:
            return [encoded]
        mid = len(payload) // 2
        out = []
        for half in (payload[:mid], payload[mid:]):
            out += self._size_bounded({**envelope, "payload": half})
        return out

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
