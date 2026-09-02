"""ODPT JSON statics: download the dump-API files, version, archive raw, parse into
lake.silver.odpt_* (partitioned city / odpt_version_id, idempotent per version).

Usage:
    python -m spark_jobs.odpt_static_parse                      # local: download + parse
    python -m spark_jobs.odpt_static_parse tokyo <version_id>   # EMR: parse staged dumps

Two-arg mode reads the JSON files the Dagster asset already archived at
static/tokyo/<version_id>/odpt/<Type>.json. Dump URLs (live-verified 2026-09-01,
docs/01 §C.4/§D): https://api.odpt.org/api/v4/odpt:<Type>.json?acl:consumerKey=…
— the dump API 301-redirects to the file (requests follows by default) and is the
only full-static path (the retrieval API caps static types at 1000 entries).

Objects are filtered to the TokyoMetro/Toei operators (the center hosts every member
operator's data). odpt:TrainTimetable explodes to stop grain here so dbt stays SQL:
`stop_sequence` is the 1-based ordinal of trainTimetableObject, and times ("HH:MM",
WRAPPED at midnight — 00:10 after 23:59, never "24:10") are reconstructed into
GTFS-style seconds-since-service-day-start (>86400 allowed): +24h whenever the clock
runs backwards mid-timetable (88 Toei timetables cross midnight), and a +24h base
offset when the FIRST time is before the tokyo service-day cutover (43 Toei
timetables start 00:00-03:00 — they belong to the previous service day, consistent
with CITY_FALLBACK_CUTOVER_HOURS['tokyo']).
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone

import boto3
from pyspark.sql import SparkSession

from spark_jobs.session import build_spark
from spark_jobs.timeutils import cutover_hours_for

UTC = timezone.utc  # noqa: UP017 — EMR Serverless runs py3.9; datetime.UTC needs 3.11

CITY = "tokyo"
DUMP_TYPES = ("Station", "Railway", "TrainTimetable", "Calendar")
OPERATORS = ("odpt.Operator:TokyoMetro", "odpt.Operator:Toei")
DUMP_URL = "https://api.odpt.org/api/v4/odpt:{rdf_type}.json"

SCHEMAS = {
    "odpt_station": (
        "city string, odpt_version_id string, station_urn string, operator string,"
        " railway_urn string, station_code string, title_ja string, title_en string,"
        " lat double, lon double"
    ),
    "odpt_railway": (
        "city string, odpt_version_id string, railway_urn string, operator string,"
        " title_ja string, title_en string, line_code string,"
        " ascending_direction string, descending_direction string,"
        " station_urn string, station_index int"
    ),
    "odpt_train_timetable": (
        "city string, odpt_version_id string, timetable_urn string, operator string,"
        " railway_urn string, rail_direction string, calendar_urn string,"
        " train_number string, train_type string, train_urn string,"
        " origin_stations_json string, destination_stations_json string,"
        " stop_sequence int, station_urn string,"
        " arrival_time string, departure_time string,"
        " arrival_seconds int, departure_seconds int"
    ),
    "odpt_calendar": (
        "city string, odpt_version_id string, calendar_urn string, title_ja string, title_en string"
    ),
}


def _tail(urn: str | None) -> str | None:
    return urn.split(":", 1)[-1] if urn else None


def _title(obj: dict, key: str, lang: str) -> str | None:
    multi = obj.get(key) or {}
    return multi.get(lang) if isinstance(multi, dict) else None


def _ours(obj: dict) -> bool:
    """Keep our operators' objects; objects with NO operator (the generic
    odpt.Calendar:Weekday family) are shared and kept."""
    op = obj.get("odpt:operator")
    return op is None or op in OPERATORS


def station_rows(obj: dict) -> list[dict]:
    return [
        {
            "station_urn": obj.get("owl:sameAs"),
            "operator": _tail(obj.get("odpt:operator")),
            "railway_urn": obj.get("odpt:railway"),
            "station_code": obj.get("odpt:stationCode"),
            "title_ja": obj.get("dc:title") or _title(obj, "odpt:stationTitle", "ja"),
            "title_en": _title(obj, "odpt:stationTitle", "en"),
            "lat": obj.get("geo:lat"),
            "lon": obj.get("geo:long"),
        }
    ]


def railway_rows(obj: dict) -> list[dict]:
    base = {
        "railway_urn": obj.get("owl:sameAs"),
        "operator": _tail(obj.get("odpt:operator")),
        "title_ja": obj.get("dc:title") or _title(obj, "odpt:railwayTitle", "ja"),
        "title_en": _title(obj, "odpt:railwayTitle", "en"),
        "line_code": obj.get("odpt:lineCode"),
        "ascending_direction": obj.get("odpt:ascendingRailDirection"),
        "descending_direction": obj.get("odpt:descendingRailDirection"),
    }
    order = obj.get("odpt:stationOrder") or []
    if not order:
        return [{**base, "station_urn": None, "station_index": None}]
    return [
        {**base, "station_urn": o.get("odpt:station"), "station_index": o.get("odpt:index")}
        for o in order
    ]


def _hhmm_seconds(hhmm: str | None) -> int | None:
    if not hhmm:
        return None
    h, m = hhmm.split(":")
    return int(h) * 3600 + int(m) * 60


def train_timetable_rows(obj: dict) -> list[dict]:
    tto = obj.get("odpt:trainTimetableObject") or []
    base_row = {
        "timetable_urn": obj.get("owl:sameAs"),
        "operator": _tail(obj.get("odpt:operator")),
        "railway_urn": obj.get("odpt:railway"),
        "rail_direction": obj.get("odpt:railDirection"),
        "calendar_urn": obj.get("odpt:calendar"),
        "train_number": obj.get("odpt:trainNumber"),
        "train_type": obj.get("odpt:trainType"),
        "train_urn": obj.get("odpt:train"),
        "origin_stations_json": json.dumps(obj.get("odpt:originStation") or []),
        "destination_stations_json": json.dumps(obj.get("odpt:destinationStation") or []),
    }
    first = next(
        (
            s
            for x in tto
            if (s := _hhmm_seconds(x.get("odpt:departureTime") or x.get("odpt:arrivalTime")))
            is not None
        ),
        None,
    )
    cutover_sec = cutover_hours_for(CITY) * 3600
    offset = 86400 if first is not None and first < cutover_sec else 0
    prev: int | None = None
    rows = []
    for i, x in enumerate(tto):
        adjusted: dict[str, int | None] = {}
        for key, raw_key in (
            ("arrival_seconds", "odpt:arrivalTime"),
            ("departure_seconds", "odpt:departureTime"),
        ):
            sec = _hhmm_seconds(x.get(raw_key))
            if sec is None:
                adjusted[key] = None
                continue
            sec += offset
            if prev is not None and sec < prev:  # clock ran backwards -> crossed midnight
                offset += 86400
                sec += 86400
            prev = sec
            adjusted[key] = sec
        rows.append(
            {
                **base_row,
                "stop_sequence": i + 1,
                "station_urn": x.get("odpt:departureStation") or x.get("odpt:arrivalStation"),
                "arrival_time": x.get("odpt:arrivalTime"),
                "departure_time": x.get("odpt:departureTime"),
                **adjusted,
            }
        )
    return rows


def calendar_rows(obj: dict) -> list[dict]:
    return [
        {
            "calendar_urn": obj.get("owl:sameAs"),
            "title_ja": obj.get("dc:title") or _title(obj, "odpt:calendarTitle", "ja"),
            "title_en": _title(obj, "odpt:calendarTitle", "en"),
        }
    ]


ROW_BUILDERS = {
    "Station": ("odpt_station", station_rows),
    "Railway": ("odpt_railway", railway_rows),
    "TrainTimetable": ("odpt_train_timetable", train_timetable_rows),
    "Calendar": ("odpt_calendar", calendar_rows),
}


def _s3():
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


def dump_key(version_id: str, rdf_type: str) -> str:
    return f"static/{CITY}/{version_id}/odpt/{rdf_type}.json"


def download_dump(rdf_type: str) -> bytes:
    import requests  # local-dev only; the Dagster asset stages for EMR

    key = os.environ.get("ODPT_CONSUMER_KEY", "")
    if not key:
        raise SystemExit("ODPT_CONSUMER_KEY is not set (see .env.example)")
    resp = requests.get(
        DUMP_URL.format(rdf_type=rdf_type),
        params={"acl:consumerKey": key},
        timeout=300,
    )
    resp.raise_for_status()
    return resp.content


def parse(spark: SparkSession, version_id: str) -> None:
    s3 = _s3()
    bucket = os.environ.get("RAW_BUCKET", "raw")
    for rdf_type in DUMP_TYPES:
        table_name, builder = ROW_BUILDERS[rdf_type]
        blob = s3.get_object(Bucket=bucket, Key=dump_key(version_id, rdf_type))["Body"].read()
        objects = [o for o in json.loads(blob) if _ours(o)]
        rows = [
            {**row, "city": CITY, "odpt_version_id": version_id}
            for obj in objects
            for row in builder(obj)
        ]
        table = f"lake.silver.{table_name}"
        df = spark.createDataFrame(rows, schema=SCHEMAS[table_name])
        writer = df.writeTo(table).partitionedBy("city", "odpt_version_id")
        if spark.catalog.tableExists(table):
            writer.overwritePartitions()
        else:
            writer.create()
        print(f"{table}: {len(rows)} rows from {len(objects)} {rdf_type} objects @ {version_id}")


def main() -> None:
    argv_city = sys.argv[1] if len(sys.argv) > 1 else CITY
    if argv_city != CITY:
        raise SystemExit(f"odpt statics are tokyo-only (got {argv_city!r})")
    version_id = sys.argv[2] if len(sys.argv) > 2 else None
    if version_id is None:  # local dev: download + version + archive here
        from dotenv import load_dotenv

        load_dotenv()
        digest = hashlib.sha256()
        blobs = {}
        for rdf_type in DUMP_TYPES:
            blobs[rdf_type] = download_dump(rdf_type)
            digest.update(blobs[rdf_type])
        version_id = f"{CITY}-odpt-{datetime.now(UTC):%Y%m%d}-{digest.hexdigest()[:8]}"
        s3 = _s3()
        bucket = os.environ.get("RAW_BUCKET", "raw")
        for rdf_type, blob in blobs.items():
            s3.put_object(Bucket=bucket, Key=dump_key(version_id, rdf_type), Body=blob)
    spark = build_spark(f"odpt-static-{CITY}")
    spark.sparkContext.setLogLevel("WARN")
    parse(spark, version_id)


if __name__ == "__main__":
    main()
