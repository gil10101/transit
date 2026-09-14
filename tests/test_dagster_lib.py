"""Unit tests for the pure logic behind the Dagster assets (orchestration/).

Covers: EMR job-request assembly (must mirror the drain Lambda's contract),
open-meteo row transforms (UTC -> city-local bucketing), raw-feed freshness
evaluation against a fake S3 listing, and Iceberg refresh statement porting.
"""

import io
import uuid
from datetime import UTC, date, datetime, timedelta

import pytest

from orchestration.transit_dagster.lib import (
    NYC_FEED_ENDPOINTS,
    REAP_AFTER_SECONDS,
    city_feed_endpoints,
    drain_job_request,
    evaluate_feed_freshness,
    hour_prefixes,
    iceberg_refresh_statements,
    latest_metadata_path,
    retired_cities,
    silver_tables,
    static_job_request,
    weather_rows,
    year_chunks,
    zombie_run_ids,
)

ENV = {
    "APP_ID": "00g86oj9urdank0d",
    "EXEC_ROLE_ARN": "arn:aws:iam::622221238588:role/transit-pulse-emr-exec",
    "ENTRY_POINT": "s3://transit-pulse-622221238588-artifacts/code/entry.py",
    "STATIC_ENTRY_POINT": "s3://transit-pulse-622221238588-artifacts/code/gtfs_static_parse.py",
    "SPARK_PARAMS": "--py-files s3://b/code/spark_jobs.zip --conf spark.executor.cores=2",
    "LOG_URI": "s3://transit-pulse-622221238588-artifacts/emr-logs/",
    "RAW_BUCKET": "transit-pulse-622221238588-raw",
}


# --- EMR job requests -------------------------------------------------------


def test_drain_request_mirrors_lambda_contract():
    req = drain_job_request(ENV)
    assert req["applicationId"] == ENV["APP_ID"]
    assert req["executionRoleArn"] == ENV["EXEC_ROLE_ARN"]
    spark = req["jobDriver"]["sparkSubmit"]
    assert spark["entryPoint"] == ENV["ENTRY_POINT"]
    # params pass through untouched: the strings are single-sourced from terraform
    assert spark["sparkSubmitParameters"] == ENV["SPARK_PARAMS"]
    monitoring = req["configurationOverrides"]["monitoringConfiguration"]
    assert monitoring["s3MonitoringConfiguration"]["logUri"] == ENV["LOG_URI"]
    uuid.UUID(req["clientToken"])  # scheduler quirk: every run needs a fresh valid token


def test_static_request_overrides_entrypoint_and_exports_raw_bucket():
    req = static_job_request(ENV, city="nyc", version_id="nyc-20260823-abc12345")
    spark = req["jobDriver"]["sparkSubmit"]
    assert spark["entryPoint"] == ENV["STATIC_ENTRY_POINT"]
    # [city, version_id]: EMR reads the staged zip — it cannot download (no requests)
    assert spark["entryPointArguments"] == ["nyc", "nyc-20260823-abc12345"]
    assert spark["sparkSubmitParameters"].startswith(ENV["SPARK_PARAMS"])
    # the parse job reads the staged zip from raw; the drain params lack RAW_BUCKET
    assert spark["sparkSubmitParameters"].endswith(
        f"--conf spark.emr-serverless.driverEnv.RAW_BUCKET={ENV['RAW_BUCKET']}"
    )
    assert req["name"] != drain_job_request(ENV)["name"]


@pytest.mark.parametrize("missing", ["APP_ID", "SPARK_PARAMS", "LOG_URI"])
def test_missing_env_named_in_error(missing):
    env = {k: v for k, v in ENV.items() if k != missing}
    with pytest.raises(RuntimeError, match=missing):
        drain_job_request(env)


# --- weather ----------------------------------------------------------------


def payload(times, **overrides):
    n = len(times)
    hourly = {
        "time": times,
        "temperature_2m": [20.0] * n,
        "precipitation": [0.0] * n,
        "rain": [0.0] * n,
        "snowfall": [0.0] * n,
        "wind_speed_10m": [10.0] * n,
        "weather_code": [1] * n,
    }
    hourly.update(overrides)
    return {"hourly": hourly}


def test_weather_rows_bucket_to_local_calendar():
    pulled = datetime(2026, 8, 22, 12, 30, tzinfo=UTC)
    rows = weather_rows(
        "nyc",
        "America/New_York",
        payload(
            ["2026-08-22T03:00", "2026-08-22T04:00"],
            temperature_2m=[21.5, 20.9],
            precipitation=[0.0, 1.2],
            wind_speed_10m=[10.1, 12.3],
            weather_code=[1, 61],
        ),
        pulled,
    )
    # 03:00Z = 23:00 EDT Aug 21; 04:00Z = 00:00 EDT Aug 22
    assert rows[0] == (
        "nyc",
        date(2026, 8, 21),
        23,
        21.5,
        0.0,
        0.0,
        10.1,
        1,
        pulled.replace(tzinfo=None),
    )
    assert rows[1] == (
        "nyc",
        date(2026, 8, 22),
        0,
        20.9,
        1.2,
        0.0,
        12.3,
        61,
        pulled.replace(tzinfo=None),
    )


def test_weather_rows_dedupe_dst_fall_back():
    # 2026-11-01: 05:00Z = 01:00 EDT and 06:00Z = 01:00 EST — one local hour,
    # one MERGE key; last (later UTC) reading wins.
    rows = weather_rows(
        "nyc",
        "America/New_York",
        payload(["2026-11-01T05:00", "2026-11-01T06:00"], temperature_2m=[10.0, 9.0]),
        datetime.now(UTC),
    )
    assert len(rows) == 1
    assert rows[0][1:4] == (date(2026, 11, 1), 1, 9.0)


def test_weather_rows_keep_nulls():
    rows = weather_rows(
        "nyc",
        "America/New_York",
        payload(["2026-08-22T12:00"], temperature_2m=[None]),
        datetime.now(UTC),
    )
    assert rows[0][3] is None


def test_year_chunks_contiguous_calendar_years():
    chunks = year_chunks(date(2024, 8, 23), date(2026, 8, 16))
    assert chunks == [
        (date(2024, 8, 23), date(2024, 12, 31)),
        (date(2025, 1, 1), date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 8, 16)),
    ]


# --- raw-feed freshness -----------------------------------------------------


class FakeS3:
    class exceptions:
        class NoSuchKey(Exception):
            pass

    def __init__(self, objects=None, hints=None):
        self.objects = objects or {}  # prefix -> [LastModified, ...]
        self.hints = hints or {}  # key -> version-hint content

    def list_objects_v2(self, Bucket, Prefix):  # noqa: N803 (boto3 casing)
        stamps = self.objects.get(Prefix, [])
        if not stamps:
            return {}
        return {"Contents": [{"Key": f"{Prefix}x.pb", "LastModified": s} for s in stamps]}

    def get_object(self, Bucket, Key):  # noqa: N803 (boto3 casing)
        if Key not in self.hints:
            raise self.exceptions.NoSuchKey()
        return {"Body": io.BytesIO(self.hints[Key].encode())}


NOW = datetime(2026, 8, 22, 14, 5, tzinfo=UTC)


def test_hour_prefixes_cross_day_boundary():
    prefixes = hour_prefixes("nyc", "l", datetime(2026, 8, 22, 0, 5, tzinfo=UTC))
    assert prefixes == ["nyc/l/2026-08-22/00/", "nyc/l/2026-08-21/23/"]


def test_freshness_fresh_and_stale_and_missing():
    s3 = FakeS3(
        objects={
            # l: newest 1 min old (current hour) -> fresh
            "nyc/l/2026-08-22/14/": [NOW - timedelta(minutes=1)],
            # g: only object is 55 min old, in the previous hour -> stale
            "nyc/g/2026-08-22/13/": [NOW - timedelta(minutes=55)],
            # si: nothing anywhere -> stale with age None
        }
    )
    report = evaluate_feed_freshness(s3, "raw", now=NOW, endpoints=["l", "g", "si"])
    assert not report["l"]["stale"] and report["l"]["age_sec"] == 60
    assert report["g"]["stale"] and report["g"]["age_sec"] == 55 * 60
    assert report["si"]["stale"] and report["si"]["age_sec"] is None


def test_freshness_previous_hour_object_counts():
    # at :05 the newest object often sits in the previous hour's prefix
    s3 = FakeS3(objects={"nyc/l/2026-08-22/13/": [NOW - timedelta(minutes=10)]})
    report = evaluate_feed_freshness(s3, "raw", now=NOW, endpoints=["l"])
    assert not report["l"]["stale"]


def test_all_eight_nyc_endpoints_probed():
    seen = []

    class SpyS3(FakeS3):
        def list_objects_v2(self, Bucket, Prefix):  # noqa: N803
            seen.append(Prefix)
            return {"Contents": [{"Key": f"{Prefix}x.pb", "LastModified": NOW}]}

    evaluate_feed_freshness(SpyS3(), "raw", now=NOW)
    probed = {p.split("/")[1] for p in seen}
    assert probed == set(NYC_FEED_ENDPOINTS) and len(NYC_FEED_ENDPOINTS) == 8


# --- retired pollers --------------------------------------------------------


def test_retired_cities_parses_and_tolerates_whitespace():
    assert retired_cities({}) == frozenset()
    assert retired_cities({"TP_POLLING_RETIRED": ""}) == frozenset()
    assert retired_cities({"TP_POLLING_RETIRED": "nyc, boston ,dc"}) == frozenset(
        {"nyc", "boston", "dc"}
    )


def test_retired_cities_drop_out_of_the_freshness_tripwire():
    """A city whose poller was stopped on purpose must not be probed: its raw
    prefix goes stale by design, and the tripwire fires four times an hour."""
    everything = city_feed_endpoints(retired=frozenset())
    assert {"nyc", "zurich", "tokyo"} <= set(everything)

    kept = city_feed_endpoints(
        retired=frozenset({"nyc", "boston", "toronto", "helsinki", "dc", "sf"})
    )
    assert set(kept) == {"zurich", "tokyo"}
    # the cities still polling keep every one of their endpoints
    assert kept["tokyo"] == everything["tokyo"]
    assert kept["zurich"] == everything["zurich"]


def test_retiring_every_city_still_fails_loudly():
    """The tripwire must never probe nothing and pass."""
    with pytest.raises(RuntimeError, match="no live-city configs"):
        city_feed_endpoints(retired=frozenset(NYC_FEED_ENDPOINTS) | frozenset(
            {"nyc", "boston", "toronto", "helsinki", "dc", "sf", "zurich", "tokyo"}
        ))


# --- iceberg refresh --------------------------------------------------------


def test_latest_metadata_path_reads_version_hint():
    s3 = FakeS3(hints={"iceberg/silver/vehicle_positions/metadata/version-hint.text": "7\n"})
    assert (
        latest_metadata_path(s3, "lake", "vehicle_positions")
        == "silver/vehicle_positions/metadata/v7.metadata.json"
    )
    assert latest_metadata_path(s3, "lake", "alerts") is None


def test_iceberg_statements_register_then_refresh():
    create, refresh = iceberg_refresh_statements(
        "vehicle_positions", "silver/vehicle_positions/metadata/v7.metadata.json"
    )
    assert "create iceberg table if not exists TRANSIT.SILVER.VEHICLE_POSITIONS" in create
    assert "metadata_file_path = 'silver/vehicle_positions/metadata/v7.metadata.json'" in create
    assert refresh.startswith("alter iceberg table TRANSIT.SILVER.VEHICLE_POSITIONS refresh")


def test_silver_tables_single_sourced_from_register_script():
    # the repo checkout has the script; the list must come from it (same content
    # as the fallback either way)
    tables = silver_tables()
    assert "stop_time_predictions" in tables
    assert "gtfs_static_stop_times" in tables
    assert len(tables) == len(set(tables))


def test_fallback_list_matches_register_script():
    # silver_tables() silently falls back to FALLBACK_SILVER_TABLES when the
    # register script cannot be loaded; this pins the two lists together so any
    # drift (new silver table added to the script only) fails make test
    import importlib.util
    from pathlib import Path

    from orchestration.transit_dagster.lib import FALLBACK_SILVER_TABLES

    script = Path(__file__).resolve().parents[1] / "scripts" / "snowflake_register_iceberg.py"
    spec = importlib.util.spec_from_file_location("_register", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert list(FALLBACK_SILVER_TABLES) == list(module.TABLES)


class TestKafkaListenerDown:
    def test_empty_bootstrap_is_not_an_error(self):
        from orchestration.transit_dagster.lib import kafka_listener_down

        assert kafka_listener_down("") is None
        assert kafka_listener_down("   ") is None

    def test_malformed_bootstrap_reports(self):
        from orchestration.transit_dagster.lib import kafka_listener_down

        assert "malformed" in kafka_listener_down("no-port-here")

    def test_reachable_listener_returns_none(self):
        import socket

        from orchestration.transit_dagster.lib import kafka_listener_down

        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        try:
            assert kafka_listener_down(f"127.0.0.1:{port}") is None
        finally:
            srv.close()

    def test_dead_listener_reports_reason(self):
        import socket

        from orchestration.transit_dagster.lib import kafka_listener_down

        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        port = srv.getsockname()[1]
        srv.close()  # bound then closed: nothing listens here now
        reason = kafka_listener_down(f"127.0.0.1:{port}", timeout_sec=2)
        assert reason is not None and f"127.0.0.1:{port}" in reason


class TestZombieRunIds:
    """A run whose worker died with its host stays STARTED forever and blocks the
    serialized chain queue — the pipeline then stops with no alert at all
    (2026-09-01). These pin the reaper's decision boundary."""

    def test_fresh_and_long_running_runs_survive(self):
        now = 1_000_000.0
        records = [
            ("fresh", now - 600),  # 10 min
            ("long_but_healthy", now - 2.9 * 3600),  # under run_monitoring's cap
            ("just_past_cap", now - 3.1 * 3600),  # cap passed; still its job
        ]
        assert zombie_run_ids(records, now) == []

    def test_reaps_run_past_the_backstop_margin(self):
        now = 1_000_000.0
        records = [("zombie", now - 6.5 * 3600), ("fresh", now - 60)]
        assert zombie_run_ids(records, now) == ["zombie"]

    def test_missing_start_time_is_never_reaped(self):
        # No start_time yet = too young to judge; guessing risks killing live work.
        now = 1_000_000.0
        assert zombie_run_ids([("unstarted", None)], now) == []

    def test_boundary_is_inclusive_at_the_margin(self):
        now = 1_000_000.0
        exactly = now - REAP_AFTER_SECONDS
        assert zombie_run_ids([("at_edge", exactly)], now) == ["at_edge"]
        assert zombie_run_ids([("just_under", exactly + 1)], now) == []
