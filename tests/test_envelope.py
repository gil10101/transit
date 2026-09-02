"""Adapter unit tests on a synthetic FeedMessage — no fixtures, no network."""

from datetime import UTC, datetime

from google.transit import gtfs_realtime_pb2 as rt

from ingestion.adapters import gtfs_rt
from ingestion.adapters.base import CONFIG_DIR, TOPICS, RawArchiver, load_city_config


def synthetic_feed() -> bytes:
    msg = rt.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = 1_755_000_000

    ent = msg.entity.add()
    ent.id = "1"
    tu = ent.trip_update
    tu.trip.trip_id = "070950_A..S58R"
    tu.trip.route_id = "A"
    tu.trip.start_date = "20260822"
    stu = tu.stop_time_update.add()
    stu.stop_id = "A15S"
    stu.arrival.time = 1_755_000_100

    ent2 = msg.entity.add()
    ent2.id = "2"
    vp = ent2.vehicle
    vp.trip.trip_id = "070950_A..S58R"
    vp.current_status = rt.VehiclePosition.STOPPED_AT
    vp.stop_id = "A15S"
    vp.timestamp = 1_755_000_090
    return msg.SerializeToString()


def test_split_and_envelopes():
    envelopes = gtfs_rt.envelopes_for_feed(
        city="nyc",
        agency="MTA-NYCT",
        endpoint="ace",
        raw=synthetic_feed(),
        fetched_at=datetime(2026, 8, 22, 12, 0, 0, tzinfo=UTC),
    )
    by_feed = {e["feed"]: e for e in envelopes}
    assert set(by_feed) == {"trip_updates", "vehicle_positions"}  # no alerts -> no envelope

    tu = by_feed["trip_updates"]
    assert tu["city"] == "nyc"
    assert tu["schema_version"] == 2
    assert tu["source_format"] == "gtfs_rt"
    assert tu["fetched_at"] == "2026-08-22T12:00:00Z"
    assert tu["feed_ts"] == 1_755_000_000
    rec = tu["payload"][0]
    assert rec["trip_id"] == "070950_A..S58R"
    assert rec["stop_time_updates"][0]["arrival"]["time"] == 1_755_000_100
    assert rec["stop_time_updates"][0]["arrival"]["delay"] is None  # absent field -> None

    vp = by_feed["vehicle_positions"]["payload"][0]
    assert vp["current_status"] == "STOPPED_AT"
    assert vp["lat"] is None  # no position on NYC subway VPs


def test_nyc_config_loads():
    cfg = load_city_config("nyc")
    assert cfg.city == "nyc"
    assert len(cfg.endpoints) == 8
    assert cfg.effective_poll_seconds >= 30
    assert all(url.startswith("https://api-endpoint.mta.info/") for url in cfg.endpoints.values())
    assert set(TOPICS) == {"trip_updates", "vehicle_positions", "alerts", "odpt_trains"}


def test_adapter_registry_and_per_endpoint_override():
    from ingestion.adapters import ADAPTERS

    assert ADAPTERS["gtfs_rt"] is gtfs_rt
    assert gtfs_rt.EXTENSION == "pb"
    # every existing city resolves every endpoint to a registered adapter
    for path in sorted(CONFIG_DIR.glob("*.yaml")):
        cfg = load_city_config(path.stem)
        for name in cfg.endpoints:
            assert cfg.adapter_for(name) in ADAPTERS, f"{path.stem}:{name}"
    # per-endpoint override falls back to city-level adapter
    cfg = load_city_config("nyc")
    assert cfg.adapter_for("ace") == "gtfs_rt"
    cfg.endpoint_adapter["ace"] = "other"
    assert cfg.adapter_for("ace") == "other"
    assert cfg.adapter_for("bdfm") == "gtfs_rt"


def test_raw_archiver_extension(monkeypatch):
    monkeypatch.setenv("MINIO_ENDPOINT", "http://localhost:1")  # never contacted

    class _Client:
        def put_object(self, Bucket, Key, Body):  # noqa: N803 — boto3 kwargs
            self.key = Key

    archiver = RawArchiver()
    archiver.client = _Client()
    at = datetime(2026, 9, 1, 3, 4, 5, tzinfo=UTC)
    assert archiver.archive("nyc", "ace", at, b"x").endswith("20260901T030405Z.pb")
    assert archiver.archive("tokyo", "trains", at, b"x", ext="json").endswith(
        "20260901T030405Z.json"
    )
