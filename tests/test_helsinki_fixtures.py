"""Decode tests over checked-in HSL fixtures (tests/fixtures/helsinki_*.pb). No live calls.

Encodes the verified Helsinki quirks from docs/01-data-dictionary.md §B:
- only two feeds: trip-updates + service-alerts. NO VehiclePositions feed (MQTT HFP is
  stretch-only) — the adapter/poller must tolerate a city with no VP endpoint
- trip_id is EMPTY on every TripUpdate -> trips resolve via (route_id, direction_id,
  start_date, start_time); all four are populated on every trip (trip_uid fallback keys)
- arrival.delay NOT set (dictionary amended 2026-08-23: two independent snapshots showed
  0 delays across ~20k arrivals; the 2026-08-22 note said populated). arrival.time is
  always present, so the canonical COALESCE computes delay vs static schedule instead.
"""

from datetime import UTC, datetime
from pathlib import Path

from ingestion.adapters import gtfs_rt
from ingestion.adapters.base import load_city_config

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def load(feed: str):
    return gtfs_rt.parse_feed((FIXTURE_DIR / f"helsinki_{feed}.pb").read_bytes())


def trip_updates(msg):
    return [e.trip_update for e in msg.entity if e.HasField("trip_update")]


def stus(msg):
    return [stu for tu in trip_updates(msg) for stu in tu.stop_time_update]


def test_feeds_present():
    for feed in ("trip_updates", "alerts"):
        msg = load(feed)
        assert msg.header.gtfs_realtime_version == "2.0"
        assert msg.header.timestamp > 0
        assert len(msg.entity) > 0


def test_no_vehicle_positions_feed_configured():
    cfg = load_city_config("helsinki")
    assert set(cfg.endpoints) == {"trip_updates", "alerts"}
    assert not (FIXTURE_DIR / "helsinki_vehicle_positions.pb").exists()


def test_trip_id_empty_and_fallback_keys_populated():
    trips = trip_updates(load("trip_updates"))
    assert trips
    for tu in trips:
        assert tu.trip.trip_id == ""  # THE Helsinki quirk
        assert tu.trip.route_id
        assert tu.trip.HasField("direction_id")
        assert tu.trip.start_date
        assert tu.trip.start_time


def test_arrival_delay_absent_time_present():
    arrivals = [stu.arrival for stu in stus(load("trip_updates")) if stu.HasField("arrival")]
    assert arrivals
    assert all(a.HasField("time") for a in arrivals)
    assert not any(a.HasField("delay") for a in arrivals), (
        "HSL is setting arrival.delay again — restore rt_delay_source=feed for helsinki "
        "and amend docs/01-data-dictionary.md §B"
    )


def test_adapter_emits_trip_id_none_for_empty_string():
    # Silver's trip_uid COALESCE needs NULL, not '' — the adapter maps empty -> None.
    rec = gtfs_rt.trip_update_record(
        next(e for e in load("trip_updates").entity if e.HasField("trip_update"))
    )
    assert rec["trip_id"] is None
    assert rec["route_id"] and rec["start_time"] and rec["start_date"]
    assert rec["direction_id"] in (0, 1)


def test_tu_fixture_yields_only_trip_updates_envelope():
    # A no-VP city must not emit empty vehicle_positions envelopes.
    envelopes = gtfs_rt.envelopes_for_feed(
        city="helsinki",
        agency="HSL",
        endpoint="trip_updates",
        raw=(FIXTURE_DIR / "helsinki_trip_updates.pb").read_bytes(),
        fetched_at=datetime(2026, 8, 23, 14, 20, 0, tzinfo=UTC),
    )
    assert [e["feed"] for e in envelopes] == ["trip_updates"]


def test_alerts_carry_business_fields():
    recs = [gtfs_rt.alert_record(e) for e in load("alerts").entity if e.HasField("alert")]
    assert recs
    assert all(r["active_periods"] and r["informed_entities"] for r in recs)
    assert all(r["effect"] for r in recs)
