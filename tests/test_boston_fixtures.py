"""Decode tests over checked-in MBTA fixtures (tests/fixtures/boston_*.pb). No live calls.

Encodes the verified Boston quirks from docs/01-data-dictionary.md §B:
- per-type feeds (TripUpdates / VehiclePositions / Alerts), all GTFS-RT 2.0
- occupancy_status populated on many vehicles (crowding metric input)
- arrival.delay is NOT set (fixture 2026-08-23) -> delay computed vs static schedule;
  arrival.time is always present so the canonical COALESCE picks the computed path
- cleanest feed of the set: trip_id, direction_id, stop_sequence set on everything
"""

from pathlib import Path

from ingestion.adapters import gtfs_rt

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def load(feed: str):
    return gtfs_rt.parse_feed((FIXTURE_DIR / f"boston_{feed}.pb").read_bytes())


def trip_updates(msg):
    return [e.trip_update for e in msg.entity if e.HasField("trip_update")]


def stus(msg):
    return [stu for tu in trip_updates(msg) for stu in tu.stop_time_update]


def vehicles(msg):
    return [e.vehicle for e in msg.entity if e.HasField("vehicle")]


def alerts(msg):
    return [e.alert for e in msg.entity if e.HasField("alert")]


def test_per_type_feeds_present():
    for feed, extractor in [
        ("trip_updates", trip_updates),
        ("vehicle_positions", vehicles),
        ("alerts", alerts),
    ]:
        msg = load(feed)
        assert msg.header.gtfs_realtime_version == "2.0"
        assert msg.header.timestamp > 0
        assert len(extractor(msg)) > 0


def test_trip_descriptor_complete():
    # Unlike NYC: real static trip_ids and direction_id on every trip -> exact-join matcher.
    # start_date shields silver's service-date fallback (see the TTC cutover in
    # silver_normalize) — if MBTA ever drops it, this failing is the early warning.
    for tu in trip_updates(load("trip_updates")):
        assert tu.trip.trip_id
        assert tu.trip.route_id
        assert tu.trip.HasField("direction_id")
        assert tu.trip.start_date
        assert tu.trip.start_time


def test_arrival_delay_absent_time_present():
    arrivals = [stu.arrival for stu in stus(load("trip_updates")) if stu.HasField("arrival")]
    assert arrivals
    assert all(a.HasField("time") for a in arrivals)
    assert not any(a.HasField("delay") for a in arrivals), "MBTA started setting arrival.delay"


def test_stop_sequence_always_set():
    assert all(stu.HasField("stop_sequence") for stu in stus(load("trip_updates")))


def test_occupancy_status_on_many_vehicles():
    vps = vehicles(load("vehicle_positions"))
    with_occ = [vp for vp in vps if vp.HasField("occupancy_status")]
    assert len(with_occ) > 0.25 * len(vps), "occupancy_status expected on many MBTA vehicles"
    # and the adapter surfaces it as the enum name
    rec = gtfs_rt.vehicle_position_record(
        next(e for e in load("vehicle_positions").entity if e.vehicle.HasField("occupancy_status"))
    )
    assert rec["occupancy_status"] is not None


def test_vehicle_positions_have_coords():
    # Surface + rail vehicles report lat/lon (NYC subway is the outlier without them).
    for vp in vehicles(load("vehicle_positions")):
        assert vp.HasField("position")


def test_alerts_carry_business_fields():
    # fct_alerts_daily needs effect/severity/active_period/informed routes.
    for alert in alerts(load("alerts")):
        assert len(alert.active_period) > 0
        assert len(alert.informed_entity) > 0
    recs = [gtfs_rt.alert_record(e) for e in load("alerts").entity if e.HasField("alert")]
    assert all(r["effect"] and r["severity_level"] for r in recs)
