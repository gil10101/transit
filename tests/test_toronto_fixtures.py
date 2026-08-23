"""Decode tests over checked-in TTC fixtures (tests/fixtures/toronto_*.pb). No live calls.

Encodes the verified Toronto quirks from docs/01-data-dictionary.md §B:
- surface modes only (bus+streetcar); subway is alerts-only
- ~3% of trips carry NEGATIVE trip_ids = unscheduled runs. Fixture 2026-08-23: the feed
  marks every one of them schedule_relationship=NEW (the spec's successor to deprecated
  ADDED) — staging must treat NEW as ADDED (service volume yes, OTP no)
- trip descriptor is trip_id+route_id ONLY: direction_id/start_date/start_time never set
  -> direction comes from the static trips join, trip_uid keys off trip_id
- arrival.delay is NOT set -> delay computed vs static schedule
- occupancy_status populated on nearly all vehicles
"""

from pathlib import Path

from ingestion.adapters import gtfs_rt

FIXTURE_DIR = Path(__file__).parent / "fixtures"

# unscheduled-run share of trips; dictionary says ~3%, keep a loose band for re-records
ADDED_SHARE_BAND = (0.001, 0.10)


def load(feed: str):
    return gtfs_rt.parse_feed((FIXTURE_DIR / f"toronto_{feed}.pb").read_bytes())


def trip_updates(msg):
    return [e.trip_update for e in msg.entity if e.HasField("trip_update")]


def stus(msg):
    return [stu for tu in trip_updates(msg) for stu in tu.stop_time_update]


def vehicles(msg):
    return [e.vehicle for e in msg.entity if e.HasField("vehicle")]


def negative_trips(msg):
    return [tu for tu in trip_updates(msg) if tu.trip.trip_id.startswith("-")]


def test_per_type_feeds_present():
    for feed, extractor in [
        ("trip_updates", trip_updates),
        ("vehicle_positions", vehicles),
        ("alerts", lambda m: [e.alert for e in m.entity if e.HasField("alert")]),
    ]:
        msg = load(feed)
        assert msg.header.gtfs_realtime_version == "2.0"
        assert msg.header.timestamp > 0
        assert len(extractor(msg)) > 0


def test_negative_trip_ids_present_at_expected_share():
    msg = load("trip_updates")
    trips, neg = trip_updates(msg), negative_trips(msg)
    share = len(neg) / len(trips)
    assert ADDED_SHARE_BAND[0] <= share <= ADDED_SHARE_BAND[1], f"unscheduled share {share:.1%}"


def test_negative_trips_marked_new():
    # Feed-verified: negative-id trips are explicitly flagged, as NEW (not ADDED, not unset).
    # If a re-record flips this to ADDED that is a spec-relevant change — update staging too.
    neg = negative_trips(load("trip_updates"))
    assert neg
    names = {tu.trip.ScheduleRelationship.Name(tu.trip.schedule_relationship) for tu in neg}
    assert names == {"NEW"}, f"unexpected schedule_relationship on negative trips: {names}"
    assert all(tu.trip.HasField("schedule_relationship") for tu in neg)


def test_trip_descriptor_is_id_and_route_only():
    for tu in trip_updates(load("trip_updates")):
        assert tu.trip.trip_id
        assert tu.trip.route_id
        assert not tu.trip.HasField("direction_id")
        assert not tu.trip.start_date
        assert not tu.trip.start_time


def test_arrival_delay_absent_time_present():
    arrivals = [stu.arrival for stu in stus(load("trip_updates")) if stu.HasField("arrival")]
    assert arrivals
    assert all(a.HasField("time") for a in arrivals)
    assert not any(a.HasField("delay") for a in arrivals), "TTC started setting arrival.delay"


def test_occupancy_status_on_most_vehicles():
    vps = vehicles(load("vehicle_positions"))
    with_occ = sum(1 for vp in vps if vp.HasField("occupancy_status"))
    assert with_occ > 0.5 * len(vps)


def test_vehicle_positions_have_coords():
    for vp in vehicles(load("vehicle_positions")):
        assert vp.HasField("position")


def test_alerts_scope_to_informed_entities():
    # TTC alerts carry informed_entity but no active_period/severity (fixture 2026-08-23).
    recs = [gtfs_rt.alert_record(e) for e in load("alerts").entity if e.HasField("alert")]
    assert recs
    assert all(r["informed_entities"] for r in recs)
