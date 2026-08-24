"""Decode tests over checked-in WMATA fixtures (tests/fixtures/dc_*.pb). No live calls.

Encodes the verified DC quirks (fixtures 2026-08-23):
- six per-type feeds: {rail,bus} x {TripUpdates, VehiclePositions, Alerts}
- trip descriptor complete on every TU (trip_id/route_id/direction_id/start_date/
  start_time) and trip_id equijoins static GTFS: rail 140/140 SCHEDULED = 100%
  (only misses were 2 UNSCHEDULED 'NR' shuttle runs), bus 3635/3635 = 100%
  -> exact-join matcher (boston branch), start_date present -> default service-date rule
- arrival.delay NOT set (0/2,914 rail + 0/17,331 bus) -> delay computed vs static
  (rt_delay_source=computed); arrival.time always present for the canonical COALESCE
- occupancy_status is populated but NOT usable as a crowding signal: rail all EMPTY,
  bus 96% NO_DATA_AVAILABLE (fixture 2026-08-23)
- bus STUs carry heavy SKIPPED marking (7,551/25,265 = 30% in fixture) -> staging must
  handle SKIPPED before OTP denominators
- alerts carry explicit effect/cause/active_period/informed routes; severity_level
  is NEVER set
"""

from pathlib import Path

from ingestion.adapters import gtfs_rt

FIXTURE_DIR = Path(__file__).parent / "fixtures"

MODES = ("rail", "bus")


def load(feed: str):
    return gtfs_rt.parse_feed((FIXTURE_DIR / f"dc_{feed}.pb").read_bytes())


def trip_updates(msg):
    return [e.trip_update for e in msg.entity if e.HasField("trip_update")]


def stus(msg):
    return [stu for tu in trip_updates(msg) for stu in tu.stop_time_update]


def vehicles(msg):
    return [e.vehicle for e in msg.entity if e.HasField("vehicle")]


def alerts(msg):
    return [e.alert for e in msg.entity if e.HasField("alert")]


def test_six_per_mode_feeds_present():
    for mode in MODES:
        for feed, extractor in [
            ("trip_updates", trip_updates),
            ("vehicle_positions", vehicles),
            ("alerts", alerts),
        ]:
            msg = load(f"{mode}_{feed}")
            assert msg.header.gtfs_realtime_version == "2.0"
            assert msg.header.timestamp > 0
            assert len(extractor(msg)) > 0


def test_trip_descriptor_complete_both_modes():
    # Static-joinable ids + start_date on every trip: exact-join matcher and the
    # default service-date rule both hold. Failing here is the early warning.
    for mode in MODES:
        for tu in trip_updates(load(f"{mode}_trip_updates")):
            assert tu.trip.trip_id
            assert tu.trip.route_id
            assert tu.trip.HasField("direction_id")
            assert tu.trip.start_date
            assert tu.trip.start_time
            assert tu.trip.HasField("schedule_relationship")


def test_arrival_delay_absent_time_present():
    for mode in MODES:
        arrivals = [
            stu.arrival for stu in stus(load(f"{mode}_trip_updates")) if stu.HasField("arrival")
        ]
        assert arrivals
        assert all(a.HasField("time") for a in arrivals)
        assert not any(a.HasField("delay") for a in arrivals), (
            f"WMATA {mode} started setting arrival.delay"
        )


def test_stop_sequence_and_stop_id_always_set():
    for mode in MODES:
        for stu in stus(load(f"{mode}_trip_updates")):
            assert stu.HasField("stop_sequence")
            assert stu.stop_id


def test_stu_schedule_relationship_vocabulary():
    # Bus marks a large SKIPPED share (30% in fixture) — vocabulary check only, the
    # share itself drifts with detours. Staging must not treat SKIPPED as a prediction.
    for mode in MODES:
        names = {
            stu.ScheduleRelationship.Name(stu.schedule_relationship)
            for stu in stus(load(f"{mode}_trip_updates"))
        }
        assert names <= {"SCHEDULED", "SKIPPED", "NO_DATA"}, names


def test_vehicle_positions_have_coords_and_ids():
    for mode in MODES:
        vps = vehicles(load(f"{mode}_vehicle_positions"))
        assert vps
        for vp in vps:
            assert vp.HasField("position")
            assert vp.vehicle.id


def test_occupancy_present_but_flagged_unusable():
    # Field coverage is high in both modes, but observed values carry no crowding
    # information (rail: all EMPTY; bus: NO_DATA_AVAILABLE). dim_city must not
    # advertise DC crowding until real values appear.
    for mode in MODES:
        vps = vehicles(load(f"{mode}_vehicle_positions"))
        with_occ = [vp for vp in vps if vp.HasField("occupancy_status")]
        assert len(with_occ) > 0.5 * len(vps)
    bus = vehicles(load("bus_vehicle_positions"))
    no_data = sum(
        1
        for vp in bus
        if vp.HasField("occupancy_status")
        and vp.OccupancyStatus.Name(vp.occupancy_status) == "NO_DATA_AVAILABLE"
    )
    assert no_data > 0.8 * sum(1 for vp in bus if vp.HasField("occupancy_status")), (
        "WMATA bus occupancy became informative — revisit the crowding flag for DC"
    )


def test_alerts_carry_business_fields_severity_absent():
    for mode in MODES:
        recs = [
            gtfs_rt.alert_record(e) for e in load(f"{mode}_alerts").entity if e.HasField("alert")
        ]
        assert recs
        for rec in recs:
            assert rec["active_periods"]
            assert rec["informed_entities"]
            assert rec["header_text"]
        assert all(r["effect"] != "UNKNOWN_EFFECT" or r["cause"] for r in recs)
    # severity_level never set (fixture 2026-08-23) — flip dim expectations if this fails
    for mode in MODES:
        assert not any(a.HasField("severity_level") for a in alerts(load(f"{mode}_alerts")))
