"""Decode tests over checked-in 511.org regional fixtures (tests/fixtures/sf_*.pb).
No live calls — the key is rate-limited to 60 req/hr, so re-record deliberately.

Encodes the verified SF Bay quirks (fixtures 2026-08-23):
- ONE feed multiplexes ~23 operators (`agency=RG`): every trip_id AND route_id is
  namespaced `<AGENCY>:<id>` (SF:, AC:, SC:, BA:, CT:, ...) — the prefix is the
  agency_id and must survive into silver
- header says gtfs_realtime_version "1.0" (like NYC) while the payload is 2.0-shaped
- arrival.delay presence is PER OPERATOR, all-or-nothing (SF/SM/BA/GG set it; AC/SC/MA
  et al. don't -> 59% of arrivals overall in fixture). arrival.time is ALWAYS set, so
  the canonical COALESCE resolves both populations (rt_delay_source=mixed)
- trip.start_date is missing for a few operators (BA/ST/3D/CT in fixture) -> those
  records take the default -12h service-date fallback; everyone else sets it
- CANCELED trips (98/2,494) and SKIPPED STUs observed; direction_id always set
- occupancy_status is REAL here (5 distinct values incl. STANDING_ROOM_ONLY/FULL on
  79% of VPs) -> crowding metric usable for SF
- alerts: informed_entity always present (with agency_id); effect/severity/cause
  explicit on only a subset — stage as nullable, don't require them
"""

from pathlib import Path

from ingestion.adapters import gtfs_rt

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def load(feed: str):
    return gtfs_rt.parse_feed((FIXTURE_DIR / f"sf_{feed}.pb").read_bytes())


def trip_updates(msg):
    return [e.trip_update for e in msg.entity if e.HasField("trip_update")]


def stus(msg):
    return [stu for tu in trip_updates(msg) for stu in tu.stop_time_update]


def vehicles(msg):
    return [e.vehicle for e in msg.entity if e.HasField("vehicle")]


def alerts(msg):
    return [e.alert for e in msg.entity if e.HasField("alert")]


def test_per_type_feeds_present_header_v1():
    for feed, extractor in [
        ("trip_updates", trip_updates),
        ("vehicle_positions", vehicles),
        ("alerts", alerts),
    ]:
        msg = load(feed)
        # 511 reports "1.0" (fixture 2026-08-23). If this flips to "2.0" the feed
        # changed underneath us — re-verify the quirks in this file.
        assert msg.header.gtfs_realtime_version == "1.0"
        assert msg.header.timestamp > 0
        assert len(extractor(msg)) > 0


def test_all_ids_carry_agency_prefix():
    tus = trip_updates(load("trip_updates"))
    assert tus
    for tu in tus:
        assert ":" in tu.trip.trip_id, f"unprefixed trip_id {tu.trip.trip_id!r}"
        assert ":" in tu.trip.route_id, f"unprefixed route_id {tu.trip.route_id!r}"
    prefixes = {tu.trip.trip_id.split(":")[0] for tu in tus}
    assert len(prefixes) >= 10, f"expected many operators in RG feed, got {prefixes}"


def test_direction_always_set_start_date_mostly():
    tus = trip_updates(load("trip_updates"))
    assert all(tu.trip.HasField("direction_id") for tu in tus)
    with_start_date = sum(1 for tu in tus if tu.trip.start_date)
    assert with_start_date > 0.8 * len(tus), "start_date coverage collapsed"


def test_arrival_delay_mixed_by_operator_time_always():
    arrivals = [stu.arrival for stu in stus(load("trip_updates")) if stu.HasField("arrival")]
    assert arrivals
    assert all(a.HasField("time") for a in arrivals), "arrival.time is the COALESCE anchor"
    delay_share = sum(1 for a in arrivals if a.HasField("delay")) / len(arrivals)
    assert 0.2 < delay_share < 0.95, (
        f"delay share {delay_share:.0%}: per-operator mix changed — recheck rt_delay_source"
    )


def test_trip_schedule_relationship_vocabulary():
    names = {
        tu.trip.ScheduleRelationship.Name(tu.trip.schedule_relationship)
        for tu in trip_updates(load("trip_updates"))
    }
    assert names <= {"SCHEDULED", "CANCELED", "ADDED", "NEW", "DUPLICATED"}, names


def test_vehicle_positions_have_coords():
    vps = vehicles(load("vehicle_positions"))
    assert vps
    assert all(vp.HasField("position") for vp in vps)


def test_occupancy_is_informative():
    vps = vehicles(load("vehicle_positions"))
    with_occ = [
        vp.OccupancyStatus.Name(vp.occupancy_status)
        for vp in vps
        if vp.HasField("occupancy_status")
    ]
    assert len(with_occ) > 0.4 * len(vps)
    informative = {v for v in with_occ if v != "NO_DATA_AVAILABLE"}
    assert len(informative) >= 3, f"SF occupancy variety collapsed: {informative}"


def test_alerts_scope_and_partial_business_fields():
    recs = [gtfs_rt.alert_record(e) for e in load("alerts").entity if e.HasField("alert")]
    assert recs
    assert all(r["informed_entities"] for r in recs)
    assert any(ie["agency_id"] for r in recs for ie in r["informed_entities"])
    explicit_effect = [a for a in alerts(load("alerts")) if a.HasField("effect")]
    explicit_severity = [a for a in alerts(load("alerts")) if a.HasField("severity_level")]
    assert explicit_effect and explicit_severity  # present on a subset
    assert len(explicit_severity) < len(recs)  # ...but NOT required on all: keep nullable
