"""Decode tests over checked-in Swiss OTD fixtures (tests/fixtures/zurich_*.pb).
No live calls. The feed is NATIONAL (all CH) — the Zurich allow-list filter happens
in silver, so fixtures deliberately contain non-Zurich operators.

Encodes the verified Zurich quirks (fixtures 2026-08-23):
- two endpoints, one token per API product: gtfs-rt (TU only, no VP/alerts) and
  gtfs-sa (alerts). Both 302 cross-host to a pre-signed URL (handled in fetch_feed)
- arrival.delay IS feed-provided on ~99% of SCHEDULED arrivals — first city of the
  set with rt_delay_source=feed; ADDED (`ojp:...sjyid...`) trips carry time-only
  events, which the canonical COALESCE absorbs
- trip_id always set and equijoins national static 399/399 SCHEDULED+CANCELED = 100%
  (only misses are runtime-created ADDED trips) -> exact-join matcher
- direction_id NEVER set on TUs (static trips join supplies it); start_date/start_time
  always set; CANCELED trips carry zero STUs
- stop_ids are SLOID (`ch:1:sloid:...`) with a handful of foreign UIC ids on
  cross-border runs
- gtfs-sa QUIRK: TripDescriptor field 7 carries a raw string journey id
  ("ch:1:sjyid:...") where the spec has the ModifiedTripSelector MESSAGE -> strict
  protobuf decode FAILS for ~19% of entities; parse_feed's tag remap recovers 100%.
  If strict parse starts succeeding, Swiss fixed their feed — drop the remap then.
- SA alerts: active_period + informed_entity (with agency_id) on every alert;
  cause is meaningful; effect explicit but always UNKNOWN_EFFECT; severity never
  set; header translations are German-first (de, en, it/fr)
"""

from pathlib import Path

import pytest
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2 as rt

from ingestion.adapters import gtfs_rt

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def load(feed: str):
    return gtfs_rt.parse_feed((FIXTURE_DIR / f"zurich_{feed}.pb").read_bytes())


def trip_updates(msg):
    return [e.trip_update for e in msg.entity if e.HasField("trip_update")]


def stus(msg):
    return [stu for tu in trip_updates(msg) for stu in tu.stop_time_update]


def alerts(msg):
    return [e.alert for e in msg.entity if e.HasField("alert")]


def test_tu_feed_is_trip_updates_only():
    msg = load("trip_updates")
    assert msg.header.gtfs_realtime_version == "2.0"
    assert msg.header.timestamp > 0
    assert len(trip_updates(msg)) > 0
    assert not any(e.HasField("vehicle") for e in msg.entity)
    assert not any(e.HasField("alert") for e in msg.entity)


def test_trip_descriptor_no_direction_static_joinable_ids():
    for tu in trip_updates(load("trip_updates")):
        assert tu.trip.trip_id
        assert tu.trip.route_id
        assert not tu.trip.HasField("direction_id"), "Swiss OTD started setting direction_id"
        assert tu.trip.start_date
        assert tu.trip.start_time


def test_arrival_delay_feed_provided():
    # rt_delay_source=feed for Zurich: delay comes from the feed on nearly every
    # arrival (helsinki-style sentinel — if this drops, flip dim_city + re-verify).
    arrivals = [stu.arrival for stu in stus(load("trip_updates")) if stu.HasField("arrival")]
    assert arrivals
    delay_share = sum(1 for a in arrivals if a.HasField("delay")) / len(arrivals)
    assert delay_share > 0.8, f"feed-provided delay share fell to {delay_share:.0%}"
    # every arrival still resolves through the canonical COALESCE
    assert all(a.HasField("delay") or a.HasField("time") for a in arrivals)


def test_added_trips_are_runtime_ojp_ids():
    added = [tu for tu in trip_updates(load("trip_updates")) if tu.trip.schedule_relationship == 1]
    for tu in added:  # may be empty in a quiet snapshot
        assert tu.trip.trip_id.startswith("ojp:"), tu.trip.trip_id


def test_stop_sequence_always_stop_ids_sloid():
    all_stus = stus(load("trip_updates"))
    assert all(stu.HasField("stop_sequence") for stu in all_stus)
    sloid = sum(1 for stu in all_stus if stu.stop_id.startswith("ch:1:sloid:"))
    assert sloid > 0.95 * len(all_stus), "SLOID stop_id format no longer dominant"


def test_sa_strict_parse_fails_tolerant_recovers():
    raw = (FIXTURE_DIR / "zurich_alerts.pb").read_bytes()
    with pytest.raises(DecodeError):
        rt.FeedMessage().ParseFromString(raw)  # Swiss fixed field 7? -> drop the remap
    msg = gtfs_rt.parse_feed(raw)
    assert len(msg.entity) > 100
    assert len(alerts(msg)) == len(msg.entity), "every SA entity is an alert"


def test_sa_alerts_fields():
    msg = load("alerts")
    for alert in alerts(msg):
        assert len(alert.active_period) > 0
        assert len(alert.informed_entity) > 0
        assert alert.header_text.translation
    causes = {a.Cause.Name(a.cause) for a in alerts(msg) if a.HasField("cause")}
    assert len(causes) >= 2, f"cause stopped being informative: {causes}"
    assert not any(a.HasField("severity_level") for a in alerts(msg))
    # German-first translations: staging's first-translation pick yields 'de'
    langs = {a.header_text.translation[0].language for a in alerts(msg)}
    assert langs == {"de"}, langs
    recs = [gtfs_rt.alert_record(e) for e in msg.entity if e.HasField("alert")]
    assert all(any(ie["agency_id"] for ie in r["informed_entities"]) for r in recs)


# --- remap correctness on synthetic bytes (covers the TU/VP trip paths the fixture
# --- doesn't exercise, and proves untouched fields survive byte-identically) ---------


def _ld(field_number: int, payload: bytes) -> bytes:
    """Length-delimited protobuf field (wire type 2), single-byte tag+length."""
    assert len(payload) < 128
    return bytes([(field_number << 3) | 2, len(payload)]) + payload


def _synthetic_with_field7(entity_kind: str) -> bytes:
    bad_trip = _ld(1, b"trip-1") + _ld(7, b"ch:1:sjyid:junk")  # field 7 = raw string
    if entity_kind == "trip_update":
        body = _ld(3, _ld(1, bad_trip))  # entity.trip_update.trip
    elif entity_kind == "vehicle":
        body = _ld(4, _ld(1, bad_trip))  # entity.vehicle.trip
    else:
        body = _ld(5, _ld(5, _ld(4, bad_trip)))  # entity.alert.informed_entity.trip
    header = _ld(1, _ld(1, b"2.0"))
    entity = _ld(2, _ld(1, b"e1") + body)
    return header + entity


@pytest.mark.parametrize("kind", ["trip_update", "vehicle", "alert"])
def test_remap_recovers_all_trip_descriptor_positions(kind):
    raw = _synthetic_with_field7(kind)
    with pytest.raises(DecodeError):
        rt.FeedMessage().ParseFromString(raw)
    msg = gtfs_rt.parse_feed(raw)
    assert len(msg.entity) == 1
    if kind == "trip_update":
        assert msg.entity[0].trip_update.trip.trip_id == "trip-1"
    elif kind == "vehicle":
        assert msg.entity[0].vehicle.trip.trip_id == "trip-1"
    else:
        assert msg.entity[0].alert.informed_entity[0].trip.trip_id == "trip-1"


def test_remap_leaves_valid_messages_untouched():
    # A spec-valid feed must never take the remap path: strict parse succeeds first.
    raw = (FIXTURE_DIR / "zurich_trip_updates.pb").read_bytes()
    strict = rt.FeedMessage()
    strict.ParseFromString(raw)  # no DecodeError -> parse_feed returns identical content
    assert gtfs_rt.parse_feed(raw) == strict
