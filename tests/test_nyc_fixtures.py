"""Decode tests over checked-in NYC fixtures (tests/fixtures/nyc_*.pb). No live calls.

Encodes the verified NYC quirks from docs/01-data-dictionary.md §B:
- trip_ids are origin-time-encoded
- arrival.time always present; arrival.delay absent — except the L feed (CBTC),
  which sets both arrival.delay and stop_sequence
"""

import re
from pathlib import Path

import pytest

from ingestion.adapters import gtfs_rt

FIXTURE_DIR = Path(__file__).parent / "fixtures"
FEEDS = ["base", "ace", "bdfm", "g", "jz", "nqrw", "l", "si"]
TRIP_ID_RE = re.compile(r"^\d{6}_[0-9A-Z]+\.\.?[NS]")


def load(feed: str):
    return gtfs_rt.parse_feed((FIXTURE_DIR / f"nyc_{feed}.pb").read_bytes())


@pytest.fixture(scope="module", params=FEEDS)
def feed_name(request):
    return request.param


def trip_updates(msg):
    return [e.trip_update for e in msg.entity if e.HasField("trip_update")]


def stus(msg):
    return [stu for tu in trip_updates(msg) for stu in tu.stop_time_update]


def test_entities_present(feed_name):
    msg = load(feed_name)
    assert len(msg.entity) > 0
    assert len(trip_updates(msg)) > 0
    assert msg.header.gtfs_realtime_version in {"1.0", "2.0"}  # MTA reports 1.0
    assert msg.header.timestamp > 0


def test_trip_id_origin_time_format(feed_name):
    for tu in trip_updates(load(feed_name)):
        trip_id = tu.trip.trip_id
        assert TRIP_ID_RE.match(trip_id), f"unexpected trip_id format: {trip_id!r}"
        origin_minutes = int(trip_id[:6]) / 100  # centiminutes after local midnight
        assert 0 <= origin_minutes < 1800  # GTFS times may exceed 24h


def test_arrival_time_present(feed_name):
    arrivals = [stu.arrival for stu in stus(load(feed_name)) if stu.HasField("arrival")]
    assert arrivals, "no arrivals in fixture"
    assert all(a.HasField("time") for a in arrivals)


def test_arrival_delay_absent_except_l(feed_name):
    msg = load(feed_name)
    with_delay = sum(
        1 for stu in stus(msg) if stu.HasField("arrival") and stu.arrival.HasField("delay")
    )
    if feed_name == "l":
        assert with_delay > 0, "L feed is expected to provide arrival.delay"
    else:
        assert with_delay == 0, f"{feed_name}: arrival.delay unexpectedly present"


def test_stop_sequence_absent_except_l(feed_name):
    msg = load(feed_name)
    with_seq = sum(1 for stu in stus(msg) if stu.HasField("stop_sequence"))
    if feed_name == "l":
        assert with_seq > 0
    else:
        assert with_seq == 0, f"{feed_name}: stop_sequence unexpectedly present"


def test_vehicle_positions_have_no_coords():
    for feed in FEEDS:
        for e in load(feed).entity:
            if e.HasField("vehicle"):
                assert not e.vehicle.HasField("position")
