from datetime import UTC, date, datetime

import pytest

from spark_jobs.timeutils import (
    gtfs_time_to_seconds,
    scheduled_ts_utc,
    service_date_of,
)

NYC = "America/New_York"


@pytest.mark.parametrize(
    ("raw", "seconds"),
    [
        ("00:00:00", 0),
        ("08:15:30", 29730),
        ("24:00:00", 86400),
        ("25:30:00", 91800),
        ("7:05:00", 25500),
    ],
)
def test_gtfs_time_to_seconds(raw, seconds):
    assert gtfs_time_to_seconds(raw) == seconds


def test_service_date_noon_rule():
    # 01:30 local on Aug 23 belongs to the Aug 22 service day
    ts = datetime(2026, 8, 23, 5, 30, tzinfo=UTC)  # 01:30 EDT
    assert service_date_of(ts, NYC) == date(2026, 8, 22)
    # 13:00 local same day stays on its own date
    ts2 = datetime(2026, 8, 22, 17, 0, tzinfo=UTC)
    assert service_date_of(ts2, NYC) == date(2026, 8, 22)


def test_scheduled_ts_utc_plain_day():
    # EDT (UTC-4): 08:15:30 local -> 12:15:30Z
    ts = scheduled_ts_utc(date(2026, 8, 22), gtfs_time_to_seconds("08:15:30"), NYC)
    assert ts == datetime(2026, 8, 22, 12, 15, 30, tzinfo=UTC)


def test_scheduled_ts_utc_past_midnight():
    # 25:30 on service day Aug 22 = 01:30 local Aug 23 = 05:30Z
    ts = scheduled_ts_utc(date(2026, 8, 22), gtfs_time_to_seconds("25:30:00"), NYC)
    assert ts == datetime(2026, 8, 23, 5, 30, tzinfo=UTC)


def test_scheduled_ts_utc_spring_forward():
    # US DST starts 2026-03-08 02:00 local. Anchor for the 03-07 service day is
    # 03-07 12:00 EST = 17:00Z minus 12h = 05:00Z. 25:30 after the anchor is
    # 03-08 06:30Z, i.e. 01:30 EST — still pre-jump.
    ts = scheduled_ts_utc(date(2026, 3, 7), gtfs_time_to_seconds("25:30:00"), NYC)
    assert ts == datetime(2026, 3, 8, 6, 30, tzinfo=UTC)
    # 27:30 lands after the jump: 08:30Z = 04:30 EDT (the 03:00-04:00 wall hour never exists)
    ts2 = scheduled_ts_utc(date(2026, 3, 7), gtfs_time_to_seconds("27:30:00"), NYC)
    assert ts2 == datetime(2026, 3, 8, 8, 30, tzinfo=UTC)


def test_round_trip_consistency_from_noon():
    # for times >= 12:00 a scheduled instant maps back to its service date
    for gtfs_time in ("12:00:00", "23:59:00", "26:15:00", "35:59:00"):
        ts = scheduled_ts_utc(date(2026, 8, 22), gtfs_time_to_seconds(gtfs_time), NYC)
        assert service_date_of(ts, NYC) == date(2026, 8, 22)


def test_early_morning_maps_to_previous_date():
    # The noon rule is a fallback for observations lacking schedule context: a 05:00
    # wall-clock instant buckets to the previous service date. Trips scheduled at
    # 05:00 on a calendar day are assigned via trip.start_date / the calendar, never
    # via this fallback — both sides of the pipeline agree on that precedence.
    ts = scheduled_ts_utc(date(2026, 8, 22), gtfs_time_to_seconds("05:00:00"), NYC)
    assert service_date_of(ts, NYC) == date(2026, 8, 21)
