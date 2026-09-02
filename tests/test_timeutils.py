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


def test_toronto_uses_its_own_cutover_not_the_noon_rule():
    """[rev 2026-08-25] Regression guard. timeutils used to hard-code 12h while
    production (silver_normalize) used 4h for Toronto, so anything fetched between
    local midnight and noon came out a day early here. Both now read one map."""
    from spark_jobs.timeutils import (
        cutover_hours_for,
        service_date_of,
    )

    TOR = "America/Toronto"
    # 06:00 local on the 22nd: past TTC's ~04:00 roll, so it is the 22nd's service day.
    ts = datetime(2026, 8, 22, 10, 0, tzinfo=UTC)  # 06:00 EDT
    assert service_date_of(ts, TOR, "toronto") == date(2026, 8, 22)
    # the generic noon rule would have called it the 21st — that was the bug
    assert service_date_of(ts, TOR) == date(2026, 8, 21)
    # 03:00 local is before the roll, so it still belongs to the 21st
    ts_early = datetime(2026, 8, 22, 7, 0, tzinfo=UTC)  # 03:00 EDT
    assert service_date_of(ts_early, TOR, "toronto") == date(2026, 8, 21)
    assert cutover_hours_for("toronto") == 4
    assert cutover_hours_for("nyc") == 12


def test_the_spark_jobs_share_one_service_date_implementation():
    """One rule, one implementation.

    [rev 2026-08-25] There were THREE Spark implementations of the service-date fallback
    — bronze_writer, silver_normalize.with_common and silver_normalize.alerts — and two
    of them hardcoded -12h, so Toronto (4h cutover, and it sets start_date on no trips)
    was misdated by both: 73 of 297 live toronto alert rows carried a service_date a day
    early. All three now go through one helper whose constants live in timeutils.
    """
    from pathlib import Path

    from spark_jobs import bronze_writer, silver_normalize, timeutils

    assert bronze_writer.CITY_FALLBACK_CUTOVER_HOURS is timeutils.CITY_FALLBACK_CUTOVER_HOURS
    assert bronze_writer.DEFAULT_FALLBACK_CUTOVER_HOURS is timeutils.DEFAULT_FALLBACK_CUTOVER_HOURS
    # silver reaches the same helper rather than rolling its own
    assert silver_normalize.service_date_expr is bronze_writer.service_date_expr

    # check CODE, not prose — the comments above deliberately quote the old expression
    code = [
        line
        for path in (silver_normalize.__file__, bronze_writer.__file__)
        for line in Path(path).read_text().splitlines()
        if not line.lstrip().startswith("#")
    ]
    offenders = [line.strip() for line in code if "INTERVAL 12 HOURS" in line]
    assert not offenders, f"a hardcoded cutover reappeared: {offenders}"


def test_tokyo_cutover_covers_the_dead_window():
    # ODPT has no start_date, so 100% of tokyo rows take the fallback. Boundary is
    # 03:00 JST (Toei network dead ~01:30-04:30): a 01:00 JST snapshot belongs to
    # the previous service day, a 05:00 JST one to its own.
    from spark_jobs.timeutils import cutover_hours_for, service_date_of

    TOKYO = "Asia/Tokyo"
    late_night = datetime(2026, 8, 31, 16, 0, tzinfo=UTC)  # 01:00 JST Sep 1
    assert service_date_of(late_night, TOKYO, "tokyo") == date(2026, 8, 31)
    first_train = datetime(2026, 8, 31, 20, 0, tzinfo=UTC)  # 05:00 JST Sep 1
    assert service_date_of(first_train, TOKYO, "tokyo") == date(2026, 9, 1)
    assert cutover_hours_for("tokyo") == 3
