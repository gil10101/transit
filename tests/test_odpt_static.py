"""Pure row-builder tests for spark_jobs.odpt_static_parse over truncated dump
fixtures (tests/fixtures/tokyo_odpt_*.json, cut from the live 2026-09-01 dumps).
No Spark session, no network.

The load-bearing behavior is time reconstruction: ODPT timetable times are "HH:MM"
WRAPPED at midnight (00:10 follows 23:59 — never "24:10" like GTFS), so
train_timetable_rows must (a) add 24h every time the clock runs backwards inside
one timetable and (b) shift a whole timetable by 24h when its FIRST time is before
the tokyo 03:00 service-day cutover (those runs belong to the previous service day).
Getting either wrong recreates the sf/toronto phantom-day bug class.
"""

import json
from pathlib import Path

from spark_jobs.odpt_static_parse import (
    OPERATORS,
    _ours,
    calendar_rows,
    railway_rows,
    station_rows,
    train_timetable_rows,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def load(name: str) -> list[dict]:
    return json.loads((FIXTURE_DIR / f"tokyo_odpt_{name}.json").read_text())


def test_station_rows_carry_join_keys():
    # stationCode is the verified URN<->GTFS join key: it equals GTFS stop_code
    # 149/149 (Toei) and 185/185 (Metro) on the 2026-09-01 zips (docs/01 §C.4)
    for obj in load("station"):
        (row,) = station_rows(obj)
        assert row["station_urn"].startswith("odpt.Station:")
        assert row["station_code"]
        assert row["title_ja"]
        assert -90 < row["lat"] < 90 and 120 < row["lon"] < 150


def test_railway_rows_explode_station_order():
    objs = [o for o in load("railway") if o["owl:sameAs"] == "odpt.Railway:Toei.Asakusa"]
    rows = railway_rows(objs[0])
    assert len(rows) >= 15  # Asakusa line has 20 stations
    indexes = [r["station_index"] for r in rows]
    assert indexes == sorted(indexes) and len(set(indexes)) == len(indexes)
    assert all(r["station_urn"].startswith("odpt.Station:Toei.Asakusa.") for r in rows)
    assert rows[0]["ascending_direction"] and rows[0]["descending_direction"]


def test_operator_filter_drops_foreign_railways():
    objs = load("railway")
    foreign = [o for o in objs if not _ours(o)]
    assert foreign, "fixture must carry one foreign-operator object to prove the filter"
    kept = [o for o in objs if _ours(o)]
    assert {o["odpt:operator"] for o in kept} <= set(OPERATORS)


def test_calendar_without_operator_is_kept():
    for obj in load("calendar"):
        assert "odpt:operator" not in obj
        assert _ours(obj)
        (row,) = calendar_rows(obj)
        assert row["calendar_urn"].startswith("odpt.Calendar:")


def _by_urn(urn: str) -> dict:
    return next(o for o in load("train_timetable") if o["owl:sameAs"] == urn)


def test_timetable_plain_day_is_monotonic_with_no_offset():
    rows = train_timetable_rows(_by_urn("odpt.TrainTimetable:Toei.Asakusa.726T.Weekday"))
    assert [r["stop_sequence"] for r in rows] == list(range(1, len(rows) + 1))
    times = [r["departure_seconds"] or r["arrival_seconds"] for r in rows]
    assert times == sorted(times)
    assert all(t < 86400 for t in times if t is not None)  # never crossed midnight


def test_timetable_crossing_midnight_gains_24h():
    rows = train_timetable_rows(_by_urn("odpt.TrainTimetable:Toei.Asakusa.2208T.Weekday"))
    times = [t for r in rows for t in (r["arrival_seconds"], r["departure_seconds"]) if t]
    assert times == sorted(times), "reconstruction must be monotonic across midnight"
    assert times[0] < 86400 < times[-1]  # starts before midnight, ends after


def test_timetable_starting_after_midnight_gets_base_offset():
    # first time 00:02 -> the run belongs to the PREVIOUS service day: every
    # second is >= 86400 (24h past that day's midnight), matching the 03:00 cutover
    rows = train_timetable_rows(_by_urn("odpt.TrainTimetable:Toei.Asakusa.2265H.Weekday"))
    times = [t for r in rows for t in (r["arrival_seconds"], r["departure_seconds"]) if t]
    assert times and all(t >= 86400 for t in times)
    assert times == sorted(times)


def test_timetable_rows_keep_rt_join_keys():
    # RT odpt:Train joins TrainTimetable on (railway, trainNumber [, calendar]);
    # train_urn carries the direct odpt:train link where the operator sets it
    for obj in load("train_timetable"):
        rows = train_timetable_rows(obj)
        assert rows[0]["train_number"] == obj["odpt:trainNumber"]
        assert rows[0]["railway_urn"] == obj["odpt:railway"]
        assert rows[0]["calendar_urn"] == obj["odpt:calendar"]
        assert all(r["station_urn"] for r in rows)
