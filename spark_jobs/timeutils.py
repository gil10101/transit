"""GTFS time semantics, implemented once and shared (Spark jobs, static parser, tests).

Rules (docs/transit-pulse-plan.md §4, CLAUDE.md canonical rules):
- service_date = local timestamp minus 12h, date part (GTFS noon rule).
- Static GTFS times may exceed 24:00:00 and are offsets from "noon minus 12h" of the
  service date — an absolute anchor that stays correct across DST transitions.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc  # noqa: UP017 — EMR Serverless runs py3.9; datetime.UTC needs 3.11


def gtfs_time_to_seconds(hhmmss: str) -> int:
    """'25:30:00' -> 91800. Accepts H:MM:SS and HH:MM:SS, hours may exceed 24."""
    h, m, s = (int(part) for part in hhmmss.strip().split(":"))
    return h * 3600 + m * 60 + s


def service_date_of(ts_utc: datetime, tz_name: str) -> date:
    """Service date of an instant: local wall time minus 12h, date part."""
    local = ts_utc.astimezone(ZoneInfo(tz_name))
    return (local - timedelta(hours=12)).date()


def noon_minus_12h_utc(service_date: date, tz_name: str) -> datetime:
    """UTC instant of the service day's anchor (local noon minus 12h)."""
    noon_local = datetime.combine(service_date, time(12), tzinfo=ZoneInfo(tz_name))
    return noon_local.astimezone(UTC) - timedelta(hours=12)


def scheduled_ts_utc(service_date: date, gtfs_seconds: int, tz_name: str) -> datetime:
    """UTC timestamp of a static GTFS time on a given service date (DST-safe)."""
    return noon_minus_12h_utc(service_date, tz_name) + timedelta(seconds=gtfs_seconds)
