"""GTFS time semantics: the reference implementation of the canonical rules.

**What actually uses what.** The hot paths are Spark SQL column expressions, not these
functions — `silver_normalize.with_common` computes service_date in SQL because doing it
per row in Python would be far slower. What IS shared is the *constants* below:
`silver_normalize` imports them, so the per-city cutover exists in exactly one place.
These functions are the executable statement of the semantics those expressions must
match, and the tests pin them.

[rev 2026-08-25] This module's docstring used to claim it was "shared (Spark jobs, static
parser, tests)". It was imported by its tests and nothing else, and `service_date_of`
hard-coded 12h — so it silently disagreed with production for Toronto by 8 hours. If you
add a rule here, wire it in or say plainly that it is reference-only.

Rules (docs/transit-pulse-plan.md §4, CLAUDE.md canonical rules):
- service_date = local timestamp minus the city's cutover, date part (GTFS noon rule).
- Static GTFS times may exceed 24:00:00 and are offsets from "noon minus 12h" of the
  service date — an absolute anchor that stays correct across DST transitions.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc  # noqa: UP017 — EMR Serverless runs py3.9; datetime.UTC needs 3.11

# Cutover hours for the fetched_at service-date fallback, per city. The GTFS noon rule
# (-12h) is right for feeds whose records may reference yesterday's overnight trips;
# Toronto's feed sets start_date on NO trips (fixture-verified 2026-08-23), so with -12h
# every record fetched between local midnight and noon would be misdated to the previous
# service day. TTC service day rolls ~04:00 local -> -4h. Amendment recorded in docs/01 §F
# and CLAUDE.md canonical rules. IMPORTED BY spark_jobs.silver_normalize — one source.
#
# [rev 2026-08-25] sf added. Its 511 aggregation carries a subset of operators (EM, MC,
# BA, ST, MV, VC, WC, CT, 3D — 3.22% of rows, 132,725 of 4,126,631 on 2026-08-24) that
# omit start_date, so those rows took the -12h default and were stamped a day early:
# everything fetched before local noon landed on the previous service date while its
# predicted arrivals were all on the current one. That produced a PHANTOM service day —
# sf/2026-08-23 held 7,260 events of which exactly 1 genuinely belonged to Aug 23, and
# fct_route_reliability_daily published an OTP for it computed from trips that ran on
# Aug 24. Measured 2026-08-25. boston/dc/helsinki/nyc have zero null-start_date rows and
# are unaffected; toronto is 100% null and already had its exception.
#
# The cutover only ever applies when the feed omits start_date, so adding a city here
# cannot move the 96.8% of sf rows that do set it.
CITY_FALLBACK_CUTOVER_HOURS = {"toronto": 4, "sf": 4}
DEFAULT_FALLBACK_CUTOVER_HOURS = 12


def cutover_hours_for(city: str | None) -> int:
    """Hours to subtract from local wall time to land on the service date."""
    return CITY_FALLBACK_CUTOVER_HOURS.get(city, DEFAULT_FALLBACK_CUTOVER_HOURS)


def gtfs_time_to_seconds(hhmmss: str) -> int:
    """'25:30:00' -> 91800. Accepts H:MM:SS and HH:MM:SS, hours may exceed 24."""
    h, m, s = (int(part) for part in hhmmss.strip().split(":"))
    return h * 3600 + m * 60 + s


def service_date_of(ts_utc: datetime, tz_name: str, city: str | None = None) -> date:
    """Service date of an instant: local wall time minus the city's cutover, date part.

    `city` is optional only because most cities take the default; pass it whenever you
    have it, or Toronto comes out a day early for anything fetched before local noon.
    """
    local = ts_utc.astimezone(ZoneInfo(tz_name))
    return (local - timedelta(hours=cutover_hours_for(city))).date()


def noon_minus_12h_utc(service_date: date, tz_name: str) -> datetime:
    """UTC instant of the service day's anchor (local noon minus 12h)."""
    noon_local = datetime.combine(service_date, time(12), tzinfo=ZoneInfo(tz_name))
    return noon_local.astimezone(UTC) - timedelta(hours=12)


def scheduled_ts_utc(service_date: date, gtfs_seconds: int, tz_name: str) -> datetime:
    """UTC timestamp of a static GTFS time on a given service date (DST-safe)."""
    return noon_minus_12h_utc(service_date, tz_name) + timedelta(seconds=gtfs_seconds)
