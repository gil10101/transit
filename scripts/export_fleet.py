"""Fleet snapshot for the site: what each city has on the road at once.

These scans are the expensive ones (three weekdays of 2.2B predictions and
167M positions), so they live here rather than in export_site_data.py and run
on demand: the fleet is fixed once a city is retired, and the answer does not
change twice a day. Writes site/data/fleet.json.

One rule for every city. A trip is IN MOTION in a poll-minute once its first
scheduled stop is reached and until its last prediction (or, for a delay-only
feed, its last scheduled stop) passes — one vehicle each. Where a city also
publishes positions, the same minute is cross-checked: vehicles reporting a
fix under two minutes old and holding a trip, and vehicles reporting with no
trip at all (deadhead, layover, yard), which are never folded in.

Usage: uv run python scripts/export_fleet.py [--from 2026-09-09 --to 2026-09-11]
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent))
from sitedata_lib import dump, q  # noqa: E402


def arg(flag: str, default: str) -> str:
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


D0, D1 = arg("--from", "2026-09-09"), arg("--to", "2026-09-11")
AS_OF = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

in_motion = q(f"""
    with win as (
        select city_key, trip_uid, min(sched_arr_ts_utc) as t0, max(sched_arr_ts_utc) as t1
        from int_stop_events_finalized
        where service_date between '{D0}' and '{D1}'
        group by 1, 2
    ), p as (
        select t.city as city_key, date_trunc('minute', t.fetched_at) as m, t.trip_uid, t.route_id,
               max(coalesce(t.arr_pred_ts_utc, t.dep_pred_ts_utc)) as p1
        from silver.stop_time_predictions t
        where t.service_date between '{D0}' and '{D1}'
        group by 1, 2, 3, 4
    ), live as (
        select p.city_key, p.m, coalesce(r.mode, 'unknown') as mode, p.trip_uid
        from p
        join win w on w.city_key = p.city_key and w.trip_uid = p.trip_uid
        left join stg_gtfs__routes r on r.city_key = p.city_key and r.route_id = p.route_id
        where w.t0 <= dateadd(minute, 1, p.m)
          and coalesce(p.p1, w.t1) >= dateadd(minute, -1, p.m)
    ), per_min as (
        select city_key, mode, m, count(distinct trip_uid) as n from live group by 1, 2, 3
    )
    select city_key, mode, max(n) as peak, round(avg(n)) as avg_minute
    from per_min group by 1, 2 order by 1, 2
""")

positions = q(f"""
    with p as (
        select v.city as city_key, date_trunc('minute', v.fetched_at) as m,
               case when v.route_id is null then 'no_route'
                    else coalesce(r.mode, 'unknown') end as mode,
               coalesce(v.vehicle_id, v.trip_uid) as vid,
               max(case when v.trip_uid is not null then 1 else 0 end) as on_trip,
               max(case when v.ts_utc >= dateadd(minute, -2, v.fetched_at)
                        then 1 else 0 end) as fresh
        from silver.vehicle_positions v
        left join stg_gtfs__routes r on r.city_key = v.city and r.route_id = v.route_id
        where v.service_date between '{D0}' and '{D1}'
        group by 1, 2, 3, 4
    ), s as (
        select city_key, mode, m,
               count(distinct case when on_trip = 1 and fresh = 1 then vid end) as on_trip_fresh,
               count(distinct case when on_trip = 0 then vid end) as no_trip
        from p group by 1, 2, 3
    )
    select city_key, mode, max(on_trip_fresh) as on_trip_fresh, max(no_trip) as no_trip
    from s group by 1, 2 order by 1, 2
""")

trains = q(f"""
    with s as (
        select date_trunc('minute', fetched_at) as m, count(distinct train_number) as n
        from silver.odpt_trains where service_date between '{D0}' and '{D1}' group by 1
    )
    select 'tokyo' as city_key, 'metro' as mode, max(n) as peak, round(avg(n)) as avg_minute
    from s
""")

cities: dict[str, dict] = {}
for r in in_motion + trains:
    c = cities.setdefault(r["city_key"], {"in_motion": {}, "positions": {}})
    c["in_motion"][r["mode"]] = {"peak": int(r["peak"]), "avg_minute": int(r["avg_minute"])}
for r in positions:
    c = cities.setdefault(r["city_key"], {"in_motion": {}, "positions": {}})
    c["positions"][r["mode"]] = {
        "on_trip_fresh": int(r["on_trip_fresh"]),
        "no_trip": int(r["no_trip"]),
    }

dump("fleet.json", {"as_of": AS_OF, "window": [D0, D1], "cities": cities})
print("fleet.json written for", ", ".join(sorted(cities)))
