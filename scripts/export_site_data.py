"""Export warehouse snapshots to site/data/*.json for the static portfolio site.

Read path only, same key-pair auth as the dashboard. The site is a static page
(Vercel) so every number it shows is a build-time snapshot stamped with as_of —
"last snapshot, honestly labeled" rather than a fake live feed.

Outputs:
  site/data/summary.json  fleet census + per-city standings (provisional metric
                          definitions identical to dashboard/app/Home.py)
  site/data/daily.json    per-city service-weighted OTP + completeness by closed
                          judged day (standings sparklines)
  site/data/hourly.json   per-city on-time share by local hour, closed judged
                          days only (the shape-of-a-day chart)
  site/data/maps/*.json   route shapes + latest vehicle fixes + stop delays,
                          same extraction as the proof renders

Usage: uv run python scripts/export_site_data.py [--skip-maps]
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "site" / "data"
# [rev 2026-09-03] tokyo added — its poller is live and it reaches every gold
# mart. Chicago stays out until CTA activates the beta key (docs/04). Note when
# reading tokyo's standings: its delay is operator-stated and rounded to whole
# minutes, so its OTP is not measured the same way as the other seven
# (docs/06 "Tokyo's punctuality number"). The site must say so wherever it ranks.
CITIES = ["nyc", "boston", "dc", "sf", "toronto", "helsinki", "zurich", "tokyo"]


def connect() -> snowflake.connector.SnowflakeConnection:
    key_path = Path(
        os.environ.get(
            "SNOWFLAKE_PRIVATE_KEY_PATH",
            Path.home() / ".snowflake/keys/transit_terraform_key.p8",
        )
    )
    pk = serialization.load_pem_private_key(key_path.read_bytes(), password=None)
    pkb = pk.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=os.environ.get("SNOWFLAKE_ACCOUNT", "wqteqyy-ib47757"),
        user=os.environ.get("SNOWFLAKE_USER", "TERRAFORM_SVC"),
        private_key=pkb,
        role=os.environ.get("SNOWFLAKE_ROLE", "TRANSIT_PIPELINE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "TRANSFORM_XS"),
        database="TRANSIT",
        schema="GOLD",
    )


CUR = connect().cursor()


def q(sql: str) -> list[dict]:
    CUR.execute(sql)
    cols = [c[0].lower() for c in CUR.description]
    return [dict(zip(cols, r, strict=True)) for r in CUR.fetchall()]


def dump(name: str, obj) -> None:
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, default=str, separators=(",", ":")))
    print(f"{name}: {path.stat().st_size:,} bytes", flush=True)


AS_OF = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

# --- summary.json ----------------------------------------------------------
census = q("""
    select count(*) as stop_events,
           count(distinct service_date) as service_days,
           count(distinct city_key) as cities,
           max(service_date) as latest_day
    from fct_stop_events
    where otp_band is not null
""")[0]
census["gold_rows"] = q(
    "select sum(row_count) as n from TRANSIT.INFORMATION_SCHEMA.TABLES where table_schema='GOLD'"
)[0]["n"]
census["silver_rows"] = q(
    "select sum(row_count) as n from TRANSIT.INFORMATION_SCHEMA.TABLES where table_schema='SILVER'"
)[0]["n"]
trips = q("""
    select sum(trips_observed) as observed, sum(trips_scheduled) as scheduled
    from fct_service_delivery_daily
""")[0]

# identical definitions to dashboard/app/Home.py "provisional metrics"
standings = q("""
    with eligible as (
        select d.city_key, d.route_id, d.service_date,
               d.trips_scheduled, d.trips_observed, d.trips_cancelled,
               d.completeness_pct
        from fct_service_delivery_daily d
        where d.service_day_closed
          and d.service_date >= d.metrics_from
          and coalesce(d.completeness_pct, 0) >= 0.50
    )
    select
        e.city_key,
        count(distinct e.service_date) as judged_days,
        sum(e.trips_observed) as observed_trips,
        round(100 * sum(e.completeness_pct * e.trips_scheduled)
            / nullif(sum(e.trips_scheduled), 0), 1) as completeness_pct,
        round(100 * sum(r.otp_pct * r.banded_events)
            / nullif(sum(case when r.otp_pct is not null then r.banded_events end), 0), 1)
            as otp_pct,
        round(sum(r.ewt_sec * r.ewt_gap_count)
            / nullif(sum(case when r.ewt_sec is not null then r.ewt_gap_count end), 0))
            as ewt_sec,
        round(100 * sum(r.bunching_pct * r.rated_gaps)
            / nullif(sum(case when r.bunching_pct is not null then r.rated_gaps end), 0), 1)
            as bunching_pct,
        round(100 * cast(sum(e.trips_cancelled) as double)
            / nullif(sum(e.trips_scheduled), 0), 2) as cancel_pct
    from eligible e
    left join fct_route_reliability_daily r
      on r.city_key = e.city_key and r.route_id = e.route_id
     and r.service_date = e.service_date
    group by 1
    order by otp_pct desc
""")
events = {
    r["city_key"]: r
    for r in q("""
        select city_key, count(*) as events, count(distinct route_id) as routes_seen
        from fct_stop_events
        where otp_band is not null
        group by 1
    """)
}
for row in standings:
    row.update(events.get(row["city_key"], {"events": 0, "routes_seen": 0}))

dump(
    "summary.json",
    {"as_of": AS_OF, "census": census, "trips": trips, "standings": standings},
)

# --- daily.json ------------------------------------------------------------
daily = q("""
    with eligible as (
        select d.city_key, d.route_id, d.service_date
        from fct_service_delivery_daily d
        where d.service_day_closed
          and d.service_date >= d.metrics_from
          and coalesce(d.completeness_pct, 0) >= 0.50
    )
    select e.city_key, e.service_date,
           round(100 * sum(r.otp_pct * r.banded_events)
               / nullif(sum(case when r.otp_pct is not null then r.banded_events end), 0), 1)
               as otp_pct
    from eligible e
    join fct_route_reliability_daily r
      on r.city_key = e.city_key and r.route_id = e.route_id
     and r.service_date = e.service_date
    group by 1, 2
    order by 1, 2
""")
dump("daily.json", {"as_of": AS_OF, "rows": daily})

# --- hourly.json -----------------------------------------------------------
hourly = q("""
    with closed as (
        select distinct city_key, service_date
        from fct_service_delivery_daily
        where service_day_closed and service_date >= metrics_from
    )
    select e.city_key, e.local_hour,
           round(100 * avg(case when e.otp_band = 'on_time' then 1.0 else 0.0 end), 1)
               as otp_pct,
           count(*) as events
    from fct_stop_events e
    join closed c on c.city_key = e.city_key and c.service_date = e.service_date
    where e.otp_band is not null
    group by 1, 2
    order by 1, 2
""")
dump("hourly.json", {"as_of": AS_OF, "rows": hourly})

# --- dist.json: delay distribution, 30s buckets clamped to [-300, 900] ------
dist = q("""
    select city_key,
           cast(floor(greatest(-300, least(900, delay_arr_sec)) / 30) * 30 as int)
               as bucket_sec,
           count(*) as n
    from fct_stop_events
    where otp_band is not null and delay_arr_sec is not null
    group by 1, 2
    order by 1, 2
""")
dump("dist.json", {"as_of": AS_OF, "rows": dist})

# --- modes.json: on-time share by city x mode -------------------------------
modes = q("""
    select e.city_key, r.mode,
           count(*) as events,
           round(100 * avg(case when e.otp_band = 'on_time' then 1.0 else 0.0 end), 1)
               as otp_pct
    from fct_stop_events e
    join dim_route r on r.route_key = e.route_key
    where e.otp_band is not null and r.mode is not null
    group by 1, 2
    having count(*) >= 5000
    order by 1, 2
""")
dump("modes.json", {"as_of": AS_OF, "rows": modes})

# --- hexes.json: stop delay aggregated to H3 r8, last 7 days ----------------
# same aggregation as the dashboard delay-map page: scored events only, hexes
# under 20 events dropped, ONE shared scale across cities
hexes = q("""
    select e.city_key, s.h3_r8,
           round(avg(e.delay_arr_sec)) as mean_delay_sec,
           count(*) as events
    from fct_stop_events e
    join dim_stop s on s.stop_key = e.stop_key
    where e.otp_band is not null
      and s.h3_r8 is not null
      and e.service_date >= dateadd(day, -7, current_date)
    group by 1, 2
    having count(*) >= 20
""")
dump("hexes.json", {"as_of": AS_OF, "days": 7, "rows": hexes})

# --- routes.json: most / least reliable routes (evidence-weighted) ----------
ROUTE_AGG = """
    select r.city_key, r.route_id,
           max(coalesce(dr.route_short_name, r.route_id)) as label,
           max(dr.route_long_name) as long_name,
           max(r.mode) as mode,
           sum(r.banded_events) as events,
           round(100 * sum(r.otp_pct * r.banded_events)
               / nullif(sum(case when r.otp_pct is not null then r.banded_events end), 0), 1)
               as otp_pct
    from fct_route_reliability_daily r
    left join dim_route dr on dr.route_key = r.route_key
    group by 1, 2
    having sum(r.banded_events) >= 2000
       and sum(case when r.otp_pct is not null then r.banded_events end) > 0
"""
best = q(f"select * from ({ROUTE_AGG}) order by otp_pct desc, events desc limit 8")
worst = q(f"select * from ({ROUTE_AGG}) order by otp_pct asc, events desc limit 8")
dump("routes.json", {"as_of": AS_OF, "best": best, "worst": worst})

# --- maps/*.json -----------------------------------------------------------
if "--skip-maps" in sys.argv:
    print("maps skipped", flush=True)
    sys.exit(0)


def shapes_from_stop_times(city: str, ver: str) -> list[dict]:
    """No shapes.txt (Swiss national GTFS): derive each observed route's path from
    the stop_times sequence of one representative trip. Restricted to routes seen
    in gold, so the zurich allow-list is applied for free."""
    return q(f"""
        with seen as (
            select distinct route_id from fct_stop_events where city_key='{city}'
        ),
        rep as (
            select t.route_id, t.direction_id, t.trip_id,
                   row_number() over (partition by t.route_id, t.direction_id
                                      order by t.trip_id) as rn
            from TRANSIT.SILVER.GTFS_STATIC_TRIPS t
            join seen on seen.route_id = t.route_id
            where t.city='{city}' and t.gtfs_version_id='{ver}'
        ),
        st as (
            select stop_id, stop_lon, stop_lat
            from TRANSIT.SILVER.GTFS_STATIC_STOPS
            where city='{city}'
            qualify row_number() over (partition by stop_id
                                       order by gtfs_version_id desc) = 1
        )
        select r.route_id, r.trip_id as shape_id,
               listagg(round(st.stop_lon,5) || ' ' || round(st.stop_lat,5), ',')
                   within group (order by x.stop_sequence) as path
        from rep r
        join TRANSIT.SILVER.GTFS_STATIC_STOP_TIMES x
          on x.city='{city}' and x.gtfs_version_id='{ver}' and x.trip_id = r.trip_id
        join st on st.stop_id = x.stop_id
        where r.rn = 1
        group by 1, 2
    """)


def stops_any_version(city: str) -> list[dict]:
    """Helsinki/Zurich event stop_ids span static versions — join each stop's
    newest version row instead of pinning one gtfs_version_id."""
    return q(f"""
        with sd as (
            -- latest HIGH-VOLUME day: a bare max(service_date) lands on misdated
            -- far-future outlier rows
            select max(service_date) as d from (
                select service_date from fct_stop_events
                where city_key='{city}' group by 1 having count(*) > 10000
            )
        ),
        st as (
            select stop_id, stop_lon, stop_lat
            from TRANSIT.SILVER.GTFS_STATIC_STOPS
            where city='{city}'
            qualify row_number() over (partition by stop_id
                                       order by gtfs_version_id desc) = 1
        )
        select st.stop_lon as lon, st.stop_lat as lat,
               round(avg(e.delay_arr_sec), 0) as delay
        from fct_stop_events e
        join st on st.stop_id = e.stop_id
        where e.city_key='{city}' and e.service_date = (select d from sd)
        group by 1, 2
    """)


for city in CITIES:
    data = {"city": city, "as_of": AS_OF}

    # newest static version = the one carrying the most trips (string max is unsafe)
    ver = q(f"""
        select gtfs_version_id from TRANSIT.SILVER.GTFS_STATIC_TRIPS
        where city='{city}' group by 1 order by count(*) desc limit 1
    """)
    if not ver:
        print(f"{city}: no static trips, skipped", flush=True)
        continue
    ver = ver[0]["gtfs_version_id"]

    # one representative shape per (route, direction): the shape most trips use
    data["shapes"] = (
        shapes_from_stop_times(city, ver)
        if city == "zurich"
        else q(f"""
        with rep as (
            select route_id, direction_id, shape_id,
                   row_number() over (partition by route_id, direction_id
                                      order by count(*) desc) as rn
            from TRANSIT.SILVER.GTFS_STATIC_TRIPS
            where city='{city}' and gtfs_version_id='{ver}' and shape_id is not null
            group by 1, 2, 3
        ),
        pts as (
            select r.route_id, s.shape_id,
                   s.shape_pt_lon as lon, s.shape_pt_lat as lat,
                   row_number() over (partition by s.shape_id order by s.shape_pt_sequence) as pn,
                   count(*) over (partition by s.shape_id) as np
            from rep r
            join TRANSIT.SILVER.GTFS_STATIC_SHAPES s
              on s.city='{city}' and s.gtfs_version_id='{ver}' and s.shape_id = r.shape_id
            where r.rn = 1
        )
        select route_id, shape_id,
               listagg(round(lon,5) || ' ' || round(lat,5), ',')
                   within group (order by pn) as path
        from pts
        where mod(pn, greatest(1, ceil(np / 150))) = 0 or pn = 1 or pn = np
        group by 1, 2
    """)
    )

    data["routes"] = {
        r["route_id"]: r
        for r in q(f"""
            select route_id, route_short_name, route_type, route_color
            from TRANSIT.SILVER.GTFS_STATIC_ROUTES
            where city='{city}' and gtfs_version_id='{ver}'
        """)
    }

    # latest fix per vehicle from the most recent drained window
    data["vehicles"] = q(f"""
        select route_id, lon, lat, bearing
        from TRANSIT.SILVER.VEHICLE_POSITIONS
        where city='{city}' and lat is not null
          and fetched_at > dateadd(hour, -3, current_timestamp())
        qualify row_number() over (partition by vehicle_id order by ts_utc desc) = 1
    """)

    # stop dots colored by mean delay on the latest service day with events
    if city in ("helsinki", "zurich"):
        data["stops"] = stops_any_version(city)
    else:
        data["stops"] = q(f"""
            with sd as (
                -- latest HIGH-VOLUME day: a bare max(service_date) lands on
                -- misdated far-future outlier rows
                select max(service_date) as d from (
                    select service_date from fct_stop_events
                    where city_key='{city}' group by 1 having count(*) > 10000
                )
            )
            select st.stop_lon as lon, st.stop_lat as lat,
                   round(avg(e.delay_arr_sec), 0) as delay
            from fct_stop_events e
            join TRANSIT.SILVER.GTFS_STATIC_STOPS st
              on st.city='{city}' and st.gtfs_version_id='{ver}' and st.stop_id = e.stop_id
            where e.city_key='{city}' and e.service_date = (select d from sd)
            group by 1, 2
        """)

    dump(f"maps/{city}.json", data)
