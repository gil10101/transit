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

import sys

from sitedata_lib import AS_OF, CITIES, dump, q  # noqa: E402

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

# identical definitions to dashboard/app/Home.py "provisional metrics".
# [rev 2026-09-25] every eligibility set here carries `not retired_day`, the
# same gate as fct_city_scorecard: with all eight pollers retired, a day cut
# short by our own decision must never count as a judged day.
standings = q("""
    with eligible as (
        select d.city_key, d.route_id, d.service_date,
               d.trips_scheduled, d.trips_observed, d.trips_cancelled,
               d.trips_cancelled_unscheduled,
               d.completeness_pct
        from fct_service_delivery_daily d
        where d.service_day_closed
          and d.service_date >= d.metrics_from
          and not d.retired_day
          and coalesce(d.completeness_pct, 0) >= 0.50
    ),
    -- [rev 2026-09-26] reliability is keyed per DIRECTION, delivery per route:
    -- joining them raw repeated every delivery row once per direction, so
    -- observed trips, cancellations and completeness read ~2x (SF cancels showed
    -- 2.85% here against 2.79% in answers.json). Collapse to the route-day first;
    -- the weighted sums below are unchanged by it.
    rel as (
        select city_key, route_id, service_date,
               sum(otp_pct * banded_events) as otp_w,
               sum(case when otp_pct is not null then banded_events end) as otp_n,
               sum(ewt_sec * ewt_gap_count) as ewt_w,
               sum(case when ewt_sec is not null then ewt_gap_count end) as ewt_n,
               sum(bunching_pct * rated_gaps) as bunch_w,
               sum(case when bunching_pct is not null then rated_gaps end) as bunch_n
        from fct_route_reliability_daily
        group by 1, 2, 3
    )
    select
        e.city_key,
        count(distinct e.service_date) as judged_days,
        sum(e.trips_observed) as observed_trips,
        round(100 * sum(e.completeness_pct * e.trips_scheduled)
            / nullif(sum(e.trips_scheduled), 0), 1) as completeness_pct,
        round(100 * sum(r.otp_w) / nullif(sum(r.otp_n), 0), 1) as otp_pct,
        round(sum(r.ewt_w) / nullif(sum(r.ewt_n), 0)) as ewt_sec,
        round(100 * sum(r.bunch_w) / nullif(sum(r.bunch_n), 0), 1) as bunching_pct,
        round(100 * cast(sum(e.trips_cancelled) as double)
            / nullif(sum(e.trips_scheduled), 0), 2) as cancel_pct,
        -- A feed that never emits CANCELED and a feed that emits it and cancelled
        -- nothing are different facts, and 0% renders them identically. The
        -- standings row shows a dash for the first (docs/06 Q6).
        -- [rev 2026-09-25] counted over BOTH buckets. The cancelled numerator
        -- matches through the newest timetable, so after a static rotation a
        -- historical cancel can land in _unscheduled instead — Helsinki and DC
        -- read zero matched cancels after the 09-20 rotation while their feeds
        -- still carry 1,389 and 415 CANCELED trips. Whether a feed SAYS
        -- cancelled is feed truth, independent of which timetable matched it.
        (sum(e.trips_cancelled + e.trips_cancelled_unscheduled) > 0) as emits_cancels,
        -- the raw count travels with the rate: round(pct, 2) turns Toronto's 4
        -- cancellations in 661,306 trips into 0.0 before the page can tell that
        -- apart from a feed that cancelled nothing
        sum(e.trips_cancelled) as cancelled,
        -- The composite is the answer to the page's actual question, so it has to
        -- be IN the table the page ranks by. Showing an on-time ranking beside a
        -- sentence naming composite scores put Tokyo first at 97.0% next to text
        -- saying Helsinki leads at 92.2, with the score nowhere on screen.
        max(sc.score_0_100) as score_0_100,
        -- Tokyo's delay is the operator's own, rounded to whole minutes, so its
        -- on-time rate is not measured the way the other seven are (docs/06). The
        -- flag comes from dim_city rather than a hard-coded city name.
        -- [rev 2026-09-26] the public page no longer prints a marker for it: the
        -- owner chose numbers without caveats. The flag stays in the export.
        max(c.rt_delay_source) = 'odpt_stated' as delay_operator_stated
    from eligible e
    join dim_city c on c.city_key = e.city_key
    left join rel r
      on r.city_key = e.city_key and r.route_id = e.route_id
     and r.service_date = e.service_date
    left join fct_city_scorecard sc
      on sc.city_key = e.city_key
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
          and not d.retired_day
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
# [rev 2026-09-26] gated on judged route-days like every other standings
# figure; it was closed days only, so retired half-days and routes under the
# completeness floor still reached the chart the page calls "judged days".
hourly = q("""
    with eligible as (
        select city_key, route_id, service_date
        from fct_service_delivery_daily
        where service_day_closed and service_date >= metrics_from
          and not retired_day and coalesce(completeness_pct, 0) >= 0.50
    )
    select e.city_key, e.local_hour,
           round(100 * avg(case when e.otp_band = 'on_time' then 1.0 else 0.0 end), 1)
               as otp_pct,
           count(*) as events
    from fct_stop_events e
    join eligible c on c.city_key = e.city_key and c.route_id = e.route_id
     and c.service_date = e.service_date
    where e.otp_band is not null
      and e.local_hour is not null  -- no arrival time, no hour
    group by 1, 2
    -- A city-hour drawn from a handful of events is noise with a line through it:
    -- Tokyo's 01:00 held 10 last-train stragglers and plotted as a 0% collapse beside
    -- hours of 5k-25k events. Same floor the dashboard's Patterns page uses.
    having count(*) >= 200
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

# --- hexes.json: stop delay aggregated to H3 r8, each city's last 7 judged days
# same aggregation as the dashboard delay-map page: scored events only, hexes
# under 20 events dropped, ONE shared scale across cities.
# [rev 2026-09-20] The window was `current_date - 7`, which only worked while
# every city was still polling. Seven pollers retired on 09-15/16, so a wall-clock
# window slides off their last service day and empties the map city by city —
# NYC and Toronto (last day 09-14) were four days from vanishing. Anchoring to
# each city's own last seven JUDGED days (the standings' definition: closed, at
# or after metrics_from, not retired, completeness over the floor) makes the map
# a fixed picture of each city's final week instead of a decaying one.
hexes = q("""
    with judged as (
        select city_key, service_date,
               row_number() over (partition by city_key order by service_date desc) as rn
        from fct_service_delivery_daily
        where service_day_closed
          and service_date >= metrics_from
          and not retired_day
          and coalesce(completeness_pct, 0) >= 0.5
        group by 1, 2
    )
    select e.city_key, s.h3_r8,
           round(avg(e.delay_arr_sec)) as mean_delay_sec,
           count(*) as events
    from fct_stop_events e
    join dim_stop s on s.stop_key = e.stop_key
    join judged j on j.city_key = e.city_key and j.service_date = e.service_date and j.rn <= 7
    where e.otp_band is not null
      and s.h3_r8 is not null
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

# --- answers.json: the docs/06 sub-questions the sections above don't already
# answer, measured with the same eligibility gate as the standings. One file,
# four keyed blocks, so the findings section renders from a single fetch.
ELIGIBLE = """
    with eligible as (
        select d.city_key, d.route_id, d.service_date
        from fct_service_delivery_daily d
        where d.service_day_closed
          and d.service_date >= d.metrics_from
          and not d.retired_day
          and coalesce(d.completeness_pct, 0) >= 0.50
    )
"""

weekday_weekend = q(
    ELIGIBLE
    + """
    select f.city_key,
           case when dayofweekiso(f.service_date) >= 6 then 'weekend'
                else 'weekday' end as day_type,
           round(100 * avg(case when f.otp_band = 'on_time' then 1.0 else 0.0 end), 1)
               as otp_pct,
           count(f.otp_band) as events
    from fct_stop_events f
    join eligible e
      on e.city_key = f.city_key and e.route_id = f.route_id
     and e.service_date = f.service_date
    where f.otp_band is not null
    group by 1, 2
    having count(f.otp_band) >= 1000
    order by 1, 2
"""
)

# locked rule: bus departing a scheduled timepoint > 60s early = failure. Only
# cities whose statics carry timepoint can appear. The flag is the fact's; the
# timepoint column never reached the prod relation, so the denominator joins the
# finalized int table at the same grain; mode joins dim_route (modes.json's join).
# [rev 2026-09-17] every mode whose static marks timepoints, not just bus. The
# mart's early_departure_flag keeps the locked bus rule; this finding asks the
# wider question with the same inputs (timepoint, departure delay, 60s grace —
# mirrors dbt var early_departure_grace_sec) and carries the mode so the page
# can show the split.
early_departures = q(
    ELIGIBLE
    + """
    select f.city_key, dr.mode,
           round(100 * count(case when f.delay_dep_sec < -60 then 1 end)
               / count(*), 1) as early_dep_pct,
           count(*) as measured
    from fct_stop_events f
    join dim_route dr on dr.route_key = f.route_key
    join int_stop_events_finalized i
      on i.city_key = f.city_key and i.service_date = f.service_date
     and i.trip_uid = f.trip_uid and i.stop_sequence = f.stop_sequence
    join eligible e
      on e.city_key = f.city_key and e.route_id = f.route_id
     and e.service_date = f.service_date
    where i.timepoint = 1 and f.delay_dep_sec is not null
    group by 1, 2
    having count(*) >= 1000
    order by 1, 2
"""
)

# peak vs off-peak per each city's own seeded windows (dim_city peak_* columns,
# local time by construction — fct_stop_events.local_hour is already local)
peak_offpeak = q(
    ELIGIBLE
    + """
    select f.city_key,
           case when f.local_hour >= hour(to_time(c.peak_am_start))
                 and f.local_hour <  hour(to_time(c.peak_am_end))   then 'am_peak'
                when f.local_hour >= hour(to_time(c.peak_pm_start))
                 and f.local_hour <  hour(to_time(c.peak_pm_end))   then 'pm_peak'
                when f.local_hour >= hour(to_time(c.peak_am_end))
                 and f.local_hour <  hour(to_time(c.peak_pm_start)) then 'midday'
                else 'off_hours' end as day_part,
           round(100 * avg(case when f.otp_band = 'on_time' then 1.0 else 0.0 end), 1)
               as otp_pct,
           count(f.otp_band) as events
    from fct_stop_events f
    join dim_city c on c.city_key = f.city_key
    join eligible e
      on e.city_key = f.city_key and e.route_id = f.route_id
     and e.service_date = f.service_date
    where f.otp_band is not null
    group by 1, 2
    having count(f.otp_band) >= 1000
    order by 1, 2
"""
)

# cancel rate context: a 0.000% is a feed property (the feed never emits
# CANCELED), not a service property — docs/06. emits_cancels makes that visible.
cancellations = q(
    ELIGIBLE
    + """
    select e.city_key,
           round(100 * cast(sum(d.trips_cancelled) as double)
               / nullif(sum(d.trips_scheduled), 0), 2) as cancel_pct,
           sum(d.trips_cancelled) as cancelled,
           (sum(d.trips_cancelled + d.trips_cancelled_unscheduled) > 0) as emits_cancels
    from eligible e
    join fct_service_delivery_daily d
      on d.city_key = e.city_key and d.route_id = e.route_id
     and d.service_date = e.service_date
    group by 1
    order by cancel_pct desc
"""
)

dump(
    "answers.json",
    {
        "as_of": AS_OF,
        "weekday_weekend": weekday_weekend,
        "early_departures": early_departures,
        "peak_offpeak": peak_offpeak,
        "cancellations": cancellations,
    },
)

# --- extras.json: four cuts the standings can't show, same judged-day gate ---
# late: how far the delay distribution reaches, not just how much of it lands
# inside the on-time window.
extras_late = q(
    ELIGIBLE
    + """
    select f.city_key, count(*) as events,
           round(approx_percentile(f.delay_arr_sec, 0.50)) as p50,
           round(approx_percentile(f.delay_arr_sec, 0.75)) as p75,
           round(approx_percentile(f.delay_arr_sec, 0.90)) as p90,
           round(approx_percentile(f.delay_arr_sec, 0.95)) as p95,
           round(avg(f.delay_arr_sec)) as mean_sec
    from fct_stop_events f
    join eligible e
      on e.city_key = f.city_key and e.route_id = f.route_id
     and e.service_date = f.service_date
    where f.otp_band is not null and f.delay_arr_sec is not null
    group by 1
    order by 1
"""
)

# rain: a wet hour has >= 0.5 mm of precipitation. Rain is not spread evenly
# over the day, so the dry baseline is reweighted to the hours it actually
# rained — a city whose showers fell at rush hour is compared with its own dry
# rush hours, not with a dry average that includes 3am.
WET_MM = 0.5
rain_rows = q(
    ELIGIBLE
    + f"""
    select f.city_key, f.local_hour,
           iff(w.precip_mm >= {WET_MM}, 'wet', 'dry') as sky,
           count(*) as n,
           count_if(f.otp_band = 'on_time') as on_time
    from fct_stop_events f
    join eligible e
      on e.city_key = f.city_key and e.route_id = f.route_id
     and e.service_date = f.service_date
    join fct_weather_hourly w
      on w.city_key = f.city_key and w.local_date = f.local_date
     and w.local_hour = f.local_hour
    where f.otp_band is not null
    group by 1, 2, 3
"""
)
extras_rain = []
for city in sorted({r["city_key"] for r in rain_rows}):
    cells = {(r["local_hour"], r["sky"]): r for r in rain_rows if r["city_key"] == city}
    wet = [v for (_, sky), v in cells.items() if sky == "wet"]
    wet_n = sum(v["n"] for v in wet)
    if wet_n < 5000:  # too few wet arrivals to say anything about rain
        continue
    expected = 0.0
    for (hour, sky), v in cells.items():
        dry = cells.get((hour, "dry"))
        if sky == "wet" and dry and dry["n"]:
            expected += v["n"] * dry["on_time"] / dry["n"]
    extras_rain.append(
        {
            "city_key": city,
            "wet_events": wet_n,
            "wet_otp": round(100 * sum(v["on_time"] for v in wet) / wet_n, 1),
            "dry_otp": round(100 * expected / wet_n, 1),
        }
    )

# along: median delay by how far along its trip the vehicle is, first stop = 0
# and last = 1, in tenths.
extras_along = q(
    ELIGIBLE
    + """
    , scored as (
        select f.city_key, f.service_date, f.trip_uid, f.stop_sequence, f.delay_arr_sec
        from fct_stop_events f
        join eligible e
          on e.city_key = f.city_key and e.route_id = f.route_id
         and e.service_date = f.service_date
        where f.otp_band is not null and f.delay_arr_sec is not null
    ),
    trip as (
        select city_key, service_date, trip_uid,
               min(stop_sequence) as lo, max(stop_sequence) as hi
        from scored
        group by 1, 2, 3
    ),
    ev as (
        select s.city_key, s.delay_arr_sec,
               (s.stop_sequence - t.lo) / nullif(t.hi - t.lo, 0) as progress
        from scored s
        join trip t
          on t.city_key = s.city_key and t.service_date = s.service_date
         and t.trip_uid = s.trip_uid
    )
    select city_key, cast(least(9, floor(progress * 10)) as int) as decile,
           count(*) as events,
           round(approx_percentile(delay_arr_sec, 0.5)) as p50
    from ev
    where progress is not null
    group by 1, 2
    order by 1, 2
"""
)

# spread: every route with enough evidence, so the page can show the whole
# distribution of a city's routes rather than one average.
extras_routes = q(
    ELIGIBLE
    + """
    select r.city_key, r.route_id,
           max(coalesce(dr.route_short_name, r.route_id)) as label,
           max(r.mode) as mode,
           sum(r.banded_events) as events,
           round(100 * sum(r.otp_pct * r.banded_events)
               / nullif(sum(case when r.otp_pct is not null then r.banded_events end), 0), 1)
               as otp_pct
    from fct_route_reliability_daily r
    join eligible e
      on e.city_key = r.city_key and e.route_id = r.route_id
     and e.service_date = r.service_date
    left join dim_route dr on dr.route_key = r.route_key
    group by 1, 2
    having sum(r.banded_events) >= 2000
       and sum(case when r.otp_pct is not null then r.banded_events end) > 0
    order by 1, otp_pct
"""
)
dump(
    "extras.json",
    {
        "as_of": AS_OF,
        "wet_mm": WET_MM,
        "late": extras_late,
        "rain": extras_rain,
        "along": extras_along,
        "routes": extras_routes,
    },
)

# --- storage.json: what each city put in the warehouse ----------------------
# Per city: the mode mix of its finalized stop events, its silver and gold row
# counts, distinct trips/routes/stops/vehicles, and every service day of data.
# Whole-history counts, no eligibility gate — this is the inventory, not a
# judgment; the judged flag on each day says which ones the standings use.
storage_modes = q("""
    select f.city_key, coalesce(r.mode, 'unknown') as mode,
           count(*) as events, count(f.otp_band) as scored,
           count(distinct f.route_key) as routes
    from fct_stop_events f
    left join dim_route r on r.route_key = f.route_key
    group by 1, 2
    order by 1, 2
""")
storage_days = q("""
    with judged as (
        select distinct city_key, service_date
        from fct_service_delivery_daily
        where service_day_closed and service_date >= metrics_from
          and not retired_day and coalesce(completeness_pct, 0) >= 0.50
    )
    select f.city_key, f.service_date,
           count(*) as events, count(distinct f.trip_uid) as trips,
           max(case when j.city_key is not null then 1 else 0 end) = 1 as judged
    from fct_stop_events f
    left join judged j on j.city_key = f.city_key and j.service_date = f.service_date
    group by 1, 2
    order by 1, 2
""")
# [rev 2026-09-26] routes and stops count agency ids, not SCD2 keys: a route or
# stop that outlived a timetable rotation holds one key per version, which put
# Toronto at 14,491 stops.
storage_totals = q("""
    select city_key, count(*) as events, count(otp_band) as scored,
           count(distinct trip_uid) as trips, count(distinct route_id) as routes,
           count(distinct stop_id) as stops, count(distinct service_date) as days
    from fct_stop_events
    group by 1
""")
storage_headways = q("select city_key, count(*) as headways from fct_headways group by 1")
storage_silver = q("""
    select 'predictions' as t, city as city_key, count(*) as n
    from silver.stop_time_predictions group by 1, 2
    union all select 'positions', city, count(*) from silver.vehicle_positions group by 1, 2
    union all select 'alerts', city, count(*) from silver.alerts group by 1, 2
    union all select 'trains', city, count(*) from silver.odpt_trains group by 1, 2
""")
storage_vehicles = q("""
    select city as city_key, count(distinct vehicle_id) as vehicles
    from silver.vehicle_positions where vehicle_id is not null group by 1
""")
storage: dict[str, dict] = {}
for r in storage_totals:
    storage[r["city_key"]] = {
        "events": int(r["events"]),
        "scored": int(r["scored"]),
        "trips": int(r["trips"]),
        "routes": int(r["routes"]),
        "stops": int(r["stops"]),
        "days": int(r["days"]),
        "headways": 0,
        "vehicles": None,
        "silver": {},
        "modes": [],
        "days_series": [],
    }
for r in storage_headways:
    storage[r["city_key"]]["headways"] = int(r["headways"])
for r in storage_vehicles:
    storage[r["city_key"]]["vehicles"] = int(r["vehicles"])
for r in storage_silver:
    storage[r["city_key"]]["silver"][r["t"]] = int(r["n"])
for r in storage_modes:
    storage[r["city_key"]]["modes"].append(
        {
            "mode": r["mode"],
            "events": int(r["events"]),
            "scored": int(r["scored"]),
            "routes": int(r["routes"]),
        }
    )
for r in storage_days:
    storage[r["city_key"]]["days_series"].append(
        {
            "service_date": str(r["service_date"]),
            "events": int(r["events"]),
            "trips": int(r["trips"]),
            "judged": bool(r["judged"]),
        }
    )
dump("storage.json", {"as_of": AS_OF, "cities": storage})

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

    # [rev 2026-09-26] every poller is retired, so "the most recent drained
    # window" is empty for all of them. The map now shows one fixed moment per
    # city instead: the last fix per vehicle in the five minutes to 08:30 local
    # on its last judged day, a morning peak every retirement left intact.
    data["vehicles"] = q(f"""
        with lj as (
            select max(service_date) as d from fct_service_delivery_daily
            where city_key='{city}' and service_day_closed and service_date >= metrics_from
              and not retired_day and coalesce(completeness_pct, 0) >= 0.5
        ),
        t as (
            select lj.d, convert_timezone(c.iana_tz, 'UTC',
                       timestamp_ntz_from_parts(lj.d, time '08:30:00')) as t_utc
            from lj join dim_city c on c.city_key = '{city}'
        )
        select v.route_id, v.lon, v.lat, v.bearing
        from TRANSIT.SILVER.VEHICLE_POSITIONS v, t
        where v.city='{city}' and v.lat is not null
          and v.service_date between dateadd(day, -1, t.d) and dateadd(day, 1, t.d)
          and v.ts_utc between dateadd(minute, -5, t.t_utc) and t.t_utc
        qualify row_number() over (partition by v.vehicle_id order by v.ts_utc desc) = 1
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
