-- P5: natural keys until SCD2 dims (P6)
--
-- Route-day reliability per (city, route, service_date, direction):
--   * OTP shares + delay stats from fct_stop_events, ADDED trips EXCLUDED
--     (locked rule: volume yes, OTP no)
--   * early_departure_pct from the fact's early_departure_flag — structurally
--     false everywhere for now: the bus-timepoint rule needs stop_times.timepoint,
--     absent from the loaded silver static (see fct_stop_events header)
--   * ewt_sec over frequent slices only (int_service_frequency.is_frequent):
--     within each (stop, daypart) slice AWT = sum(g^2)/(2*sum(g)) on actual
--     gaps, SWT likewise on the slice's scheduled headways, ewt = AWT - SWT
--     kept SIGNED (negative = better-than-schedule regularity; scoring clamps
--     later). Slices roll up to route-direction-day weighted by gap count.
--   * bunching/big-gap shares over all rated gaps (not only frequent slices)
--   * cancel/completeness/trip counts from fct_service_delivery_daily, which
--     is route-grain: both direction rows repeat the route-level values.

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['city_key', 'service_date', 'route_id', 'direction_id']
) }}

with events as (

    -- null direction (unmatched trip without a ..N/..S token) folds to -1 so the
    -- grain, joins, and md5 key behave identically on duckdb and Snowflake
    -- (concat_ws skips NULLs on duckdb but propagates them on Snowflake)
    select
        * exclude (direction_id),
        coalesce(direction_id, -1) as direction_id
    from {{ ref('fct_stop_events') }}
    {% if is_incremental() %}
    where service_date >= current_date - cast(ceil({{ var('lookback_hours') }} / 24.0) as int)
    {% endif %}

),

base as (

    select distinct city_key, service_date, route_id, direction_id
    from events

),

otp as (

    select
        city_key,
        service_date,
        route_id,
        direction_id,
        count(otp_band) as n_banded,
        count(case when otp_band = 'on_time' then 1 end) as n_on_time,
        count(case when otp_band = 'early' then 1 end) as n_early,
        count(case when otp_band = 'late' then 1 end) as n_late,
        count(case when otp_band = 'very_late' then 1 end) as n_very_late,
        avg(delay_arr_sec) as mean_delay_sec,
        percentile_cont(0.5) within group (order by delay_arr_sec) as median_delay_sec,
        percentile_cont(0.9) within group (order by delay_arr_sec) as p90_delay_sec,
        avg(case when early_departure_flag then 1.0 else 0.0 end) as early_departure_pct
    from events
    where coalesce(schedule_relationship, 'SCHEDULED') <> 'ADDED'
    group by city_key, service_date, route_id, direction_id

),

ewt_slices as (

    select
        h.city_key,
        h.service_date,
        h.route_id,
        h.direction_id,
        h.stop_id,
        h.daypart,
        sum(cast(h.actual_gap_sec as double) * h.actual_gap_sec) as sum_g2,
        sum(cast(h.actual_gap_sec as double)) as sum_g,
        sum(case when h.sched_headway_sec is not null
                 then cast(h.sched_headway_sec as double) * h.sched_headway_sec end) as sum_s2,
        sum(case when h.sched_headway_sec is not null
                 then cast(h.sched_headway_sec as double) end) as sum_s,
        count(*) as n_gaps
    from {{ ref('fct_headways') }} h
    join {{ ref('int_service_frequency') }} f
      on f.city_key = h.city_key
     and f.service_date = h.service_date
     and f.route_id = h.route_id
     and f.direction_id = h.direction_id
     and f.daypart = h.daypart
    where f.is_frequent
      and h.actual_gap_sec > 0
    group by h.city_key, h.service_date, h.route_id, h.direction_id, h.stop_id, h.daypart

),

slice_ewt as (

    select
        *,
        sum_g2 / (2 * sum_g) - sum_s2 / (2 * nullif(sum_s, 0)) as ewt_slice_sec
    from ewt_slices
    where sum_g > 0

),

route_ewt as (

    select
        city_key,
        service_date,
        route_id,
        direction_id,
        sum(case when ewt_slice_sec is not null then ewt_slice_sec * n_gaps end)
            / nullif(sum(case when ewt_slice_sec is not null then n_gaps end), 0) as ewt_sec
    from slice_ewt
    group by city_key, service_date, route_id, direction_id

),

gap_shares as (

    select
        city_key,
        service_date,
        route_id,
        direction_id,
        count(gap_ratio) as n_rated,
        count(case when bunched_flag then 1 end) as n_bunched,
        count(case when big_gap_flag then 1 end) as n_big_gap
    from {{ ref('fct_headways') }}
    group by city_key, service_date, route_id, direction_id

)

select
    md5(concat_ws('|', b.city_key, b.service_date, b.route_id, b.direction_id)) as route_day_key,
    b.city_key,
    b.service_date,
    b.route_id,
    b.direction_id,
    coalesce(r.mode, 'unknown') as mode,
    cast(o.n_on_time as double) / nullif(o.n_banded, 0) as otp_pct,
    cast(o.n_early as double) / nullif(o.n_banded, 0) as early_pct,
    cast(o.n_late as double) / nullif(o.n_banded, 0) as late_pct,
    cast(o.n_very_late as double) / nullif(o.n_banded, 0) as very_late_pct,
    o.early_departure_pct,
    o.mean_delay_sec,
    o.median_delay_sec,
    o.p90_delay_sec,
    e.ewt_sec,
    cast(g.n_bunched as double) / nullif(g.n_rated, 0) as bunching_pct,
    cast(g.n_big_gap as double) / nullif(g.n_rated, 0) as big_gap_pct,
    cast(d.trips_cancelled as double) / nullif(d.trips_scheduled, 0) as cancel_pct,
    d.completeness_pct,
    d.trips_scheduled as scheduled_trips,
    d.trips_observed as observed_trips
from base b
left join otp o
  on o.city_key = b.city_key and o.service_date = b.service_date
 and o.route_id = b.route_id and o.direction_id = b.direction_id
left join route_ewt e
  on e.city_key = b.city_key and e.service_date = b.service_date
 and e.route_id = b.route_id and e.direction_id = b.direction_id
left join gap_shares g
  on g.city_key = b.city_key and g.service_date = b.service_date
 and g.route_id = b.route_id and g.direction_id = b.direction_id
left join {{ ref('fct_service_delivery_daily') }} d
  on d.city_key = b.city_key and d.service_date = b.service_date
 and d.route_id = b.route_id
left join {{ ref('stg_gtfs__routes') }} r
  on r.city_key = b.city_key and r.route_id = b.route_id
