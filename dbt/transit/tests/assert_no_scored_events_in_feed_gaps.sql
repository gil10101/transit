-- Contract: an arrival that landed while its city's feed was dark (int_feed_gaps) is
-- unobserved, so it must never carry an otp_band. Pins the 2026-09-10 fix: TTC's
-- outage left 23,474 frozen pre-outage predictions scored as arrivals (docs/08).
-- Scoped to the repairable window like assert_delays_are_plausible.
select e.city_key, e.service_date, e.trip_uid, e.stop_sequence, e.actual_arr_ts_utc
from {{ ref('fct_stop_events') }} e
join {{ ref('int_feed_gaps') }} g
  on g.city_key = e.city_key
 and e.actual_arr_ts_utc > g.gap_start_utc
 and e.actual_arr_ts_utc < g.gap_end_utc
where e.otp_band is not null
  and e.service_date >= {{ dbt.dateadd('day',
        "-cast(ceil(" ~ var('lookback_hours') ~ " / 24.0) as int)", 'current_date') }}
