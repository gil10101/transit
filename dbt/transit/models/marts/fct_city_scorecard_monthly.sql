-- P6: the headline. One composite reliability score per city per month, 0-100.
--
-- Grain: (city_key, month). Full refresh each run — the score is a function of the
-- month's data, so it must be recomputed rather than accumulated.
--
-- THIS IS THE MOST QUOTABLE NUMBER THE PROJECT PRODUCES, which drives three decisions:
--
-- 1. IT REFUSES TO EXIST RATHER THAN BE THIN. A city needs var('scorecard_min_judged_days')
--    closed, judged service days in the month or it produces no row. On 2026-08-26 that
--    means NO city is scored, because there are four days of data. That is the correct
--    output for four days, not a gap to work around. A confident-looking 0-100 built on a
--    sliver is worse than no number, because nobody can see the sliver in "68".
--
-- 2. IT IS VERSIONED, NEVER SILENTLY REWRITTEN. Every row carries methodology_version and
--    the four weights that produced it. Changing weights makes v2 rows; it does not
--    reinterpret history (docs/02 §fct_city_scorecard_monthly).
--
-- 3. IT SHOWS ITS WORKING. score_0_100 is useless without the sub-scores, the inputs they
--    came from, and how much was excluded — so all of it is on the row. A reader must be
--    able to see WHY a city scored what it did without re-deriving anything.
--
-- Weights (var score_weights): wait .35, otp .30, cancel .20, bunch .15. Wait dominates
-- deliberately — on frequent service, which is where most riders are, waiting longer than
-- promised IS the experience of unreliability.

{{ config(materialized='table') }}

with weights as (
    select
        {{ var('score_weights')['wait'] }}   as w_wait,
        {{ var('score_weights')['otp'] }}    as w_otp,
        {{ var('score_weights')['cancel'] }} as w_cancel,
        {{ var('score_weights')['bunch'] }}  as w_bunch
),

-- Only route-days we are prepared to stand behind: the service day finished in the
-- city's own local time, it is at or after that city's metrics_from, and completeness
-- clears the floor. A route-day below the floor is evidence about our feed rather than
-- about the city's service, so including it would score our own coverage.
eligible as (

    select
        r.city_key,
        date_trunc('month', r.service_date) as month,
        r.service_date,
        r.route_id,
        r.direction_id,
        r.mode,
        r.otp_pct,
        r.ewt_sec,
        r.bunching_pct,
        r.cancel_pct,
        r.completeness_pct,
        r.scheduled_trips,
        r.observed_trips
    from {{ ref('fct_route_reliability_daily') }} r
    join {{ ref('fct_service_delivery_daily') }} d
      on d.city_key = r.city_key
     and d.route_id = r.route_id
     and d.service_date = r.service_date
    where d.service_day_closed
      and d.service_date >= d.metrics_from
      and coalesce(r.completeness_pct, 0) >= {{ var('scorecard_min_completeness') }}

),

-- what we deliberately dropped, so the row can admit it rather than hide it
excluded as (

    select
        d.city_key,
        date_trunc('month', d.service_date) as month,
        count(*) as excluded_route_days
    from {{ ref('fct_service_delivery_daily') }} d
    where d.service_day_closed
      and d.service_date >= d.metrics_from
      and coalesce(d.completeness_pct, 0) < {{ var('scorecard_min_completeness') }}
    group by 1, 2

),

per_city as (

    select
        city_key,
        month,
        count(distinct service_date)                  as judged_days,
        count(*)                                      as route_days,
        sum(scheduled_trips)                          as scheduled_trips,
        sum(observed_trips)                           as observed_trips,
        -- weight each route-day by the service it actually represents: a 200-trip trunk
        -- route and a 12-trip feeder must not count equally toward a city's score
        sum(otp_pct * observed_trips)      / nullif(sum(case when otp_pct      is not null then observed_trips end), 0) as otp_pct,
        sum(bunching_pct * observed_trips) / nullif(sum(case when bunching_pct is not null then observed_trips end), 0) as bunching_pct,
        sum(cancel_pct * scheduled_trips)  / nullif(sum(case when cancel_pct   is not null then scheduled_trips end), 0) as cancel_pct,
        sum(completeness_pct * scheduled_trips) / nullif(sum(case when completeness_pct is not null then scheduled_trips end), 0) as completeness_pct,
        -- ewt exists only on frequent slices; weighting it by observed trips keeps a
        -- one-off frequent route from dominating a city with mostly timetabled service
        sum(ewt_sec * observed_trips)      / nullif(sum(case when ewt_sec      is not null then observed_trips end), 0) as ewt_sec,
        -- how much of the city's service is frequent enough for EWT to mean anything.
        -- Published because it is the main reason two cities' scores are not directly
        -- comparable: a mostly-timetabled network is being judged on a different mix.
        sum(case when ewt_sec is not null then observed_trips else 0 end)
            / nullif(sum(observed_trips), 0) as frequent_service_share
    from eligible
    group by 1, 2

),

scored as (

    select
        p.*,
        w.w_wait, w.w_otp, w.w_cancel, w.w_bunch,
        -- Sub-scores, each 0-100 and each "higher is better" so the composite reads the
        -- obvious way. The EWT anchor is 600s: a rider waiting ten minutes longer than
        -- the timetable promised is a total failure of frequent service, and anything
        -- beyond that is not more informative. Stated here rather than buried in a var
        -- because it is a judgement, not a tuning knob.
        100.0 * least(1.0, greatest(0.0, 1.0 - coalesce(p.ewt_sec, 0) / 600.0)) as s_wait,
        100.0 * least(1.0, greatest(0.0, coalesce(p.otp_pct, 0)))              as s_otp,
        100.0 * least(1.0, greatest(0.0, 1.0 - coalesce(p.cancel_pct, 0)))     as s_cancel,
        100.0 * least(1.0, greatest(0.0, 1.0 - coalesce(p.bunching_pct, 0)))   as s_bunch
    from per_city p
    cross join weights w

)

select
    md5(concat_ws('|', s.city_key, cast(s.month as string), '{{ var("methodology_version") }}'))
        as scorecard_key,
    s.city_key,
    s.month,
    round(
        s.w_wait * s.s_wait + s.w_otp * s.s_otp
        + s.w_cancel * s.s_cancel + s.w_bunch * s.s_bunch
    , 1) as score_0_100,
    round(s.s_wait, 1)   as s_wait,
    round(s.s_otp, 1)    as s_otp,
    round(s.s_cancel, 1) as s_cancel,
    round(s.s_bunch, 1)  as s_bunch,
    -- the inputs, so the score can be audited without re-deriving it
    round(s.ewt_sec)              as ewt_sec,
    round(s.otp_pct, 4)           as otp_pct,
    round(s.cancel_pct, 4)        as cancel_pct,
    round(s.bunching_pct, 4)      as bunching_pct,
    round(s.completeness_pct, 4)  as completeness_pct,
    round(s.frequent_service_share, 4) as frequent_service_share,
    s.judged_days,
    s.route_days,
    s.scheduled_trips,
    s.observed_trips,
    coalesce(e.excluded_route_days, 0) as excluded_route_days,
    -- the weights that produced THIS row, so a later weight change is visible in the
    -- data rather than only in git history
    s.w_wait, s.w_otp, s.w_cancel, s.w_bunch,
    '{{ var("methodology_version") }}' as methodology_version
from scored s
left join excluded e
       on e.city_key = s.city_key and e.month = s.month
-- the guard: too little history means no score, not a quiet one
where s.judged_days >= {{ var('scorecard_min_judged_days') }}
