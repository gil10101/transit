-- P6: the headline. One composite reliability score per city, 0-100, over that
-- city's full judged window.
--
-- Grain: (city_key) — one row per city per methodology version, covering
-- [window_start, window_end] = the span of its eligible route-days. Full
-- refresh each run — the score is a function of the window's data, so it must
-- be recomputed rather than accumulated.
--
-- [rev 2026-09-10 — WINDOW grain replaces calendar-month grain; Jake approved]
-- The month grain required var('scorecard_min_judged_days') judged days inside
-- ONE calendar month. With metrics_from spanning 23 Aug – 4 Sep and the
-- project closing 17 Sep, no month could ever hold 20 judged days even once
-- 20+ genuinely judged days existed — August ends too early, September gets
-- cut off. The month boundary was measuring the calendar, not the evidence.
-- The 20-day floor itself is UNCHANGED (this is a grain change, not a
-- threshold change): a city still needs 20 closed judged days, they just
-- accumulate across its whole judged history. The retired monthly relation
-- (fct_city_scorecard_monthly) stays in the warehouse untouched until
-- teardown; nothing builds or reads it.
--
-- THIS IS THE MOST QUOTABLE NUMBER THE PROJECT PRODUCES, which drives four decisions:
--
-- 1. IT REFUSES TO EXIST RATHER THAN BE THIN. A city needs var('scorecard_min_judged_days')
--    closed, judged service days in its window or it produces no row. An empty scorecard
--    is the correct output for thin history, not a gap to work around. A confident-looking
--    0-100 built on a sliver is worse than no number, because nobody can see the sliver
--    in "68".
--
-- 2. IT IS VERSIONED, NEVER SILENTLY REWRITTEN. Every row carries methodology_version and
--    the four weights that produced it. Changing weights makes v2 rows; it does not
--    reinterpret history (docs/02 §fct_city_scorecard).
--
-- 3. IT SHOWS ITS WORKING. score_0_100 is useless without the sub-scores, the inputs they
--    came from, and how much was excluded — so all of it is on the row. A reader must be
--    able to see WHY a city scored what it did without re-deriving anything.
--
-- 4. MISSING EVIDENCE IS NOT A SCORE. A sub-score whose input is absent stays NULL and its
--    weight is renormalised over the components that exist. The first version of this
--    model coalesced missing OTP to 0 (scoring absence as total failure) and missing EWT
--    to 0 excess wait (scoring absence as perfection) — both wrong the same way round:
--    they turned "we didn't measure this" into a measurement.
--
-- GRAIN DISCIPLINE (the bug that forced the rewrite): fct_route_reliability_daily is
-- direction-grain and repeats route-level delivery values on every direction row, so
-- summing its trip counts double-counts two-direction routes (triple where an
-- unmatched direction folds to -1). Delivery quantities are therefore taken from
-- route-grain fct_service_delivery_daily directly, and reliability ratios are weighted
-- by their own evidence counts (banded_events, rated_gaps, ewt_gap_count), never by
-- trip counts that live at a different grain.
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
-- ROUTE grain — one row per (city, route, service_date).
eligible_route_days as (

    select
        d.city_key,
        d.service_date,
        d.route_id,
        d.trips_scheduled,
        d.trips_observed,
        d.trips_cancelled,
        d.completeness_pct
    from {{ ref('fct_service_delivery_daily') }} d
    where d.service_day_closed
      and d.service_date >= d.metrics_from
      and coalesce(d.completeness_pct, 0) >= {{ var('scorecard_min_completeness') }}

),

-- what we deliberately dropped, so the row can admit it rather than hide it
excluded as (

    select
        d.city_key,
        count(*) as excluded_route_days
    from {{ ref('fct_service_delivery_daily') }} d
    where d.service_day_closed
      and d.service_date >= d.metrics_from
      and coalesce(d.completeness_pct, 0) < {{ var('scorecard_min_completeness') }}
    group by 1

),

-- Delivery quantities at their native route grain: real trip totals, and
-- cancel/completeness averaged with each route-day weighted by the service it
-- actually scheduled — a 200-trip trunk route and a 12-trip feeder must not
-- count equally toward a city's score.
delivery as (

    select
        city_key,
        count(distinct service_date) as judged_days,
        min(service_date)            as window_start,
        max(service_date)            as window_end,
        count(*)                     as route_days,
        sum(trips_scheduled)         as scheduled_trips,
        sum(trips_observed)          as observed_trips,
        cast(sum(trips_cancelled) as double) / nullif(sum(trips_scheduled), 0) as cancel_pct,
        sum(completeness_pct * trips_scheduled) / nullif(sum(trips_scheduled), 0) as completeness_pct
    from eligible_route_days
    group by 1

),

-- Reliability ratios weighted by their own evidence: OTP by banded events,
-- bunching by rated gaps, EWT by the gap count behind each route-day's figure.
-- Direction rows join to eligible route-days, so a route excluded for
-- completeness contributes no reliability either.
reliability as (

    select
        r.city_key,
        sum(r.otp_pct * r.banded_events)
            / nullif(sum(case when r.otp_pct      is not null then r.banded_events end), 0) as otp_pct,
        sum(r.bunching_pct * r.rated_gaps)
            / nullif(sum(case when r.bunching_pct is not null then r.rated_gaps end), 0) as bunching_pct,
        sum(r.ewt_sec * r.ewt_gap_count)
            / nullif(sum(case when r.ewt_sec      is not null then r.ewt_gap_count end), 0) as ewt_sec
    from {{ ref('fct_route_reliability_daily') }} r
    join eligible_route_days e
      on e.city_key = r.city_key
     and e.route_id = r.route_id
     and e.service_date = r.service_date
    group by 1

),

-- How much of the city's observed service runs frequently enough for EWT to
-- mean anything. Published because it is the main reason two cities' scores are
-- not directly comparable: a mostly-timetabled network is being judged on a
-- different mix from a turn-up-and-go one.
frequent_share as (

    select
        e.city_key,
        sum(case when f.has_ewt then e.trips_observed else 0 end)
            / nullif(sum(cast(e.trips_observed as double)), 0) as frequent_service_share
    from eligible_route_days e
    left join (
        select city_key, route_id, service_date,
               max(case when ewt_sec is not null then true else false end) as has_ewt
        from {{ ref('fct_route_reliability_daily') }}
        group by 1, 2, 3
    ) f
      on f.city_key = e.city_key
     and f.route_id = e.route_id
     and f.service_date = e.service_date
    group by 1

),

scored as (

    select
        d.city_key,
        d.judged_days,
        d.window_start,
        d.window_end,
        d.route_days,
        d.scheduled_trips,
        d.observed_trips,
        d.cancel_pct,
        d.completeness_pct,
        r.otp_pct,
        r.bunching_pct,
        r.ewt_sec,
        fs.frequent_service_share,
        w.w_wait, w.w_otp, w.w_cancel, w.w_bunch,
        -- Sub-scores, each 0-100 and each "higher is better" so the composite reads the
        -- obvious way — and each NULL when its input was never measured. The EWT anchor
        -- is 600s: a rider waiting ten minutes longer than the timetable promised is a
        -- total failure of frequent service, and anything beyond that is not more
        -- informative. Stated here rather than buried in a var because it is a
        -- judgement, not a tuning knob.
        case when r.ewt_sec is not null
             then 100.0 * least(1.0, greatest(0.0, 1.0 - r.ewt_sec / 600.0)) end as s_wait,
        case when r.otp_pct is not null
             then 100.0 * least(1.0, greatest(0.0, r.otp_pct)) end               as s_otp,
        case when d.cancel_pct is not null
             then 100.0 * least(1.0, greatest(0.0, 1.0 - d.cancel_pct)) end      as s_cancel,
        case when r.bunching_pct is not null
             then 100.0 * least(1.0, greatest(0.0, 1.0 - r.bunching_pct)) end    as s_bunch
    from delivery d
    left join reliability r
           on r.city_key = d.city_key
    left join frequent_share fs
           on fs.city_key = d.city_key
    cross join weights w

)

select
    md5(concat_ws('|', s.city_key, '{{ var("methodology_version") }}')) as scorecard_key,
    s.city_key,
    s.window_start,
    s.window_end,
    -- Composite over the components that exist, weights renormalised so absence
    -- neither punishes nor flatters. Which components were present is visible in
    -- the sub-score columns: a NULL sub-score took no part in this number.
    round(
        (
            coalesce(s.w_wait   * s.s_wait,   0)
          + coalesce(s.w_otp    * s.s_otp,    0)
          + coalesce(s.w_cancel * s.s_cancel, 0)
          + coalesce(s.w_bunch  * s.s_bunch,  0)
        ) / nullif(
            case when s.s_wait   is not null then s.w_wait   else 0 end
          + case when s.s_otp    is not null then s.w_otp    else 0 end
          + case when s.s_cancel is not null then s.w_cancel else 0 end
          + case when s.s_bunch  is not null then s.w_bunch  else 0 end
        , 0)
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
       on e.city_key = s.city_key
-- the guard: too little history means no score, not a quiet one
where s.judged_days >= {{ var('scorecard_min_judged_days') }}
