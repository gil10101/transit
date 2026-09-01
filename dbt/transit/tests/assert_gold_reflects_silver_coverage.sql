-- Gold must account for the trips silver actually holds.
--
-- [rev 2026-08-30] The gap this closes: int_stop_events_finalized is incremental
-- with a {{ var('lookback_hours') }}h lookback, so a day computed while an upstream
-- input was briefly broken is silently frozen wrong the moment it ages out of the
-- reprocessing window. That is not hypothetical — zurich 2026-08-27 sits at 66% of
-- its silver trips (25,094 of 37,836) because the schedule join was failing during
-- the checkpoint-replay incident; the neighbouring days computed the same way
-- self-healed once the input recovered and landed at 98-99%. Nothing caught it: the
-- 50% completeness floor passes at 66%, so gold was wrong and green for three days.
--
-- The threshold is measured, not guessed. Across 2026-08-25..29 every healthy
-- city-day landed between 0.950 and 0.99 (silver rows legitimately lost to
-- unmatched trips, stale revisions and missing stop_ids); the one broken day sat at
-- 0.663. A 0.90 floor is 5 points under the healthy minimum and 24 above the failure.
--
-- Scope is deliberately the REPAIRABLE window: only closed days still inside the
-- lookback, where the next chain run can actually rewrite them. A wider window would
-- pin the test permanently red on history no run can reach, which teaches everyone to
-- ignore it. Days that age out already-wrong are exactly what this prevents.

{% set coverage_floor = 0.90 %}

with silver as (

    -- [rev 2026-09-01] SCHEDULABLE trips only — those with a schedule row to be
    -- finalized against. Counting every published trip asked an unanswerable
    -- question: zurich 2026-08-31 read 0.68 because the Swiss feed declared
    -- 12,711 trips with start_date=20260831 that its OWN static assigns to
    -- Saturday/Sunday services, so no version of the schedule places them on a
    -- Monday. A delay-only city cannot finalize a trip with no schedule, so
    -- those trips were never ours to lose, and this test was repeating the very
    -- mistake it was written to replace: blaming us for the agency's silence.
    --
    -- Crucially this keeps the teeth. The window is evaluated at test time, so
    -- the 2026-08-27 freeze this test was built for STILL fires: the schedule
    -- rows existed by then and gold was missing the trips anyway. Only trips
    -- that genuinely have nowhere to land drop out.
    select p.city_key, p.service_date, count(distinct p.trip_uid) as silver_trips
    from {{ ref('stg_gtfsrt__trip_updates') }} p
    where exists (
        select 1
        from {{ ref('int_gtfs_scheduled_stop_times') }} s
        where s.city_key = p.city_key
          and s.service_date = p.service_date
          and s.trip_id = p.trip_id
    )
    group by p.city_key, p.service_date

),

gold as (

    select city_key, service_date, count(distinct trip_uid) as gold_trips
    from {{ ref('fct_stop_events') }}
    group by city_key, service_date

),

judged as (

    select
        s.city_key,
        s.service_date,
        s.silver_trips,
        coalesce(g.gold_trips, 0) as gold_trips,
        coalesce(g.gold_trips, 0) / nullif(s.silver_trips, 0) as coverage_ratio
    from silver s
    left join gold g
      on g.city_key = s.city_key
     and g.service_date = s.service_date
    join {{ ref('dim_city') }} c
      on c.city_key = s.city_key
    left join {{ ref('incident_days') }} i
      on i.city_key = s.city_key
     and i.service_date = s.service_date
    where
        -- closed, by the same local-time rule the delivery mart uses
        cast({{ to_local('current_timestamp', 'c.iana_tz') }} - interval '3 hour' as date)
            > s.service_date
        -- still inside the window a chain run can rewrite
        and s.service_date >= {{ dbt.dateadd('day',
              "-cast(ceil(" ~ var('lookback_hours') ~ " / 24.0) as int)", 'current_date') }}
        -- our own outages are declared in the seed, not re-litigated here
        and i.city_key is null
        -- a day with almost no silver is a feed problem the freshness tripwire owns
        and s.silver_trips >= 100

)

select *
from judged
where coverage_ratio < {{ coverage_floor }}
