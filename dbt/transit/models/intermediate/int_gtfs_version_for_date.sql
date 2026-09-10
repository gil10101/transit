-- Which static GTFS version was LIVE on each service date: (city, date) -> version.
--
-- The bug this exists to kill (docs/08, 2026-09-06). Every schedule-side staging
-- model pinned `max(gtfs_version_id)` per city, and fct_service_delivery_daily is
-- a full-refresh table, so each chain recomputed ALL of history against whatever
-- static happened to be newest. Agencies whose calendar.txt begins at the
-- publication date therefore deleted their own past every Sunday: TTC started a
-- board period on 2026-09-06 and Toronto's fifteen days of events became
-- unjudgeable in a single refresh. Measured calendar lookback per publication —
-- how far back each agency's calendar reaches — makes the split obvious:
--
--   sf 1902 days · zurich 266 · nyc 103 · dc 77   -> unaffected, always covered
--   boston 7-13 · helsinki 1-5                    -> ceiling BELOW 20 judged days
--   toronto 0-39 (0 on a new board period)        -> loses everything at rotation
--
-- Helsinki and Boston can never reach 20 judged days while the newest version is
-- the only one consulted; with this model they reach it on their event history.
--
-- Rule: the newest version whose download date is at or before the service date —
-- literally "what we were running that day". Dates earlier than our first pull
-- fall back to the earliest version we hold, which is the closest honest answer
-- for backfilled days; version_date comes from the id itself (int_gtfs_versions),
-- so this needs no snapshot state that could be lost.
--
-- Deliberately NOT applied to the delay path. int_gtfs_scheduled_stop_times and
-- the trip matchers stay pinned to the newest version because they only ever
-- serve the 72h reprocessing window, where the newest static always covers the
-- date anyway — and re-scanning every version of stop_times (the largest table in
-- the schedule layer) on every chain would cost far more credit than the problem
-- is worth. Only the DENOMINATOR path needs history, and calendar and trips are
-- small enough to carry every version cheaply.

with observed_dates as (

    select distinct city_key, service_date
    from {{ ref('stg_gtfsrt__trip_updates') }}

),

versions as (

    select city_key, gtfs_version_id, version_date
    from {{ ref('int_gtfs_versions') }}

),

ranked as (

    select
        d.city_key,
        d.service_date,
        v.gtfs_version_id,
        v.version_date,
        row_number() over (
            partition by d.city_key, d.service_date
            -- live-on-the-day first: versions at or before the date, newest of
            -- those; only if none exists do we reach forward to the earliest
            order by case when v.version_date <= d.service_date then 0 else 1 end,
                     case when v.version_date <= d.service_date
                          then {{ dbt.datediff('v.version_date', 'd.service_date', 'day') }}
                          else {{ dbt.datediff('d.service_date', 'v.version_date', 'day') }}
                     end,
                     v.gtfs_version_id
        ) as rn
    from observed_dates d
    join versions v on v.city_key = d.city_key

)

select
    city_key,
    service_date,
    gtfs_version_id,
    version_date
from ranked
where rn = 1
