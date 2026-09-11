-- One row per (city, minute) in which the city's realtime feed delivered anything to
-- silver: the feed's pulse. A stretch of minutes with no row is a stretch in which we
-- could not see the city at all — an agency outage, a dead poller, a dropped drain.
--
-- [2026-09-11] Exists for feed_gap_flag in fct_stop_events (docs/08, 2026-09-10).
-- When a feed goes dark, the finalizer still turns each in-flight trip's LAST
-- prediction into an "arrival" at every stop the vehicle reached in the dark — 23,474
-- scored Toronto events on 2026-09-10 were frozen pre-outage predictions. The
-- prediction-lead cap cannot catch them (their lead is minutes, and NYC's normal lead
-- runs to 45), but the feed's own silence can: nothing was fetched, so nothing was seen.
--
-- Built from the silver FETCH log, not from event timestamps: event first/last_seen
-- thins out overnight and read quiet-but-alive hours as outages (~400 false flags a
-- day in Tokyo alone when measured that way). Minute grain keeps it tiny (8 cities x
-- 1,440 a day) so every chain can scan it whole; the silver scan itself is bounded to
-- the lookback window, which also picks up any backfill replayed into that window.

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['city_key', 'fetch_minute']
) }}

{% set window %}
    {% if is_incremental() %}
    where fetched_at >= {{ dbt.dateadd('hour', -var('lookback_hours'), 'current_timestamp') }}
    {% endif %}
{% endset %}

with fetches as (
    select city_key, fetched_at from {{ ref('stg_gtfsrt__trip_updates') }} {{ window }}
    union all
    select city_key, fetched_at from {{ ref('stg_odpt__trains') }} {{ window }}
)

select distinct
    city_key,
    {{ dbt.date_trunc('minute', 'fetched_at') }} as fetch_minute
from fetches
