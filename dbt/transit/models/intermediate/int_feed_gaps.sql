-- Stretches longer than var('feed_gap_min') in which a city's feed delivered nothing
-- (int_feed_heartbeat). [gap_start_utc, gap_end_utc) runs from the end of the last
-- minute with a fetch to the start of the next one. An arrival strictly inside is one
-- nobody observed (fct_stop_events.feed_gap_flag).
--
-- A few gaps are quiet rather than dark — overnight Zurich, when no allow-listed trip is
-- in the national feed. They cost nothing: an arrival inside one means its own trip had
-- dropped out of the feed too, so it is unobserved all the same.

{{ config(materialized='view') }}

with pulse as (
    select
        city_key,
        fetch_minute,
        lead(fetch_minute) over (partition by city_key order by fetch_minute) as next_minute
    from {{ ref('int_feed_heartbeat') }}
)

select
    city_key,
    {{ dbt.dateadd('minute', 1, 'fetch_minute') }} as gap_start_utc,
    next_minute as gap_end_utc,
    {{ dbt.datediff('fetch_minute', 'next_minute', 'minute') }} as gap_min
from pulse
where {{ dbt.datediff('fetch_minute', 'next_minute', 'minute') }} > {{ var('feed_gap_min') }}
