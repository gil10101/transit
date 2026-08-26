-- SCD2 route dimension, derived ENTIRELY from versioned silver static — not a dbt
-- snapshot. docs/02 sketched "SCD2 via dbt snapshot"; amended (same change) because
-- silver already keeps every static version as its own partition, which makes
-- snapshot state strictly worse on every axis that matters here:
--   * rebuildable: drop this table and the full history comes back from silver.
--     A snapshot table IS the history — lose it and it is gone.
--   * honest valid_from: the version's download date (parsed from gtfs_version_id),
--     not whenever a snapshot job happened to run after the fact.
--   * no schedule coupling: history accrues even if dbt does not run for a week.
--
-- Grain: one row per (city, route_id, validity interval). A new interval starts
-- when a route first appears, or when any tracked attribute changes between
-- versions. Re-downloads of an unchanged zip mint new version_ids (date part
-- changes, sha stays) — attribute comparison collapses those into one interval.
-- A route ABSENT from a version has its interval closed at that version's date;
-- if it reappears later it gets a fresh interval (real pattern: seasonal routes).
--
-- valid_to is exclusive; NULL means current. Point-in-time join:
--   on service_date >= valid_from and (valid_to is null or service_date < valid_to)

{{ config(materialized='table') }}

with routes as (

    select * from {{ ref('stg_gtfs__route_versions') }}

),

versions as (

    select * from {{ ref('int_gtfs_versions') }}

),

-- every (version, route ever seen in the city): the grid on which presence and
-- change are judged. Without the grid a route missing from a version is invisible
-- and its interval would silently span the absence.
grid as (

    select
        v.city_key,
        v.gtfs_version_id,
        v.version_date,
        v.version_seq,
        r0.route_id
    from versions v
    join (select distinct city_key, route_id from routes) r0
      on r0.city_key = v.city_key

),

states as (

    select
        g.city_key,
        g.route_id,
        g.gtfs_version_id,
        g.version_date,
        g.version_seq,
        r.agency_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type,
        r.mode,
        r.route_color,
        -- Absence keys on the JOIN, never on the hash: with every attribute
        -- coalesced, md5(concat_ws(...)) is non-NULL even when the route row is
        -- missing — an absent version would hash to the "all attributes empty"
        -- state and produce a ghost dim row with NULL attributes. Same defect
        -- class as the trip_uid concat_ws collapse; caught in prod by
        -- not_null_dim_route_mode (16 rows) before any fact joined it.
        case
            when r.route_id is null then '__absent__'
            else md5(concat_ws('|',
                coalesce(r.agency_id, ''),
                coalesce(r.route_short_name, ''),
                coalesce(r.route_long_name, ''),
                coalesce(cast(r.route_type as varchar), ''),
                coalesce(r.route_color, '')
            ))
        end as state
    from grid g
    left join routes r
      on r.city_key = g.city_key
     and r.gtfs_version_id = g.gtfs_version_id
     and r.route_id = g.route_id

),

-- a transition is any version where the route's state differs from the previous
-- version: first appearance, attribute change, disappearance, reappearance
transitions as (

    select *
    from (
        select
            s.*,
            lag(s.state) over (
                partition by s.city_key, s.route_id order by s.version_seq
            ) as prev_state
        from states s
    ) x
    where prev_state is null or state <> prev_state

),

-- each transition's validity runs until the NEXT transition (of any kind — an
-- absence transition closes the previous interval without producing a row)
intervals as (

    select
        t.*,
        lead(t.version_date) over (
            partition by t.city_key, t.route_id order by t.version_seq
        ) as valid_to
    from transitions t

),

-- indicative flag only (docs/02): the SCORING classification lives at
-- (route x direction x daypart x service_date) in int_service_frequency, because
-- a route can be frequent at peak and timetabled at night. Any schedule-derived
-- frequent peak slice on any observed date sets it — a dashboard filter, not
-- history, so it stamps the same value on every interval of the route.
peak_frequent as (

    select distinct city_key, route_id
    from {{ ref('int_service_frequency') }}
    where daypart in ('am_peak', 'pm_peak')
      and is_frequent

)

select
    md5(concat_ws('|', i.city_key, i.route_id, cast(i.version_date as varchar))) as route_key,
    i.city_key,
    i.route_id,
    i.agency_id,
    i.route_short_name,
    i.route_long_name,
    i.route_type,
    i.mode,
    i.route_color,
    pf.route_id is not null as is_frequent_service_typical,
    i.version_date as valid_from,
    i.valid_to,
    i.valid_to is null as is_current,
    i.gtfs_version_id
from intervals i
left join peak_frequent pf
       on pf.city_key = i.city_key and pf.route_id = i.route_id
where i.state <> '__absent__'
