-- SCD2 stop dimension, derived from versioned silver static — same mechanism and
-- same docs/02 amendment as dim_route (rebuildable from silver; snapshot state is
-- not). One row per (city, stop_id, validity interval); valid_to exclusive, NULL
-- means current.
--
-- National-feed cut: Zurich's stops.txt is every stop in Switzerland. A stop
-- counts as present in a version only if some allow-listed trip serves it there
-- (int_stops_served), or it is the parent station of one that is — parent rows
-- carry the station names the dashboard shows. Other cities keep their full
-- stops.txt including never-served station records, which downstream joins
-- reference via parent_station.
--
-- is_timepoint_default (docs/02 sketch) is DEFERRED, amended in the same change:
-- it exists to drive the early-departure rule, which itself is not implemented yet
-- (fct_stop_events header), and computing bus timepoints per version means walking
-- every version of stop_times. Add it when the rule lands, not before.
--
-- geo / h3_r8 / h3_r9 are Snowflake-only (H3_LATLNG_TO_CELL_STRING at build time
-- makes the dashboard hexmap a plain GROUP BY); duckdb dev pins NULL.

{{ config(materialized='table') }}

with stops as (

    select * from {{ ref('stg_gtfs__stop_versions') }}

),

versions as (

    select * from {{ ref('int_gtfs_versions') }}

),

served as (

    select city_key, gtfs_version_id, stop_id
    from {{ ref('int_stops_served') }}

),

-- parents of served stops count as served: they carry the station-level names
parents as (

    select distinct
        sv.city_key,
        sv.gtfs_version_id,
        sv.parent_station as stop_id
    from stops sv
    join served s
      on s.city_key = sv.city_key
     and s.gtfs_version_id = sv.gtfs_version_id
     and s.stop_id = sv.stop_id
    where sv.parent_station is not null

),

allowed as (

    select * from served
    union
    select * from parents

),

grid as (

    select
        v.city_key,
        v.gtfs_version_id,
        v.version_date,
        v.version_seq,
        s0.stop_id
    from versions v
    join (select distinct city_key, stop_id from stops) s0
      on s0.city_key = v.city_key

),

states as (

    select
        g.city_key,
        g.stop_id,
        g.gtfs_version_id,
        g.version_date,
        g.version_seq,
        s.stop_name,
        s.stop_lat,
        s.stop_lon,
        s.parent_station,
        s.location_type,
        -- Absence keys on the JOINs, never on the hash — with every attribute
        -- coalesced the md5 is non-NULL even for a missing stop row (same ghost-
        -- row defect dim_route had; see its states CTE).
        case
            -- national-city stop not serving (nor parenting) any allow-listed
            -- trip in this version: treat as absent from the version.
            -- City list mirrors macros/national_allowlist.sql.
            when g.city_key = 'zurich' and a.stop_id is null then '__absent__'
            when s.stop_id is null then '__absent__'
            else md5(concat_ws('|',
                coalesce(s.stop_name, ''),
                coalesce(cast(round(s.stop_lat, 6) as varchar), ''),
                coalesce(cast(round(s.stop_lon, 6) as varchar), ''),
                coalesce(s.parent_station, ''),
                coalesce(cast(s.location_type as varchar), '')
            ))
        end as state
    from grid g
    left join stops s
      on s.city_key = g.city_key
     and s.gtfs_version_id = g.gtfs_version_id
     and s.stop_id = g.stop_id
    left join allowed a
      on a.city_key = g.city_key
     and a.gtfs_version_id = g.gtfs_version_id
     and a.stop_id = g.stop_id

),

transitions as (

    select *
    from (
        select
            s.*,
            lag(s.state) over (
                partition by s.city_key, s.stop_id order by s.version_seq
            ) as prev_state
        from states s
    ) x
    where prev_state is null or state <> prev_state

),

intervals as (

    select
        t.*,
        lead(t.version_date) over (
            partition by t.city_key, t.stop_id order by t.version_seq
        ) as valid_to
    from transitions t

)

select
    md5(concat_ws('|', i.city_key, i.stop_id, cast(i.version_date as varchar))) as stop_key,
    i.city_key,
    i.stop_id,
    i.stop_name,
    i.stop_lat as lat,
    i.stop_lon as lon,
    i.parent_station,
    i.location_type,
    {% if target.type == 'snowflake' %}
    case when i.stop_lat is not null and i.stop_lon is not null
         then st_makepoint(i.stop_lon, i.stop_lat) end as geo,
    case when i.stop_lat is not null and i.stop_lon is not null
         then h3_latlng_to_cell_string(i.stop_lat, i.stop_lon, 8) end as h3_r8,
    case when i.stop_lat is not null and i.stop_lon is not null
         then h3_latlng_to_cell_string(i.stop_lat, i.stop_lon, 9) end as h3_r9,
    {% else %}
    cast(null as varchar) as geo,
    cast(null as varchar) as h3_r8,
    cast(null as varchar) as h3_r9,
    {% endif %}
    i.version_date as valid_from,
    i.valid_to,
    i.valid_to is null as is_current,
    i.gtfs_version_id
from intervals i
where i.state <> '__absent__'
