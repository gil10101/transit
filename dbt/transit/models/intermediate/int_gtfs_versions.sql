-- Registry of every static GTFS version ever loaded, one row per (city, version).
--
-- gtfs_version_id = "<city>-<YYYYMMDD download date>-<sha256[:8] of zip>"
-- (spark_jobs/gtfs_static_parse.py), so the version's effective date is parseable
-- from the id itself and the whole SCD2 layer derives from silver alone — no dbt
-- snapshot state that could be lost and cannot be rebuilt. Re-downloading an
-- UNCHANGED zip on a later day mints a new version_id (date changes, sha does not);
-- the dims collapse those into one validity interval by comparing attributes.
--
-- Routes is the canonical registry table: every GTFS zip must contain routes.txt,
-- so its version set is the version set.

with versions as (

    select distinct
        city as city_key,
        gtfs_version_id
    from {{ source('silver', 'gtfs_static_routes') }}

)

select
    city_key,
    gtfs_version_id,
    -- "<city>-YYYYMMDD-sha8" -> the YYYYMMDD part; split_part is 1-indexed on both
    -- duckdb and Snowflake, and no city_key contains a hyphen
    cast(
        substr(split_part(gtfs_version_id, '-', 2), 1, 4) || '-'
        || substr(split_part(gtfs_version_id, '-', 2), 5, 2) || '-'
        || substr(split_part(gtfs_version_id, '-', 2), 7, 2)
        as date
    ) as version_date,
    row_number() over (
        partition by city_key
        order by split_part(gtfs_version_id, '-', 2), gtfs_version_id
    ) as version_seq
from versions
