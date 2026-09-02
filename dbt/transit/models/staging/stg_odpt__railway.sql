-- Latest ODPT railway dump (tokyo), one row per (railway, station in order).
-- station_index is the canonical along-line position used by transition-based
-- passage detection (docs/01 §C.1: never a toStation-null test).

with latest as (
    select city, max(odpt_version_id) as odpt_version_id
    from {{ source('silver', 'odpt_railway') }}
    group by 1
)

select
    r.city as city_key,
    r.odpt_version_id,
    r.railway_urn,
    r.operator,
    r.title_ja,
    r.title_en,
    r.line_code,
    r.ascending_direction,
    r.descending_direction,
    r.station_urn,
    r.station_index
from {{ source('silver', 'odpt_railway') }} r
join latest on r.city = latest.city and r.odpt_version_id = latest.odpt_version_id
