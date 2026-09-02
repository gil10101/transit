-- Latest ODPT station dump (tokyo). station_code is the verified URN<->GTFS
-- join key (= GTFS stop_code, 149/149 Toei / 185/185 Metro — docs/01 §C.4).

with latest as (
    select city, max(odpt_version_id) as odpt_version_id
    from {{ source('silver', 'odpt_station') }}
    group by 1
)

select
    s.city as city_key,
    s.odpt_version_id,
    s.station_urn,
    s.operator,
    s.railway_urn,
    s.station_code,
    s.title_ja,
    s.title_en,
    s.lat,
    s.lon
from {{ source('silver', 'odpt_station') }} s
join latest on s.city = latest.city and s.odpt_version_id = latest.odpt_version_id
