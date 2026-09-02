-- ODPT URN <-> tokyo GTFS id map (docs/01 §C.4, docs/03: required, tested both
-- ways). Join key: odpt:stationCode = GTFS stop_code, verified 149/149 on the
-- 2026-09-01 Toei zips; route side joins the railway's Japanese title to the
-- GTFS route_long_name (浅草線 etc., 1:1 on the same zips).
--
-- TOEI ONLY on purpose: Metro's GTFS zip is not ingested (its stop_ids collide
-- with Toei rail's and Metro has no RT trains to map — docs/01 §D), so a Metro
-- row here would dangle against stg_gtfs__stops. Metro stations still reach
-- dims via stg_odpt__station directly. The map is empty until the tokyo GTFS
-- statics are parsed — models tolerate zero rows (docs/03 convention).

with stations as (
    select station_urn, operator, railway_urn, station_code, title_ja
    from {{ ref('stg_odpt__station') }}
    where operator = 'Toei'
),

railways as (
    select distinct railway_urn, title_ja as railway_title_ja
    from {{ ref('stg_odpt__railway') }}
    where operator = 'Toei'
),

tokyo_stops as (
    select stop_id, stop_code, stop_name
    from {{ ref('stg_gtfs__stops') }}
    where city_key = 'tokyo' and stop_code is not null
),

tokyo_routes as (
    select route_id, route_long_name
    from {{ ref('stg_gtfs__routes') }}
    where city_key = 'tokyo'
)

select
    'tokyo' as city_key,
    s.station_urn,
    s.operator,
    s.railway_urn,
    s.station_code,
    st.stop_id,
    st.stop_name,
    r.route_id
from stations s
join tokyo_stops st on st.stop_code = s.station_code
join railways rw on rw.railway_urn = s.railway_urn
left join tokyo_routes r on r.route_long_name = rw.railway_title_ja
