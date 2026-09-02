-- Typed interface over silver.odpt_trains (tokyo train-grain RT snapshots,
-- docs/01 §C.1). URN parsing per docs/03: railway 'odpt.Railway:Toei.Asakusa'
-- -> operator_name 'Toei', line_name 'Asakusa'. delay_sec is the operator-stated
-- delay (authoritative for Tokyo — finalization_method 'odpt_stated'); the
-- plausibility cap is applied downstream with every other city's
-- (int_odpt_stop_events), not here, so the raw stated value stays auditable.

with src as (
    select * from {{ source('silver', 'odpt_trains') }}
)

select
    city as city_key,
    agency,
    endpoint,
    source_format,
    feed_ts,
    fetched_at,
    service_date,
    trip_uid,
    trip_id,
    train_number,
    operator,
    railway as railway_urn,
    split_part(railway, ':', 2) as railway_tail,
    split_part(split_part(railway, ':', 2), '.', 2) as line_name,
    rail_direction as rail_direction_urn,
    train_type as train_type_urn,
    delay_sec,
    from_station as from_station_urn,
    to_station as to_station_urn,
    origin_stations_json,
    destination_stations_json,
    car_composition,
    train_index,
    ts_utc,
    valid_until_ts_utc
from src
