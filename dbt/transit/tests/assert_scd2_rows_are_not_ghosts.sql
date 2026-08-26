-- Contract: an SCD2 dim row must describe an entity that EXISTS in its opening
-- version. The failure shape this pins: absence detection keyed on the attribute
-- hash instead of the join — with every attribute coalesced, md5(concat_ws(...))
-- is non-NULL even for a missing row, so absent versions become "present with
-- every attribute NULL" and open ghost validity intervals. Happened on first prod
-- build (16 dim_route rows, caught by not_null_dim_route_mode); this is the
-- direct assertion, kept so the next dim added here cannot re-grow the bug.

select 'dim_route' as dim, city_key, route_id as natural_id, valid_from
from {{ ref('dim_route') }}
where agency_id is null
  and route_short_name is null
  and route_long_name is null
  and route_type is null
  and route_color is null

union all

select 'dim_stop', city_key, stop_id, valid_from
from {{ ref('dim_stop') }}
where stop_name is null
  and lat is null
  and lon is null
  and parent_station is null
  and location_type is null
