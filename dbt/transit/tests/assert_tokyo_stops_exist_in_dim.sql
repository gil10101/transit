-- Stop-map coverage: every tokyo stop_id emitted by the odpt path must exist in
-- the tokyo GTFS static (int_odpt_stop_map only emits mapped stations, so a
-- failure here means the map and the static drifted apart — e.g. a schedule
-- revision renamed stop ids between the weekly dump and zip refreshes).
select distinct e.stop_id
from {{ ref('int_odpt_stop_events') }} e
left join {{ ref('stg_gtfs__stops') }} s
  on s.city_key = 'tokyo' and s.stop_id = e.stop_id
where s.stop_id is null
