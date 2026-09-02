-- Tokyo rail has exactly one finalization path: odpt_stated via
-- int_odpt_stop_events (docs/01 §C — the ODPT JSON API is the primary and only
-- rail RT source; ToeiBus is VP-only so it can never mint a trip-update event).
-- A tokyo row with any other method means GTFS-RT data leaked into the city's
-- rail path, or the odpt branch lost its label — either silently corrupts the
-- city's OTP story. The reverse also holds: odpt_stated belongs to tokyo alone.
select event_key, city_key, finalization_method, source_format
from {{ ref('fct_stop_events') }}
where (city_key = 'tokyo' and finalization_method != 'odpt_stated')
   or (city_key != 'tokyo' and finalization_method = 'odpt_stated')
