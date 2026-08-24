-- WMATA quirk (dictionary §B): rail's extra 'NR' shuttle runs arrive marked
-- UNSCHEDULED (fixtures 2026-08-23: 2/142 rail TUs, ids absent from static).
-- Staging must normalize them to ADDED so the locked rule (service volume yes,
-- OTP no) applies through the generic ADDED path. Any dc row still reading
-- UNSCHEDULED means the normalization regressed.

select trip_uid, trip_id, schedule_relationship
from {{ ref('stg_gtfsrt__trip_updates') }}
where city_key = 'dc'
  and schedule_relationship = 'UNSCHEDULED'
