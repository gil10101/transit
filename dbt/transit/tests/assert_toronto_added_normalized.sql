-- TTC quirk (dictionary §B): ~3% of trips are extra runs with synthetic negative
-- trip_ids, marked NEW by the feed (fixtures 2026-08-23). Staging must normalize
-- them to ADDED so the locked rule (service volume yes, OTP no) applies through
-- the generic ADDED path. Any toronto row still carrying NEW, or a negative-id
-- row still reading SCHEDULED/null, means the normalization regressed.

select trip_uid, trip_id, schedule_relationship
from {{ ref('stg_gtfsrt__trip_updates') }}
where city_key = 'toronto'
  and (
      schedule_relationship = 'NEW'
      or (trip_id like '-%' and coalesce(schedule_relationship, 'SCHEDULED') = 'SCHEDULED')
  )
