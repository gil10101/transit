-- An event finalized from a prediction last seen long before the event is a
-- schedule echo, not an observation, so it must score no otp_band — the same
-- treatment SKIPPED stops get. It still counts as service volume.
-- HSL is the extreme case: it publishes trip updates for journeys up to three
-- days ahead, so a single poll can carry an "arrival" for a trip that has not
-- started. fct_route_reliability_daily drops the same rows from its OTP and
-- delay aggregation; this test is what keeps the two models in step.
select event_key, city_key, service_date, otp_band
from {{ ref('fct_stop_events') }}
where stale_observation_flag
  and otp_band is not null
