-- A SKIPPED stop was never served, so it can never be on time: fct_stop_events
-- must leave otp_band null for it, and fct_route_reliability_daily drops the
-- same rows from its OTP aggregation. WMATA bus marks ~30% of its stop-time
-- updates SKIPPED (fixture 2026-08-23: 7,551/25,265), so letting one of the two
-- models drift from the other would quietly skew DC's headline reliability.
select event_key, city_key, service_date, otp_band
from {{ ref('fct_stop_events') }}
where skipped_flag
  and otp_band is not null
