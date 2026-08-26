-- Contract: NYC direction extracted from the FULL static trip_id must agree with
-- the extraction from the prefix-free origin_time_token.
--
-- The failure this pins: Snowflake string literals eat backslashes, so the
-- direction regex '\.\.?([NS])' degraded to "any two chars then N/S" and matched
-- the S inside static id prefixes ('L0S3-…'). Every 7-line static trip read as
-- southbound, the northbound half of the line matched nothing, and 346 of 661 RT
-- trips vanished between silver and gold on 2026-08-24 — surfaced by
-- completeness_above_error_50pct (13 route-days), root-caused by hand, macro
-- fixed to double backslashes for Snowflake. If the escaping ever regresses, the
-- full-id extraction breaks first (prefixes carry stray S/N; bare tokens do not)
-- and this goes red.

select
    trip_id,
    origin_time_token,
    {{ re_extract('trip_id', '\.\.?([NS])', 1) }} as from_full_id,
    {{ re_extract('origin_time_token', '\.\.?([NS])', 1) }} as from_token
from {{ ref('stg_gtfs__trips') }}
where city_key = 'nyc'
  and {{ re_extract('trip_id', '\.\.?([NS])', 1) }}
      is distinct from {{ re_extract('origin_time_token', '\.\.?([NS])', 1) }}
