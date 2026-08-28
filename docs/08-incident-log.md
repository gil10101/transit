
## 2026-08-28 · SF ADDED trip with no route_id breaks delivery keys
04:05Z chain red: `not_null_fct_service_delivery_daily_{delivery_key,route_id}`,
1 row — sf/2026-08-27, an ADDED trip (`SA:t_6153536_b_86615_tn_0`, SMART) whose
RT rows omit route_id even though the trip id exists in the 511 static
(match_confidence 1.0). Snowflake CONCAT_WS propagates NULL, so delivery_key
nulled too. Fix by construction, not filter: the matcher now emits
`coalesce(rt.route_id, static.route_id)` (a matched trip inherits the static's
route — same authority rule as the matcher itself), the finalizer coalesces
`matched_route_id` through to events, and both route-grain marts fold any
residual unmatched-unrouted event into an explicit `__unrouted__` bucket
(route_key stays NULL). Regression test `assert_matched_events_have_route`.
Verified: full dev build green; prod chain 26833524 SUCCESS 05:18:37Z on image
13c83096e7d6; 0 null-route rows; the trip now reads route `SA:SMART`.
