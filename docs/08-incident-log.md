
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

## 2026-08-28 · Kafka disk-full outage (both 16:05Z-window chain reds)
Chains a6330021 (16:05Z scheduled) and 74b2c838 (16:54Z relaunch) failed at
emr_drain. Surface error was misleading ("eventlog dir already exists" — the
driver's second context attempt); driver stderr showed the real one: Kafka
timeouts ("Timed out waiting for a node assignment"). The kafka box (t4g.small,
30GB) hit 100% disk after 5.8 days at KAFKA_LOG_RETENTION_HOURS=168 —
retention was sized as an archive when it is a disk budget (raw S3 is the
archive; drains read at most a few hours back). Broker crash-looped; the
raw-feed tripwire stayed green because pollers dual-write raw S3 directly.
Structural fixes, not patches:
- retention 168h -> 24h (terraform user_data + live topic-level override),
  volume 30 -> 48GB (grown online during recovery; terraform matched)
- broker TCP reachability added to the 15-min raw_feed_freshness job
  (kafka_listener_down in lib, unit-tested) — pages within one tick
- infra steps emr_drain + snowflake_iceberg_refresh get RetryPolicy
  (2 retries, exponential from 120s; fresh EMR job id per attempt). The dbt
  step deliberately never retries: data failures must stay loud.
Data note: broker-down window ≈ 16:00–recovery; pollers kept raw S3 fresh, so
the Kafka-side gap is replayable from raw if ever needed; silver resumes from
committed checkpoints (latest offsets) at the next green drain.
