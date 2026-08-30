# 08 — Incident log

Every production incident, newest last. Entries before 2026-08-28 (the
2026-08-24 checkpoint overlap, the 2026-08-27 checkpoint-v2 replay, the
Zurich finds) predate this file and live in `docs/10-production-gates.md`
— seed rows citing "docs/08" for those dates mean that appendix.


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

## 2026-08-28 · Services box wedge (overlapping warehouse_chain runs)
The 17:43Z manual relaunch (4aed9466, recovering from the kafka outage above)
overlapped the 18:05Z scheduled chain — nothing serialized chain runs, so two
dbt builds ran concurrently on the services box (t4g.medium, 4GB — an earlier
revision of this entry said 2GB, which is the kafka box; wrong hardware in an
incident log poisons sizing decisions). The resulting thrash killed the
SSM agent and every poller at ~18:06Z; CPU stayed pinned 60%+ with no
self-heal until an operator reboot at 18:48Z. 4aed9466 had already committed
its drain and Iceberg refresh (silver current to 17:46Z) but its dbt build
never completed — and because gold is written only by dbt, no partial gold
exists; the chain is atomic by construction.

Structural fixes, not patches:
- QueuedRunCoordinator with a tag concurrency limit of 1 on warehouse_chain:
  a manual relaunch now queues behind (never beside) a scheduled run. Plus
  run_monitoring with a 90-min runtime cap so a wedged run fails loudly
  instead of blocking the queue (commit 39e60dd).
- External CloudWatch watchdog -> pipeline-alerts SNS (commit 0cce44e):
  sustained-CPU wedge alarm + status-check alarms on both boxes. This closes
  the run-failure sensor's structural blind spot — a box too sick to start
  runs emits no failure event. Validated live: the CPU alarm fired mid-incident.
- Docker json-log rotation (50m x 3) via /etc/docker/daemon.json on both
  boxes + both user_data templates: nothing rotated container logs before.
  STAGED, not yet active: dockerd reads daemon.json only at startup AND
  log-opts bind per-container at creation — so activation needs a dockerd
  restart followed by container recreation (verified: containers recreated
  2026-08-28 20:49Z still show log-opts map[]). Both boxes get it at their
  next reboot; growth is ~2G/month against 20G+ free, so nothing forces one.

Data note: pollers were down 18:06–18:48Z, so that window was never captured —
unlike the kafka incident's gap it is NOT replayable from raw. Both windows
seeded in incident_days for 2026-08-28 (all 7 polled cities). Recovery chain
a1858eca launched through the queue at 18:53Z.

## 2026-08-30 · Zurich completeness collapse, and the frozen-day class behind it
Chains went red from 00:05Z on `completeness_above_error_50pct` (331 route-days,
all zurich/2026-08-29). The feed was never the problem: zurich raw and silver ran
uninterrupted at ~4.2M rows and ~38k trips a day throughout, and its RT trips
matched the static at 99% on every affected day.

The loss was in `int_stop_events_finalized`. Zurich is delay-only, so its events
exist only if the schedule join lands (actual = scheduled + stated delay). During
the 2026-08-27 checkpoint-replay and 2026-08-28 kafka/box incidents that join was
failing at compute time, so the model wrote a fraction of the day: gold held
25,094 of 37,836 silver trips for 08-27, 10,907 of 38,140 for 08-28, 7,490 of
31,785 for 08-29 — a decay that looked like a stale static and was not.

Days inside the 48h window then healed themselves once the input
recovered: by 08:05Z on 08-30 the 08-28 and 08-29 rows had been rewritten to
37,794 and 31,438 trips (98-99% of silver) with no intervention. That is the
system working. **08-27 did not heal — it had aged past the 48h lookback and no
run will ever touch it again.** It sits at 66% coverage, and nothing detected it:
the 50% completeness floor passes at 66%, so gold was wrong and green for three
days. That is the failure class this entry is really about — not zurich.

Structural fixes:
- `lookback_hours` 48 -> 72 (dbt_project.yml). The lookback is the self-heal
  window, not just a freshness knob; a third day covers a multi-day incident, and
  the next chain repairs 08-27 as a side effect of the same mechanism that healed
  its neighbours.
- New test `assert_gold_reflects_silver_coverage`: gold trips per closed city-day
  must be >= 90% of silver trips, scoped to the repairable window (keyed off the
  same var) and excluding seeded incident days. Threshold is measured — every
  healthy city-day 08-25..29 landed 0.950-0.998, the broken day 0.663. Validated
  against prod before shipping: 7 rows, all >= 0.951, zero failures.
- Seeded 9 confirmed agency gaps that surfaced once zurich's noise cleared, each
  with evidence (`known_coverage_gaps.csv`): **nyc W and Z** — realtime carries 26
  of the 28 scheduled routes and these are the only two absent, while siblings on
  the same nqrw/jz feeds publish normally (N 1,277 trips, J 925); **sf
  AF:Tib-AIF** (Angel Island ferry, 0 silver rows across 7 judged days) and
  **BA:BridgeA/BridgeB** (BART weekend bus bridges, 0 silver rows ever, recurring
  every Sat/Sun); **zurich N46, N75, 28R** (0 realtime on every judged day) and
  **871** (route_type 715, demand-responsive — publication follows bookings).
  Not seeded: six zurich routes that dipped below the floor only on 08-29 and
  average 62-78%. They are real signal, not gaps, and hiding them is how a
  warehouse ends up all-green and wrong.
