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
  Six further zurich routes (140/142/145/156/240/892) were held back until their
  cause was proven, because a one-day dip on a route averaging 62-78% looks like
  real signal. Measured: silver announced every scheduled trip, but only a
  fraction carried any arrival payload, and gold scored EXACTLY the trips that
  did (140: 75 announced, 31 with payload, 31 scored; 142: 73/25/25; 145:
  76/27/27; 156: 75/26/26; 240: 28 announced, all single rows with no sequence,
  no delay, no timestamp, 0 scored). Announced-but-empty entries are the same
  upstream partial-publication class already seeded for HSL, so they are seeded
  with that evidence. The pipeline was never losing them.

## 2026-08-31 · The origin-stop deletion, and retiring the seed treadmill
Chains kept going red on `completeness_above_error_50pct` after the 08-30 fix —
5 rows, then 9, each batch a different set of Zurich routes. The routes changed
because the DAY TYPE changed: Zurich went live on a Tuesday, so its first
Saturday surfaced weekend-only routes and its first Sunday surfaced replacement
(Ersatzverkehr) and night services, none of which the Swiss feed publishes
realtime for. Every batch was "fixable" by adding rows to known_coverage_gaps,
and that is exactly what made it worth stopping: an error test whose green
depends on a human enumerating an open-ended set is a chore, not a tripwire, and
a month of holidays would have kept feeding it.

Root cause of the noise: the mart could not tell **the agency published nothing**
from **the agency published and we lost it**. Both show up as low
completeness_pct, only the second is ours, and only the second is worth paging
on — which is what the error test's own comment had claimed since 2026-08-27
("the error tripwire fires only when WE are blind") without the data to do it.

Fixes:
- `trips_published` and `capture_rate` are now columns on
  fct_service_delivery_daily. trips_published counts trips the feed gave us
  something usable for; capture_rate = trips_observed / trips_published is the
  only ratio here that is about us. The error test's scope adds
  `capture_rate < 0.90`, so agency silence (NULL capture_rate, read as 1) can no
  longer reach an error-severity check. Measured on prod before shipping: the
  08-30 failures drop from 9 to 1, and the survivor was a real defect —

- **the origin-stop deletion.** fct_stop_events ended with a misdating guard
  written as `where not (<dated condition>)`. An event with no ARRIVAL time has
  a NULL condition, `not NULL` is NULL, and a WHERE keeps only TRUE — so every
  such event was silently deleted. That is the first stop of nearly every trip
  in every city, because GTFS publishes a departure and no arrival at a trip's
  origin: **803,125 finalized events across six cities in six days** (toronto
  194,135, zurich 224,327, sf 202,861, dc 81,471, boston 79,649, nyc 20,682),
  roughly 134k a day, gone. It hid perfectly — the trips still appeared via
  their other stops, so no completeness, coverage or capture ratio moved — and
  it landed hardest on precisely the events `early_departure_flag` exists to
  judge, since early departure is measured at origin timepoints. Fixed with
  `coalesce(..., false)`: the guard drops only what it can prove misdated. It
  still drops all 34,920 genuinely misdated rows; 376,422 origin-stop events
  re-enter gold inside the current lookback.

Consequence to expect: event counts rise ~5% and early-departure figures move.
docs/06 numbers must be re-run from analysis/business_questions.sql, never
hand-edited.

## 2026-08-31 (second) · Corrupt feed delays beat a correct computation
Found while re-running analysis/business_questions.sql after the origin-stop
fix: SF's mean arrival delay read **-35,559 seconds** against a median of +63.
Not a rounding artifact — 511 publishes `arrival.delay` values like -16,245,480
on SFMTA trips whose scheduled and actual times are two minutes apart, and the
canonical COALESCE preferred the stated delay unconditionally, so a corrupt
field beat a correct computation sitting right beside it.

Scale, 2026-08-25..31: 17,810 scored events with |delay| > 1 day — sf 4,297,
toronto 11,733, nyc 1,780. Zurich, Helsinki, Boston and DC emit none, so this is
feed-specific corruption (511, TTC, MTA), not our arithmetic. At 0.08% of scored
events it never moved a median and no test saw it, which is exactly why it
survived: every published mean was wrong while every published median was fine.

Fix: a stated delay is now honoured only when `abs(delay) <= var
max_plausible_delay_sec` (86400, one day). Out of range is treated as ABSENT, so
the canonical COALESCE falls through to `actual - scheduled` — which we already
have and which is correct. The bound rejects the impossible rather than
second-guessing a genuinely terrible day. Canonical rule amended in
docs/01 §A.2 and CLAUDE.md; regression test `assert_delays_are_plausible`
asserts the outcome (any |delay| > bound in gold) rather than the mechanism, so
a feed inventing a new way to be wrong still trips it.

## 2026-09-01 · Static version rotation re-keys trip ids (root cause + fix)
The Sunday weekly static refresh landed `zurich-20260830-15afa2ba` and the next
weekday broke: Monday 08-31 read 0.68 gold-vs-silver coverage while 08-28..30
sat at 0.99. Caught by `assert_gold_reflects_silver_coverage` — the test doing
exactly its job on its first real incident.

Root cause: `int_trip_matching_generic`'s exact branch proved a trip id EXISTS
in the static, never that the calendar places it on that service_date. Agencies
re-key trip ids when publishing a new static version, so after the rotation
38,518 of 39,108 RT trips still id-matched but only 26,397 had a schedule row.
Zurich is delay-only — no schedule, no event — so a third of the day could not
be finalized. Of the 12,711 orphans, only 2,437 were recoverable from the old
version, so "pin the version current at publication time" was NOT the fix; and
all of them carried an explicit feed `start_date` of 20260831, so it was not
service_date misattribution either.

What they were: the same runs under different ids. 12,563 of them (100% of
those carrying route + start_time) match an ACTIVE static trip on the same route
with the same first-stop departure second.

Fix: a re-key recovery branch in the matcher. A trip whose id lands on nothing
the calendar runs today is re-matched by (route, origin departure) against a
trip it DOES run, at exact-second tolerance (recovering a re-keyed identity, not
guessing at one), confidence 0.9. Purely additive — it only sees trip_uids the
exact branch could not place on a calendar-active trip, so no existing match can
be displaced, and an id match with no calendar row is still kept because ADDED
trips legitimately look like that.

Generalises beyond Zurich: every exact-match city (boston, dc, sf, toronto,
zurich) gets the same recovery at every future static refresh, which is a weekly
event. Provider-side contradictions are NOT deleted — the trips the agency
declares for a date its own static contradicts stay visible in silver and in the
coverage columns; only the measurement was corrected.

## 2026-09-01 · Second box wedge, and making it self-heal
The services box wedged again at 16:07-16:10Z: InstanceStatus `impaired`,
SystemStatus ok, SSM ConnectionLost, CPU pinned flat at ~52% for six hours,
every poller stopped. Last raw S3 object for every city is 16:10Z — unlike a
Kafka backlog this window was never written down, so **6.4 hours of feed data is
permanently lost** (16:10-22:36Z). Recovered only when Jake ran reboot-instances
at 22:31Z; containers came back clean, pollers writing within 30s, no OOM kill
in dmesg and disk at 36%, so the trigger is still unidentified.

The alarms worked exactly as designed — cpu-wedge fired 17:02Z, status-check
21:21Z, both emailed. What did not work is the part that assumed a human reads
the email: unattended, this box sits dead for as long as it takes someone to
notice. That is the AFK-critical gap, not the detection.

Fixes:
- **Self-healing reboot**: new alarm `transit-pulse-services-autoreboot` on
  StatusCheckFailed_Instance (2x5min) with EC2's built-in
  `arn:aws:automate:...:ec2:reboot` action plus an SNS notify. The built-in
  action needs no agent on the box, which is the entire point — it works when
  the box cannot help itself. Both wedges would have self-cleared in ~10 min
  instead of 6 hours.
- **CPU alarm retuned** 55%/30min -> 75%/45min. It flapped four times in one
  afternoon because steady state under a heavy drain plus rebuild measured
  51-64% — the threshold sat inside normal operation. A flapping alarm is worse
  than no alarm: it teaches the reader to ignore the channel.

Open question, deliberately not guessed at: the wedge trigger. Both times two
warehouse_chain runs were in flight (14:05 still draining when 16:05 started)
on a 4GB t4g.medium. The serialization tag IS present on all three runs and the
queue demonstrably blocks now (22:36: one STARTED, one QUEUED), so why the 16:05
run dequeued while 14:05 was live is unexplained. Next occurrence: capture the
daemon's dequeue decision before rebooting.

### Follow-on: the zombie that blocks the queue (same incident)
After the reboot the interrupted 16:05Z chain stayed STARTED and the 22:36Z
chain sat QUEUED behind it. MonitoringDaemon logged "Checking run
2d3fe921..." every 120s and never cleared it: its run worker died with the box,
so the max_runtime_seconds termination path has nothing to terminate. With
tag_concurrency_limits limit=1 that single dead run blocks every subsequent
chain — runs queue, nothing executes, and NOTHING ALERTS, because a blocked
queue is not a failure. A silent stop is worse than a red chain.

Cleared by hand with `dagster run delete --force <run_id>`; the queued chain
went STARTED within seconds. But a fix that needs a human is the same assumption
that had just failed with the reboot, and the auto-reboot makes this state MORE
likely — every future wedge now self-heals straight into it.

So it is automated: sensor `zombie_run_reaper` (orchestration/transit_dagster/
reaper.py, RUNNING by default, 120s interval) fails any run still STARTED 3.5h
after it began — run_monitoring's 3h cap plus a 30-min margin, so it acts only
where run_monitoring has already had its chance and could not reach. It reports
the run FAILED rather than deleting it, which keeps the record and fires the
run-failure sensor, so a reaped zombie arrives as an email instead of vanishing.
Decision logic lives in lib.zombie_run_ids so it is unit-tested in the repo venv
(which has no dagster), same convention as alert_body; boundary cases pinned in
tests/test_dagster_lib.py — a 2.9h run and a 3.1h run both survive, a 6.5h one
is reaped, and a run with no start_time is never touched.

### Swap added to the services box (2026-09-02)
Both wedges (08-28, 09-01) recorded **no OOM kill** — the kernel never got to
kill a process, the machine simply thrashed until SSM and every poller died.
That is the signature of memory pressure with nowhere to spill, and it cost 7.1
hours of unrecoverable feeds across the two events.

4 GB swapfile added, `vm.swappiness=10` (safety net, not routine paging),
persisted in /etc/fstab so it survives the new auto-reboot, and mirrored into the
services user_data so a rebuilt box gets it before Docker starts. Live values:
4095 MB swap, 0 used, free RAM 909 MB with a rebuild running.

This is deliberately the cheap half of the fix — it converts a cliff into a
slowdown, it does not add capacity. Sizing to 8 GB remains available and costs
about $24/month gross (currently credit-absorbed). Done ahead of Tokyo's poller,
which adds an eighth container to this box.

## 2026-09-02 · The plan that would have destroyed both boxes
Running `terraform plan` for the Tokyo rollout returned **"7 to add, 2 to change,
2 to destroy"**, with both instance ids going to `(known after apply)`. Cause:
`data.aws_ssm_parameter.al2023_arm` resolves the LATEST Amazon Linux AMI, AWS had
published a new one since the boxes launched on 08-22, and `ami` forces
replacement on aws_instance.

Applying it to add one poller would have destroyed and recreated both EC2 boxes:
the Dagster postgres (every run record and the queue state), the Kafka buffer,
the services box public IP, and both instance ids — which the CloudWatch alarms
name explicitly, so the watchdog would have been left watching machines that no
longer existed. Two weeks before the deliverable.

Fix: `lifecycle { ignore_changes = [ami] }` on both instances. The data source
still supplies an image for a genuinely new box; it no longer rebuilds a running
one as a side effect of an unrelated upstream release. Upgrading the AMI is now a
deliberate act, which is what it should always have been. Re-plan after the pin:
**5 to add, 4 to change, 0 to destroy**.

Generalisable: any long-lived stack that resolves "latest" anything into a
ForceNew attribute will eventually rebuild itself on someone else's schedule.

## 2026-09-03 · A TRY_CAST Snowflake would not take (3 red chains)
Chains `20aaaa8d`, `cf217c47` and `5c9c99d9` all died on the same node:

```
194 of 267 ERROR creating sql table model GOLD.fct_benchmark_mlit_monthly
  001065 (22023): SQL compilation error:
  Function TRY_CAST cannot be used with arguments of types NUMBER(38,3) and FLOAT
```

`fct_benchmark_mlit_monthly` (shipped with the P4 Tokyo work) used
`{{ dbt.safe_cast('median(e.delay_arr_sec)', 'double') }}`. On Snowflake
`safe_cast` renders `TRY_CAST`, and **Snowflake's TRY_CAST accepts a string
source only**; `median()` over an integer is `NUMBER(38,3)`. duckdb's `TRY_CAST`
takes anything, so the model was green in dev and failed the first time
production reached it. A second `safe_cast` in the same model (date → varchar)
would have failed next for the same reason.

Fix: plain `cast(... as {{ dbt.type_float() }})` and
`cast(... as {{ dbt.type_string() }})`. A number to a float cannot fail, so the
safe cast was defending against nothing.

**No data was lost.** dbt does not stop at a failed leaf, so the 193 models ahead
of it built normally and Tokyo landed in gold on schedule through all three red
chains. What was lost was the green signal — a red chain that is actually fine
trains you to ignore red chains.

Same class as the 2026-08-26 7-train regex: **dev is duckdb, production is
Snowflake, and the gaps are not exotic.** Standing rule from here: every new or
changed model gets its compiled Snowflake SQL executed against the warehouse
before merge. This fix was verified that way in under a minute, rather than 40
minutes later when the chain reached it.

## 2026-09-06 · Every Sunday an agency deletes its own history
Found while checking judged-day counts. Agencies publish a static timetable whose
`calendar.txt` **begins at the day it was published**, and
`fct_service_delivery_daily` is a full-refresh table: every chain recomputes all
of history against whatever static is current (`stg_gtfs__*` all pin
`max(gtfs_version_id)`). So each Sunday refresh silently zeroes `trips_scheduled`
for every service date the new calendar does not reach back to, which nulls
`completeness_pct` and makes those days unjudgeable.

Measured after the 2026-09-06 refresh — "judged days" = closed, at-or-after
`metrics_from`, completeness ≥ 0.50:

| City | Current calendar starts | Days with events | Judged days |
|---|---|---|---|
| toronto | 2026-09-06 (that day) | 15 | **0** |
| tokyo | n/a — see below | 4 | **0** |
| helsinki | 2026-09-04 | 15 | 2 |
| boston | 2026-08-30 | 15 | 7 |
| zurich | 2025-12-14 | 13 | 11 |
| dc | 2026-06-21 | 14 | 12 |
| sf | 2021-06-22 | 15 | 12 |
| nyc | 2026-05-26 | 16 | 14 |

TTC started a new board period on the 6th, so **Toronto's whole history became
unjudgeable in one refresh** — 13.8M stop events and a 49.9% OTP that no
scorecard may use. Re-running `scripts/export_site_data.py` the same day dropped
the public standings from 7 cities to 6: Toronto and Tokyo both fell out.

The stop events themselves are safe (`fct_stop_events` is incremental and keeps
what it computed). What is destroyed is the denominator that lets a city be
scored at all.

**Tokyo is 0 for an unrelated reason** and needs its own fix: `active_trips` in
the delivery mart joins `stg_gtfs__trips` to `int_service_dates`, and
`int_service_dates` derives its dates from `stg_gtfsrt__trip_updates` — Tokyo has
no GTFS-RT rail stream, so it contributes no service dates and no scheduled
trips. Its events, OTP, alerts and vehicle activity are all correct; only
`trips_scheduled` is missing, so it cannot reach the scorecard either.

Fix (**APPLIED 2026-09-10**, commits `49db3f7` + `b7eb1bc`, deployed 03:34Z):
**point-in-time statics for the denominator only** — `int_gtfs_version_for_date`
pins (city, service_date) → the version live that day; `int_service_dates_pit`
and `int_gtfs_trips_for_date` resolve calendar and trips against that pin; the
delivery mart reads those two. The delay path (matchers,
`int_gtfs_scheduled_stop_times`, `int_service_dates`) deliberately stays
newest-pinned — it only operates inside the 72h window, where newest is correct,
and PIT dates × newest trips would silently drop delay events after an id
renumbering. Snowflake simulation before deploy: judged days Boston 10 → 18,
Helsinki 5 → 18, Toronto 3 → 18. Tokyo's branch feeds `trips_scheduled` from
`odpt:TrainTimetable` (`int_odpt_scheduled_trips`). First chain to run all of
it: 04:05Z 2026-09-10 — verification below in the day's entry.

Correction to the 2026-09-03 note on this: Helsinki's August was called
unrecoverable on the strength of a 31% service_id overlap. That measurement
paired the OLD calendar with the NEW trips file; point-in-time pairs each
calendar with its own trips, so the overlap is not the constraint.

Generalisable: **a full-refresh fact computed against a mutable reference is not
reproducible — it is a snapshot of the reference, not of the day.** Either the
reference must be versioned point-in-time or the fact must be frozen.

## 2026-09-10 · The day-offset ghosts the 1-day bound was built for walked under it

Found by the field-level sampling pass (memory rule: a green job is not
evidence), not by any test — the same way the 2026-08-31 corrupt-delay class was
found, and one band below it.

**Mechanism.** A trip matched to the wrong calendar day produces
`delay = ±(86400 − true_delay)`. The 2026-08-31 result-bound rejected
`|delay| > 86400` — but the artifact lands just UNDER a day by construction, so
the bound passed almost every one it existed to kill. Only the rare cases that
crossed a day (±86400 and beyond) were caught.

**Measured** (gold, 2026-09-10 04:00Z):

| band | events |
|---|---|
| 12–24h | **57,153** (toronto 29,776 / sf 16,537 / nyc 10,815 / zurich 21 / dc 4; extremes exactly ±86400) |
| 6–12h | 2,084 |
| 2–6h | 23,341 |

A distribution with a hole between 6h and 12h and a wall at exactly one day is
an artifact family, not a service pattern. Medians never moved; means were
wrecked (the 08-31 lesson again: a metric that only breaks in the mean is still
broken, and docs/06 quotes means).

**Fix.** `max_plausible_delay_sec` 86400 → 43200. Nothing real in urban transit
is 12 hours late at a stop; every day-offset ghost is at least 12h out so long
as true delay stays under 12h. Stated-input bound and result bound both
tighten; `assert_delays_are_plausible` reads the var, so the tripwire follows
automatically (and its 72h scope means the next chain's delete+insert window
recompute clears recent rows before the test ever sees them).

**Repair.** Incremental models never revisit old rows, and an in-place UPDATE
on gold is exactly the kind of mutation this project routes through a human.
Instead the bound now lives at the FACT layer too: `fct_stop_events` applies
`max_plausible_delay_sec` to whatever the finalized layer carries (and
`otp_band` / `early_departure_flag` inherit the bounded values by
construction), so one `--full-refresh` of the fact — a cheap read of the int
table, no silver reprocess — heals all of history, for this bound change and
any future one. `int_stop_events_finalized` keeps its stale >72h values,
documented in both model headers; every consumer reads the fact.

**Executed 2026-09-10 ~04:53Z** (fact + fct_route_reliability_daily +
fct_benchmark_mlit_monthly + fct_city_scorecard_monthly, `--full-refresh`
`--target prod`: 61 pass / 3 pre-existing warns / 0 errors). Proof:

| | before | after |
|---|---|---|
| events with \|delay\| in the dead band | 57,523 (toronto 29,776 / sf 16,824 / nyc 10,898 / zurich 21 / dc 4) | **0** |
| nyc mean / median arrival delay | −193s / +2s | **+59s** / +3s |
| toronto mean / median | −35s / −5s | **+52s** / −5s |
| sf mean / median | +198s / +62s | **+174s** / +61s |

Means moved by minutes; medians moved by at most a second — the fingerprint of
removing a symmetric artifact rather than reshaping the distribution.

## 2026-09-10 · TTC's feed went dark for six hours, and the alerting shouted 22 times

**Agency outage, not ours.** `bustime.ttc.ca` stopped answering at **15:51Z**
(11:51 ET): read timeouts, then connect timeouts, plus one 41-byte non-protobuf
body ("Error parsing message" at 15:54Z, archived at 16:05Z). Service returned
**22:17Z** (18:17 ET). The other seven cities were untouched — bronze holds NYC
and Boston complete through 15–17Z — so broker, network and box are cleared.

**Nothing to recover, proven rather than assumed.** A bronze→gold rebuild was
requested and declined on evidence: raw S3 (the replay archive) and bronze agree
at both edges — last good payload 15:58:21Z in each, next 22:17:51Z in each, only
the 41-byte error object between. Silver and gold reproduce that same hole hour
for hour (gold local hours 13–17 near-zero on 2026-09-10). A rebuild would spend
a full reprocess of credits and write identical gold. The six hours were never
published to anyone.

**Detection worked; notification failed the reader.** `raw_feed_freshness_job`
failed correctly on 22 consecutive runs (16:55Z–22:10Z), and the run-failure
sensor emailed every one — 22 byte-identical messages for one incident, plus one
more for a weather run killed by an image restart. Subject lines read "FAILED",
so the reader heard "the chain is failing" all afternoon. That is the alert
fatigue the sensor's original "no dedup window" rationale warned against, reached
from the other direction. Fixed: **transition-only alerting** —
alert when a job starts failing or fails a *new* way (different error detail),
stay silent on an identical repeat, and send one RECOVERED message on the first
success after a failure (`lib.alert_kind`, `alerting.py`, tests/test_alerting.py).

**Scoring impact.** Toronto 2026-09-10 read 53.5% complete at gold's 20:35 ET
edge. Route-days under the 50% floor drop out of scoring by design; the rest count.
Not seeded into `incident_days` — that seed is OUR-outage-only by contract, and
the error tripwire already reads agency silence as capture ≈ 1.

**One edge the tripwire's heuristic gets wrong, measured.** Three route-days
(202, 191, 185) would page if the day closed as it stood: completeness 0.45–0.49
with capture 0.85–0.89. The outage cut trips off mid-route — they published
predictions before the feed died and never finalized — so they read as
published-but-uncaptured, i.e. as our blindness. Projected clear: the late
schedule supplies 13 / 29 / 34 more trips after the edge against 1 / 2 / 6
needed. Verified at the 11:05Z chain (below). If a future mid-day agency outage
lands near day-close, this is the misfire to expect.

## 2026-09-10 · The 12-hour cadence silently dropped silver rows — found in bronze, replayed from bronze

**Found by asking whether gold was skewed, not by any test.** Chasing Toronto's
outage-edge echoes turned up "feed gaps" in cities whose feeds never failed. Bronze
vs silver, distinct fetches per hour, settled it: bronze complete, silver holed.

| city | lost (UTC, 2026-09-10) | silver / bronze fetches |
|---|---|---|
| helsinki | 06–07Z, 15–21Z | 20 / 1,080 |
| nyc | 06Z | 27 / 120 |
| dc | 06Z | 27 / 120 |
| zurich | 06Z | 14 / 60 |
| sf | 06Z | 4 / 18 |

(Zurich's nightly 23–00Z shortfall is the allow-list emptying fetches overnight — it is
there on 2026-09-08 too, under the 2h cadence. Not loss.)

**Mechanism.** Silver's dedup, `withWatermark("fetched_at", "2 hours")
.dropDuplicatesWithinWatermark(...)`, is also a late-data filter: a row more than 2h
behind the newest event its query has seen is discarded. Under the 2h cadence each
drain's backlog was about the watermark, so partitions never drifted 2h apart. The 12h
cadence (72e2026) made single drains process 7–13h of backlog; partitions advanced
unevenly, the watermark ran ahead of the slow ones, and their rows were dropped as
"late". Bronze is written in the same drain with no watermark, so it kept everything.
Every lost window sits inside one of the two long-backlog drains (11:05Z, 23:05Z).

**Owned.** The cadence change shipped with the credit arithmetic and without checking
the streaming semantics it leaned on. Jake asked for a replay from bronze the same
evening; the first answer proved Toronto unrecoverable (true — TTC never served those
hours) and generalised it. It did not generalise.

**Fix.**
- Drains every 2h again (`silver_drain` job + schedule, EMR only — no Snowflake credit);
  dbt stays at 12h. The shared `transit/serialize` tag keeps drain, chain and backfill
  from overlapping on the Iceberg tables.
- `spark_jobs/silver_backfill.py` replays bronze windows through the SAME silver
  builders (bronze's `envelope_json` is the Kafka value verbatim) and anti-joins on the
  stream's own dedup keys (`silver_normalize.DEDUP_KEYS`, looked up 2h before each
  window), so a rerun appends nothing. Launched through the Dagster `silver_backfill`
  job on the production EMR path.

### Verification (2026-09-11, 21:50 ET)

Backfill EMR run `00g8m95mgjfg0o0f` SUCCESS. Bronze-vs-silver parity for 2026-09-10
went **92.65% → 97.29%**, and every real hole closed: Helsinki 06–07Z and 15–21Z, NYC /
DC / SF / Zurich 06Z. What remains is benign and predates the cadence change — Zurich's
overnight hours, where the national allow-list leaves a fetch with no rows (present on
2026-09-08 under the old 2h cadence too), and Tokyo's 20Z, which is the 5am start of bus
service. Drains have run every 2h since; the 3h drain that followed the fix dropped
nothing (99.03% parity, only Zurich's overnight).

Toronto's three at-risk route-days closed exactly as projected — 185/191/202 at 0.68–0.76
completeness with capture 1.0 — and the completeness tripwire would page on **0**
route-days. Both scheduled chains (11:05Z, 23:05Z) succeeded.

One drain failed at 23:05Z and succeeded on retry at 23:48Z. That is the first live test
of transition-only alerting: one FAILED mail, one RECOVERED mail, instead of the old
behaviour's repeat-per-tick.

## 2026-09-11 · An arrival nobody could see was still being scored

**Found by pulling on "is the data skewed?" rather than by a test.** When a feed goes
dark the finalizer still turns each in-flight trip's LAST prediction into an "actual" at
every stop the vehicle reached in the dark. Measured across gold: **1,004,952 scored
events sat inside stretches where their own city's feed delivered nothing** — Helsinki
380,755, SF 193,882, Toronto 123,387, Zurich 120,117, DC 92,024, Boston 62,600, NYC
31,424, Tokyo 763.

Every one of them traces to a known incident, which is what makes the rule safe to
apply: 2026-09-10 (TTC outage + the silver drops above) 306,503; 2026-08-28 (kafka disk
+ box wedge) 274,751; 2026-09-01 (box wedge) 196,102; 2026-08-31 76,043; 2026-08-27
(checkpoint replay) 25,136 + the 108,870 filed under local service date 2026-08-26,
because that replay gap (00:25–04:47Z) falls inside North America's previous service
day. Genuinely ordinary days hold at most 364 events between them.

**Why the existing guard missed it.** `max_prediction_lead_min` (60) asks how old the
winning prediction was; an outage echo's lead is *minutes*, because the feed died right
after predicting. It caught 45 of Toronto's 23,474.

**The rule** (docs/01 §A.2): an arrival strictly inside a feed gap — over
`feed_gap_min` (10) minutes with no fetch for that city — is unobserved. Volume yes, OTP
no. `int_feed_heartbeat` (incremental, one row per city-minute that delivered anything)
→ `int_feed_gaps` (view) → `feed_gap_flag` folded into `stale_observation_flag` on
`fct_stop_events`. Built from the silver FETCH log, not event timestamps: the first cut
used event first/last_seen and misread quiet overnight hours as outages (~400 false flags
a day in Tokyo alone). `fct_headways` drops any gap interval overlapping a dark stretch —
dropping the *gap* rather than the arrival, so an outage cannot manufacture one six-hour
headway — and the MLIT benchmark excludes stale arrivals. Asserted by
`assert_no_scored_events_in_feed_gaps`.

## 2026-09-11 · Rotation freeze: a refresh may no longer erase the days before it

TTC's 2026-09-06 static publishes a calendar starting 2026-09-06. The delay path resolves
schedules against the NEWEST static, so the next chains recomputed 09-03..05 against a
calendar that does not cover them: **3.13M Toronto events, 0% scored** — volume intact,
every delay gone. The point-in-time fix of 2026-09-10 covered the denominator only; this
is the same erosion one layer over.

DC made it urgent rather than historical: its newest calendar **ends 2026-09-12**, so the
Sunday 09-13 refresh is mandatory *and* would have taken DC 09-10..12 the same way.

Fix, in `int_stop_events_finalized`'s incremental filter: a (city, service_date) the
newest static does not cover is **not recomputed** — it keeps what it computed while its
own static was current. A day never computed before is still computed, because volume must
never be lost to protect a delay. Simulated against the live window before shipping: every
city-day currently in range is covered, so the guard is inert until a rotation lands.
(`ref()` inside `is_incremental()` needs an explicit `-- depends_on:` hint; dbt cannot
infer it.) The Sunday static refresh also moves 09:00Z → 12:30Z, after the 11:05Z chain,
so Saturday's late evening is computed on its own static before the rotation arrives.

Toronto 09-03..05 stay volume-only: repairing them needs a recompute against the old
static, which the delay path cannot address today.
