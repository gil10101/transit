# 05 — End-to-end pipeline walkthrough

**Audience:** a new engineer joining Transit Pulse who has never seen the repo.
**Goal:** after reading this you can trace one bus arrival from an agency's API to a
number on a chart, and know which knob to turn when a number looks wrong.

This doc is a *guide*, not a spec. The specs are `01-data-dictionary.md` (fields and
feed quirks), `02-warehouse-schema.md` (table grains and keys), `03-dbt-spec.md`
(models and tests), `04-deliverables-todo.md` (phase checklists). Where this doc and a
spec disagree, the spec wins — and the disagreement is a bug in one of them.

Status snapshot in this doc is dated **2026-08-25 01:30Z**. Numbers move; the shape doesn't.

---

## 1. The business question

> **Which cities run the most reliable public transit?**

Everything below exists to make that question answerable with evidence rather than
vibes. Three decisions fall out of the question and explain most of the design:

1. **"Reliable" is not one number.** A rider on a 4-minute subway never reads a
   timetable — they care whether the wait is *regular*. A rider on a 30-minute
   suburban bus cares whether it hits the printed time. So we compute several
   metrics and only combine them at the very end, with published weights.
2. **Cross-city comparison is the whole point**, so every city must be measured the
   same way. That forces one canonical schema, one delay rule, one service-date rule,
   and analysis in *local* time (3am Tokyo compares to 3am New York, not to 3am UTC).
3. **A trip that never ran is the worst delay of all.** Punctuality measured only over
   trips that showed up flatters cities that cancel. Hence completeness is a
   first-class metric and gates whether a city's punctuality is trustworthy at all.

### What we actually compute

| Metric | Mart | What it means | Why a rider cares |
|---|---|---|---|
| OTP % | `fct_stop_events` → `fct_route_reliability_daily` | share of stop arrivals in the on-time band (−60s .. +299s) | did it arrive when promised |
| Mean / median / p90 delay | `fct_route_reliability_daily` | delay distribution, not just the pass rate | p90 is the trip you plan around |
| EWT (excess wait time) | `fct_route_reliability_daily` | actual average wait minus scheduled average wait, `Σgap²/2Σgap` | on frequent service this *is* reliability |
| Bunching / big-gap % | `fct_headways` | gap < 0.5× or > 1.5× scheduled headway | the mechanism behind bad EWT |
| Completeness % | `fct_service_delivery_daily` | observed trips ÷ scheduled trips | the bus that never came |
| Early departure % | `fct_stop_events` | bus left a timepoint > 60s early | you got to the stop on time and still missed it |
| Cancellations | `fct_service_delivery_daily` | feed-declared CANCELED trips | explicit service withdrawal |
| Alert minutes | `fct_alerts_daily` | disruption exposure per route-day | context for a bad day |
| Weather sensitivity *(collected, not yet joined)* | `silver.weather_hourly` | reliability vs precipitation/snow | resilience, not just fair-weather scores |
| Composite score | `fct_city_scorecard_monthly` *(P6, not built)* | weighted: wait .35, otp .30, cancel .20, bunch .15 | the headline ranking |

---

## 2. The shape of the system in one paragraph

Per-city **pollers** hit agency GTFS-Realtime endpoints every 30 seconds, archive the raw
protobuf bytes to S3, and publish decoded **canonical envelopes** to Kafka. An hourly
**Spark** job on EMR Serverless drains Kafka into **Iceberg** tables on S3 — bronze
(envelopes as received) and silver (typed, deduped, one schema for every city). Static
GTFS schedule zips are downloaded weekly and parsed by a second Spark job into silver
schedule tables. **Snowflake** reads all of silver as external Iceberg tables — no copy.
**dbt** then does the analytics: match each realtime trip to its scheduled trip, finalize
one row per stop visit, and roll up to daily reliability marts in **GOLD**. **Dagster**
runs the whole chain on a schedule and asserts freshness; a Streamlit dashboard (P6) is
the presentation layer.

```
agency API ─poller─> Kafka ─Spark─> Iceberg silver ─external table─> Snowflake ─dbt─> GOLD marts ─> charts
     └─────────────> S3 raw archive (replay)          ▲
static GTFS zip ────> S3 ──Spark parse────────────────┘
Open-Meteo ─────────> Snowflake silver.weather_hourly ┘
```

---

## 3. The tools, and why each one is here

| Tool | Role | Why this one |
|---|---|---|
| **Python 3.12 + `uv`** | pollers, Dagster assets, scripts | one language for ingest + orchestration; `uv` for fast, locked deps |
| **Docker** | one image for all city pollers | a city is a YAML file, not a new deployment |
| **Kafka** (single broker, EC2) | buffer between polling and processing | decouples a 30s poll cadence from an hourly batch; lets Spark replay by offset. Redpanda locally, Kafka in prod |
| **Spark Structured Streaming** on **EMR Serverless** | bronze + silver normalization | exactly-once via checkpoints, stateful dedup, and it scales to the Zurich national feed. Serverless because the work is bursty — we pay per drain, not per hour |
| **Iceberg on S3** (+ Glue) | the lakehouse | open table format: Spark writes, Snowflake reads the *same files*. Schema evolution and snapshot isolation without a copy step |
| **Snowflake** | warehouse / query engine | external Iceberg tables for silver (zero-copy), native tables for GOLD. `TRANSFORM_XS` runs only while dbt runs |
| **dbt** | all business logic | SQL under version control with tests; every metric definition is one model, reviewable in a diff |
| **Dagster** | orchestration | asset-aware scheduling, `dagster-dbt` integration, freshness checks as first-class objects |
| **Terraform** | all AWS + Snowflake infra | nothing is clicked in a console; the runbook is the code |
| **Streamlit + pydeck** *(P6)* | presentation | fastest path from a Snowflake query to an interactive map |
| **DuckDB** (`dbt-duckdb`) | local dev target | run the whole dbt project on a laptop against MinIO, no cloud cost |

---

## 4. Stage by stage

### 4.1 Extract — pollers

**Code:** `ingestion/poller.py`, `ingestion/adapters/*.py`, config in `ingestion/config/cities/<city>.yaml`.
**Runs as:** one container per city on the services EC2 box (`transit-poller-<city>`).

A city config is the entire per-city surface:

```yaml
city: nyc
agency: MTA-NYCT
timezone: America/New_York
adapter: gtfs_rt
poll_seconds: 30
static_gtfs: https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip
feed_groups:
  ace: https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace
  ...
```

Each poll does three things, in this order:

1. **Archive first.** Raw response bytes go to
   `s3://…-raw/<city>/<endpoint>/<YYYY-MM-DD>/<HH>/<YYYYMMDDTHHMMSS>Z.pb`, unmodified. This is the
   replay layer: every silver table can be rebuilt from here without touching an agency
   API again. Archive-before-decode means a decode bug never loses data.
2. **Decode** the protobuf into the **canonical envelope** (below).
3. **Publish** to Kafka, chunked so no message exceeds 900 KB.

**The canonical envelope** — every adapter emits this shape, so downstream code never
branches on city:

| Field | Meaning |
|---|---|
| `city`, `agency` | which city / operator |
| `feed` | entity kind: `trip_updates`, `vehicle_positions`, `alerts` |
| `endpoint` | which of the city's URLs it came from (NYC has 8) |
| `source_format` | `gtfs_rt`, `odpt_json` |
| `schema_version` | envelope version (currently 2), for replay compatibility |
| `feed_ts` | the feed's own header timestamp |
| `fetched_at` | when *we* pulled it — the honest observation time |
| `payload` | list of decoded entities |

`fetched_at` matters more than it looks: it drives dedup watermarks, prediction
staleness, and the `stale_observation_flag`. A feed's own timestamp can lie or freeze;
ours cannot.

**Per-city quirks handled at this layer** (full list in `01-data-dictionary.md` §B–C):

| City | Quirk | Handling |
|---|---|---|
| NYC | 8 separate feeds; `arrival.delay` absent except the L feed; VP has no lat/lon; trip_ids encode origin time (`070950_A..S58R`) | delay computed against static schedule; dedicated origin-time matcher |
| Helsinki | `trip_id` is **empty string** | trip identity = (route, direction, start_date, start_time) |
| Toronto | ~3% ADDED trips (negative ids); subway publishes alerts only | ADDED counts for volume, not OTP; scored on surface modes (streetcar + bus) |
| DC | ships **two** static zips (rail + bus); ~30% of stop updates are SKIPPED | multi-source static under one version; SKIPPED scores no band |
| SF Bay | 511 regional feed, ~78k stop updates per poll; must request gzip; 60 req/hr cap | 200s cadence, `agency=RG`, size-aware chunking |
| Zurich | endpoint serves the **whole Swiss network** | silver filters to a Zurich route allow-list; poller stays off until the list exists |
| Tokyo *(P4)* | `odpt:delay` is operator-stated | that value is authoritative, `finalization_method='odpt_stated'` |

### 4.2 Buffer — Kafka

Three topics, keyed by city: `transit.trip_updates`, `transit.vehicle_positions`,
`transit.alerts`. Keying by city means one city's backlog can't reorder another's.
Kafka is a buffer, not storage — retention is short, and the S3 raw archive is the
durable copy.

### 4.3 Transform — Spark bronze + silver

**Code:** `spark_jobs/silver_normalize.py` (drain), `spark_jobs/gtfs_static_parse.py` (schedules).
**Runs as:** EMR Serverless app `transit-pulse-streaming`, triggered by Dagster's
2-hour chain. The hourly EventBridge->Lambda drain still exists but is DISABLED
since 2026-08-26 (cost cut — see the rev comment in `infra/modules/spark/main.tf`);
the chain adopts any in-flight run rather than starting a second one.

One job starts **four concurrent streaming queries** — bronze envelopes, silver stop-time
predictions, silver vehicle positions, silver alerts — all with
`Trigger.AvailableNow`: drain everything currently in Kafka, then exit. Each query owns a
checkpoint under `s3://…-lakehouse/checkpoints/`, and **Structured Streaming permits
exactly one writer per checkpoint** — which is why concurrent drains are prevented at
every layer (Lambda guard, Dagster adoption).

What silver normalization does:

- explodes the envelope into one row per stop-time update / vehicle position / alert;
- applies the **canonical rules** (§5) — `trip_uid`, `service_date`, UTC storage;
- **deduplicates** with a 2-hour watermark (`dropDuplicatesWithinWatermark`) — a 30s poll
  sees the same prediction many times, and only revisions are interesting;
- **filters national feeds** to their city allow-list (Zurich). If the allow-list file is
  missing, that city's rows are **dropped on purpose** — filing every Swiss operator
  under `city='zurich'` would silently produce wrong answers, and wrong is worse than absent.

Silver tables (Iceberg, partitioned by `city` / `service_date`):

| Table | Grain | Key columns |
|---|---|---|
| `silver.stop_time_predictions` | one prediction revision per (trip, stop, fetch) | `city, service_date, trip_uid, stop_id, stop_sequence, arr_pred_ts_utc, arr_delay_sec, sched_arr_ts_utc, fetched_at, stu_schedule_relationship` |
| `silver.vehicle_positions` | one position report | `city, trip_uid, vehicle_id, lat, lon, bearing, occupancy_status, fetched_at` |
| `silver.alerts` | one alert revision | `city, alert_id, effect, cause, informed routes/stops, active periods` |
| `silver.gtfs_static_*` | schedule, per `gtfs_version_id` | trips, stop_times, routes, stops, calendar, calendar_dates, shapes |
| `silver.weather_hourly` | city × local hour (native Snowflake table, not Iceberg) | `temp_c, precip_mm, snowfall_cm, wind_kph, weather_code` |

### 4.4 Static GTFS — the schedule side

Realtime tells you when a vehicle *did* arrive. Only the static schedule tells you when it
*should* have. Weekly (Sun 09:00 UTC) Dagster downloads each city's zip(s) on the services
box, uploads them to S3 under a shared **`gtfs_version_id = <city>-<UTC date>-<sha8>`**, and
launches a Spark job to parse them into the canonical silver schedule tables. Facts carry
the `gtfs_version_id` they were scored against, so a schedule change never silently
rewrites history.

Two subtleties worth knowing before you touch this:

- A city can have **several source zips** under one version (DC: rail + bus). The parser
  conforms each source to the canonical column set, then unions.
- The parser **drops and recreates** a table whose columns drifted, which mints a new
  Iceberg table UUID — Snowflake's external table then refuses to refresh. The Dagster
  refresh step catches that and rebinds with `create or replace iceberg table`.

### 4.5 Load — Snowflake

`TRANSIT.SILVER` is **external Iceberg tables**: Snowflake reads the same parquet files
Spark wrote, no copy, no load job. The catch is that an external table is pinned to a
metadata file, so it must be **refreshed** after every drain — that is a step in the
Dagster chain, not something the warehouse does on its own. If silver looks hours stale in
Snowflake but the S3 data is fresh, the refresh is what failed.

`TRANSIT.GOLD` holds dbt's output as native Snowflake tables.

### 4.6 Transform — dbt (where all the business logic lives)

Three layers, one job each:

**staging** (views, 1:1 with silver) — rename to canonical names, cast types, no logic.

**intermediate** (tables) — the hard part:

- `int_service_dates` — which service_ids run on which dates (calendar + calendar_dates exceptions).
- `int_trip_matching_nyc` — NYC's origin-time trip ids matched on (route, direction, service_date, origin time).
- `int_trip_matching_generic` — every other city. Exact `trip_id` join for Boston / DC / Zurich / SF; route+direction+start_time for Helsinki (empty trip_ids); fuzzy for Toronto.
- `int_trip_matching` — union of the two, one row per matched trip with a `match_confidence`.
- `int_gtfs_scheduled_stop_times` — the schedule expanded to real timestamps per service date (handles GTFS times past midnight like `25:30:00`).
- `int_service_frequency` — scheduled headway per route/direction/hour; decides frequent vs timetabled service.
- **`int_stop_events_finalized`** — *the* model. Collapses many prediction revisions into one finalized stop visit.

How finalization works, because it is the model most likely to confuse you:

1. Join predictions to their matched static trip and scheduled stop time.
2. Drop revisions fetched more than `prediction_staleness_min` (30) after their own
   predicted time — those are stale re-statements of a stop already passed.
3. Finalize only events at least `finalize_horizon_min` (20) behind the freshest
   `fetched_at` **in the data** — a data-driven watermark, so the model is replayable.
4. The **last surviving prediction** stands in for the actual arrival (v1 method:
   `last_prediction`). Vehicle-position-based finalization is deliberately not used —
   NYC's VP `current_stop_sequence` is unreliable.
5. `delay_arr_sec = COALESCE(feed delay, actual − scheduled)`.

**marts** (tables) — the star schema in §6.

### 4.7 Present

Snowsight today. P6 adds Streamlit + pydeck: a city scorecard, a route drill-down, a map
of delay by stop, and a weather-resilience view. Charts read GOLD only — never silver,
never the lake.

---

## 5. Canonical rules (implemented once, obeyed everywhere)

These four rules are what make nine cities comparable. Changing one changes every number
in the warehouse.

| Rule | Definition | Why |
|---|---|---|
| `trip_uid` | `hash(city, service_date, COALESCE(trip_id, route_id‖'-'‖direction_id‖'-'‖start_time))` | Helsinki has no trip_id; Toronto reuses ids across days. One stable identity per trip per day |
| `delay_pred_sec` | `COALESCE(arrival.delay, arrival.time − scheduled_arrival)`; Tokyo uses stated delay | use the agency's own number when it publishes one, compute it when it doesn't |
| `service_date` | feed `start_date` if set; else `local_ts − 12h`, date part (GTFS noon rule). Per-city fallback where the feed *never* sets it — Toronto uses −4h | a 00:30 train belongs to the previous service day; Toronto's −12h default misdated the whole midnight-to-noon half day |
| time zones | store UTC, **analyse local** via `dim_city.iana_tz` | comparing peak hours across continents is meaningless in UTC |

Atomic fact unique key: `(city_key, service_date, trip_uid, stop_sequence)`.

---

## 6. Warehouse schema (GOLD)

One dimension, six facts. `fct_stop_events` is the atomic fact — everything else is a
rollup of it, so if a number looks wrong, start there.

### `dim_city` (seed — 7 rows)

`city_key` · `city_name` · `country` · `iana_tz` · `peak_am_start/end` · `peak_pm_start/end` ·
`rt_delay_source` · `crowding_usable` · `attribution_text` · `metrics_from` · `schedule_matchable`

Two columns carry policy, not description:
- **`metrics_from`** — the city's first *full* service day of polling. Completeness is not
  judged before it (a poller that came up at 00:07 local scores a real but meaningless 2%).
- **`schedule_matchable`** — false when a city's realtime stop ids don't share a namespace
  with its own published schedule (Toronto). False ⇒ no OTP band, ever.

### `fct_stop_events` — atomic fact
**Grain:** one finalized stop visit. **Unique key:** `(city_key, service_date, trip_uid, stop_sequence)`.

| Group | Columns |
|---|---|
| keys | `event_key`, `city_key`, `service_date`, `trip_uid`, `stop_sequence` |
| identity | `trip_id_raw`, `static_trip_id`, `route_id`, `direction_id`, `stop_id`, `vehicle_id`, `match_confidence` |
| local time | `local_date`, `local_hour`, `local_dow`, `is_peak` |
| timestamps (UTC) | `sched_arr_ts_utc`, `sched_dep_ts_utc`, `actual_arr_ts_utc`, `actual_dep_ts_utc` |
| measures | `delay_arr_sec`, `delay_dep_sec`, `otp_band` |
| flags | `early_departure_flag`, `stale_observation_flag`, `cancelled_flag`, `skipped_flag` |
| provenance | `schedule_relationship`, `finalization_method`, `prediction_count`, `first_seen_utc`, `last_seen_utc`, `gtfs_version_id`, `source_format` |

`otp_band` ∈ {`early`, `on_time`, `late`, `very_late`, **NULL**}. NULL is meaningful: it
removes the row from both the numerator *and* the denominator of every punctuality
number, while the row still counts as service volume. See §7.

### The rollups

| Mart | Grain | Notable columns |
|---|---|---|
| `fct_headways` | consecutive arrivals at a stop | `actual_gap_sec`, `sched_headway_sec`, `gap_ratio`, `bunched_flag`, `big_gap_flag`, `daypart` |
| `fct_service_delivery_daily` | city × route × service_date | `trips_scheduled`, `trips_observed`, `trips_added`, `trips_cancelled`, `completeness_pct`, `metrics_from` |
| `fct_route_reliability_daily` | city × route × direction × service_date | `otp_pct`, `early/late/very_late_pct`, `mean/median/p90_delay_sec`, `ewt_sec`, `bunching_pct`, `big_gap_pct`, `cancel_pct`, `completeness_pct`, `scheduled_trips`, `observed_trips` |
| `fct_alerts_daily` | city × route × service_date | `alerts_active`, `alert_minutes`, `worst_effect` |
| `fct_vehicle_activity_hourly` | city × mode × service_date × local_hour | `distinct_vehicles`, `distinct_trips_active`, `distinct_routes_active`, `vehicle_id_reliable` |
| `fct_city_scorecard_monthly` | city × month — **P6, not built** | composite score, versioned weights |

### Lineage

```
silver.stop_time_predictions ─> stg_gtfsrt__trip_updates ─┬─> int_service_dates ─┐
silver.vehicle_positions ────> stg_gtfsrt__vehicle_positions │                    │
silver.alerts ───────────────> stg_gtfsrt__alerts            │                    │
silver.gtfs_static_trips ────> stg_gtfs__trips ──────────────┼────────────────────┤
silver.gtfs_static_stop_times> stg_gtfs__stop_times ─────────┤                    │
silver.gtfs_static_routes ───> stg_gtfs__routes              │                    │
silver.gtfs_static_calendar ─> stg_gtfs__calendar ───────────┘                    │
silver.gtfs_static_calendar_dates > stg_gtfs__calendar_dates ─────────────────────┘
                                                    │
        ┌───────────────────────────────────────────┴──────────────────┐
        v                          v                                   v
int_trip_matching_nyc     int_trip_matching_generic          int_gtfs_scheduled_stop_times
        └───────────┬──────────────┘                              │        │
                    v                                             │        v
             int_trip_matching ───────────┐                       │  int_service_frequency
                                          v                       │        │
                          int_stop_events_finalized <─────────────┘        │
                                          │                                │
                                          v                                │
      dim_city (seed) ────────────> fct_stop_events                        │
                                     │      │      │                       │
             ┌───────────────────────┘      │      └──────────────┐        │
             v                              v                     v        │
      fct_headways              fct_service_delivery_daily        │        │
             └────────────────┬─────────────┘                     │        │
                              v                                   │        │
                   fct_route_reliability_daily <──────────────────────────-┘
      stg_gtfsrt__alerts ──> fct_alerts_daily
      stg_gtfsrt__vehicle_positions ──> fct_vehicle_activity_hourly
```

---

## 7. What we filter, and why

This is the section to read before you trust a number. Every exclusion below is
deliberate, and each one is enforced in exactly one place.

| # | Filter | Where | Effect | Rationale |
|---|---|---|---|---|
| 1 | **Dedup**, 2-hour watermark | Spark silver | drops re-sent identical predictions | a 30s poll re-reads the same prediction ~120× |
| 2 | **National-feed allow-list** | Spark silver `national_filter` | keeps only Zurich-area routes; **drops the city entirely if the list is missing** | the Swiss endpoint is national; unfiltered it would file every operator as "zurich" |
| 3 | **Stale prediction revisions** (> 30 min after their own predicted time) | `int_stop_events_finalized` | not eligible to finalize an event | they restate a stop already passed |
| 4 | **Finalization horizon** (20 min) | `int_stop_events_finalized` | events too recent aren't finalized yet | still-active stops keep getting revised |
| 5 | **ADDED trips** | `fct_service_delivery_daily` / OTP | count as service volume, **excluded from OTP** | an unscheduled extra bus has no scheduled time to be late against — but it *is* service delivered |
| 6 | **SKIPPED stops** → `otp_band = NULL` | `fct_stop_events` | out of both OTP numerator and denominator | the vehicle never served the stop (WMATA bus marks ~30% of updates SKIPPED) |
| 7 | **Stale observations** (last seen > 60 min before the event) → `otp_band = NULL` | `fct_stop_events` | out of OTP, still counted as volume | that's a schedule echo, not an observation. HSL publishes trips up to 3 days ahead |
| 8 | **`schedule_matchable = false`** → `otp_band = NULL` | `fct_stop_events` | city scores volume, headways and wait regularity, but no punctuality | Toronto: RT and static agree on 0–6 of ~200 stops per route, so any "match" is a numeric collision and its delay is fiction |
| 9 | **`metrics_from`** | `fct_service_delivery_daily` + tests | completeness not judged before a city's first full day | a partial first day is arithmetic, not a defect |
| 10 | **`lookback_hours` = 48** | incremental models | each run reprocesses 2 days | late-arriving predictions still land; full history stays stable |
| 11 | **Early bus departure > 60s at a timepoint** | `fct_stop_events` | flagged as a failure, not "extra on-time" | leaving early makes the bus unmissable-early, which is a service failure |

One more is **declared but not yet wired**: `completeness_exclusion_threshold` (0.70) is
set in `dbt_project.yml` for the P6 scorecard — the intent is that a route-day observed
below 70% is not ranked on punctuality — but no model reads it today. Don't cite it as an
active guard until the scorecard lands.

**The general principle:** when data can't answer a question honestly, the row stays
(so volume and coverage stay true) and the *metric* goes NULL — never a zero, never a
guess, and never a quietly loosened threshold.

---

## 8. What is actively running

**AWS account** 622221238588 · **region** us-east-2 · **prefix** `transit-pulse`

| Component | Where | Cadence | State (2026-08-24 21:00Z) |
|---|---|---|---|
| Pollers: nyc, boston, toronto, helsinki, dc, sf | services EC2 `i-0f0d6e32cb15ce471`, one container each | 30s (SF 200s) | running |
| Poller: zurich | services box | 60s trip updates / 600s alerts | **live since 2026-08-25 03:08Z** — allow-list shipped (615 routes) |
| Kafka broker | EC2 `10.20.0.34:9092` | always | running |
| EMR drain (`transit-drain`) | Lambda `transit-pulse-emr-drain` on EventBridge | hourly | **DISABLED 2026-08-26** (cost cut — chain drains instead; flip to ENABLED in spark module to restore) |
| Dagster 2h chain: drain → Iceberg refresh → dbt build → checks | services box | `5 */2 * * *` | running |
| Dagster freshness tripwire | services box | `10,25,40,55 * * * *` | running — detects a killed feed within ~55 min |
| Dagster weather asset | services box | `20 * * * *` | running |
| Dagster static refresh | services box | `0 9 * * 0` (Sun) | running |
| Snowflake `TRANSFORM_XS` | Snowflake `WQTEQYY-IB47757` | only while dbt runs | ~2.8 credits / 30 days |

**Current data state (2026-08-25 01:30Z):**

| Layer | Cities | Detail |
|---|---|---|
| Raw archive | 6 | every poll kept verbatim; the replay layer |
| Silver realtime | 6 | all deployed cities; backlog cleared, drains steady |
| Silver schedule | 7 | incl. Zurich (2.08M trips) and DC (rail + bus under one version) |
| GOLD | 6 | 3,446,955 stop events |
| **Scored on punctuality** | **6** | every deployed city |

| City | Events | Scored | OTP | Median delay |
|---|---|---|---|---|
| toronto | 1,250,592 | 96.8% | 49.7% | −23s |
| helsinki | 675,875 | 99.9% | 81.3% | +31s |
| boston | 468,435 | 94.2% | 59.0% | +112s |
| sf | 430,709 | 96.3% | 58.9% | +52s |
| nyc | 362,241 | 94.2% | 69.9% | +14s |
| dc | 259,103 | 100% | 53.8% | +81s |

dbt: 118 pass, 4 warn, 1 error (the completeness floor, §9).

**Storage:** S3 raw ~17 GB · lakehouse ~11 GB (checkpoints 7.3 GB of it) · artifacts ~3 GB ·
Snowflake GOLD ~150 MB. **Cost** ≈ $150/mo at list price, EMR the largest line — it tracks the
*number* of drains, not the volume, because most of a run is Spark start-up.

## 9. What's missing, in plain language

Nothing is broken as of 2026-08-25 01:30Z. All six deployed cities ingest, drain, and score.
What follows is what the warehouse still cannot tell you, and why. Read this before quoting
any number to anyone.

### Data we will never get from the feed we poll

**Helsinki ferries — 3 routes, 149 trips a day, zero coverage.**
HSL does not put ferries in its GTFS-RT feed at all. Suomenlinna ferry positions come from
AIS ship tracking, piped into HSL's separate HFP (MQTT) stream — they maintain a dedicated
`suomenlinna-ferry-hfp` service just for it. No change on our side produces ferry data from
the feed we poll. Getting it means subscribing to their MQTT stream, which this project
currently treats as stretch-only. Until then Helsinki's completeness is measured against a
denominator that includes 149 trips we cannot observe.

**Helsinki tram line H — 187 trips a day, 69 stops, zero coverage.**
A full passenger line running 04:00–02:00 that has never produced a single realtime row.
It is not an id mismatch: only 2 of Helsinki's 428 realtime route ids are orphans, and its
sibling variants (1007H, 1001H, 1004H) all report normally. Something on HSL's side is not
publishing it. This one needs an email to HSL, not a code change.

**[rev 2026-08-25] Ferries are the systematic hole, not just Helsinki's.** Measured across
every city: Boston has 9 ferry routes scheduled and 0 ever observed; SF has 14 scheduled and
only 6 observed. Same shape as Helsinki — agencies publish ferry realtime outside GTFS-RT.
Helsinki's is confirmed agency-side; Boston's and SF's have not been separately confirmed.

**[rev 2026-08-25] SF's 109 unobserved bus routes are six operators, not one bug.** The 511
feed aggregates ~30 operators under one `city_key`, and they do not all publish realtime: CM
(20 routes), PE (11), UC (6), GF (5), MB (4) and SS (3) file schedules and nothing else. The
large operators run 84–100% coverage (AC Transit 112/123, SamTrans 69/75, Muni 58/68, VTA
55/67). Any SF number therefore averages a well-instrumented core with a handful of blind
operators — say so wherever one is quoted. Full breakdown in `docs/06-business-answers.md`.

**[rev 2026-08-25] Toronto's subway and NYC's buses are scope decisions, not gaps.** TTC
publishes alerts only for the subway, so Toronto is a surface-mode score by design. NYC buses
are deliberately out of scope. Both are stated in the plan; neither is a defect to chase.

### Collected but not yet used — optionality, not debt

**[rev 2026-08-26] This section used to be framed as a list of loose ends. That framing
was wrong and worth correcting**, because it changes what these fields are for.

The reliability question is what justified building the pipeline, but it is not the limit
of what the data can answer. Every stop event carries local hour, day of week, a peak
flag, mode, route, stop, direction, vehicle identity and — where the feed provides it —
occupancy. Alongside hourly weather per city, that is a substrate for questions nobody has
asked yet: how service volume responds to weather, whether crowding tracks delay, how
overnight coverage differs between cities, how a network's shape changes across the day.

Those are not the business question. They are the questions the business question makes
*possible*, and they are why it is worth keeping fields the current models do not read.
The distinction that matters is between **unused** (a field nothing reads because nothing
has needed it yet — keep it) and **dead** (config that reads as an active guard and is
not — delete it, as `completeness_exclusion_threshold` was on 2026-08-25).

**Weather.** `silver.weather_hourly` fills every hour and no dbt model reads it yet. The
resilience question — does Zurich handle snow better than New York — needs roughly two
years of accrual before it means anything, so the series is banking against that. The
2-year historical backfill (`make p5-backfill-weather`) is built and has never been run;
weather is the one dimension here that *can* be backfilled, because Open-Meteo publishes
a real archive.

**`stg_gtfs__stops`.** Built and populated with names and coordinates. No consumer today;
it is what any map, corridor analysis or stop-level view would be built on.

**Route geometry** (`silver.gtfs_static_shapes`, 7.1M rows). Same shape: loaded for the
P6 map, unread until then.

### A test that measures the wrong thing

The completeness floor fails any route-day below 50%. On a route with four scheduled trips,
missing two is a 50% failure — which is noise, not a finding. 15 route-days currently fail
and most are this. The two that matter are the real gaps above. Fixing this means requiring
a minimum denominator before a route is judged, which changes what the number means, so it
needs a decision rather than a quiet edit.

### Cities not yet contributing

**Zurich.** Live since 2026-08-25. Its schedule is parsed (2.08M national trips, filtered to 615 Zurich routes on BOTH the realtime and schedule sides) and the poller is running. Its feed is
the entire Swiss network; without the route allow-list every Swiss operator would be filed
under `city='zurich'`, and archiving the unfiltered feed at 30s runs ~0.5–1 TB/month.
Sequence: generate the allow-list from the parsed static, then start the poller.

**Chicago.** CTA's GTFS-RT beta key is not active. Retested 2026-08-24: still `errCd 101`.

**Tokyo.** ODPT account awaiting admin approval. The adapter is specified but unbuilt.

### Not built yet

SCD2 dimensions, the composite city scorecard with versioned weights, and the Streamlit
dashboard are all P6. A credible ranking also needs roughly 30 days of multi-city accrual —
right now the warehouse holds two to three days.

### One lesson worth internalising

Toronto spent a day recorded as "cannot be ranked on punctuality — its realtime and static
feeds disagree on stop numbering". That was true, and the conclusion was wrong: TTC publishes
**two** GTFS files, and we were reading the one that does not pair with the realtime feed.
Against the right file (`SurfaceGTFS.zip`) trip ids match 100% and stops 99.6% per route, and
Toronto now scores 96.8% of 1.25M events. Treat every coverage gap as our bug until the
agency's own documentation proves otherwise.

---

## 10. Working on it

### Local

```bash
make up               # compose: Redpanda, MinIO, Dagster, dbt-duckdb
make record-fixtures  # ONE live snapshot per open feed into tests/fixtures/*.pb
make test             # pytest — every unit test decodes a fixture, never the network
make lint             # ruff
make spark-local      # silver normalize against local Redpanda + MinIO
make gtfs-static      # parse schedule zips locally
make dbt-build        # dbt against duckdb
make poll-nyc         # (also poll-boston/-toronto/-helsinki/-dc/-sf/-zurich)
                      # explicit live polls — the only sanctioned way to hit an agency
```

**Fixture-first is a rule, not a preference.** Minimum poll interval is 30s per feed;
nothing in a test or a loop may hammer an agency endpoint. Agencies can and do revoke
access.

### Cloud

```bash
make infra-plan       # terraform plan — ALWAYS before apply
make infra-apply      # only after the plan is reviewed and approved
make deploy-images    # build + push ingestion and dagster images to ECR
make dagster-deploy   # restart the services box onto the new image
make emr-drain        # submit a drain by hand
make snowflake-refresh # re-pin the external Iceberg tables
```

Health checks, ids, and every recovery procedure are in `docs/operations.md`. Two habits
that save hours:

- **Read the driver log's *first* exception.** EMR retries a failed driver inside the same
  container, so the reported failure is usually the retry's ("log directory already
  exists"), not the cause.
- **Check `commits/` vs `offsets/` in the checkpoint** to see whether a drain is actually
  progressing. Equal-numbered = healthy; offsets ahead = a batch that never finished.

### Conventions

- Secrets live in `.env` / SSM only (`CTA_API_KEY`, `WMATA_API_KEY`, `BAY511_API_TOKEN`,
  `SWISS_OTD_TOKEN`, `ODPT_CONSUMER_KEY`). Never printed, never committed.
- No `terraform apply` without showing the plan and getting explicit approval.
- **If a dbt test fails on real data, investigate the data first.** Loosening a threshold
  requires a doc amendment explaining why. Every filter in §7 exists because the data
  justified it — not because a test was inconvenient.
- Never invent a feed URL. If it isn't in `01-data-dictionary.md` §B–C, verify it first.
- Docs are the source of truth. When reality diverges, amend the doc in the same PR.
