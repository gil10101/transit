# Transit Pulse — Global Transit Reliability Warehouse

**Business question:** Which cities run the most reliable public transit — and what makes them reliable?

**Cities (9):** New York, Chicago, Washington DC, Boston, SF Bay Area, Toronto, Zurich, Helsinki, Tokyo.

**Stack:** Terraform · Kafka · Spark Structured Streaming · S3/Iceberg · Snowflake · dbt · Dagster · Docker · Streamlit+pydeck. (k8s optional, Phase 8.)

**Headline stats for the README:** 9 cities · 3 continents · 3 real-time formats (GTFS-RT protobuf, ODPT JSON, MQTT stream) · ~10–20M events/day · one composite reliability score.

---

## 1. Sub-questions the warehouse answers

1. **On-time performance** — % of stop arrivals early / on-time / late per city, route, mode, local hour. Signed delay (early is negative and tracked separately — early *departures* at bus timepoints are a service failure, not a win).
2. **Average delay** — mean, median, p90 delay by city × route × local hour × day-of-week.
3. **Service volume** — how many vehicles/trips/routes are actually running per city per local hour; service span (first/last departure), overnight service coverage.
4. **Frequency & headways** — scheduled vs actual gaps between consecutive vehicles; bunching rate (gap < 0.5× scheduled) and big-gap rate (> 2× scheduled).
5. **Excess Wait Time (EWT)** — for frequent services (headway ≤ 10 min) where nobody checks a schedule, the metric that matters: how much longer riders actually wait vs what the schedule promises. This is TfL's official metric and the key to a fair NYC-vs-Zurich comparison.
6. **Cancellations & disruptions** — cancelled/skipped trips, alert frequency and duration.
7. **Weather sensitivity** — how much does reliability degrade per mm of rain / cm of snow, per city (Helsinki in snow vs Boston in snow is a great chart).
8. **Data completeness** — observed trips vs scheduled trips per feed, tracked as a first-class metric (feeds differ in quality; measuring your sources is part of the answer).

All of it rolls up into **`fct_city_scorecard_monthly`** — a 0–100 composite reliability score per city, with Tokyo's official government delay statistics loaded as an external benchmark to validate our measurements.

---

## 2. Data sources

### 2.1 Real-time feeds by city

| City | Agency / system | Modes covered (RT) | Format | Auth | Cadence | Notes |
|---|---|---|---|---|---|---|
| New York | MTA subway | Subway (all lines) | GTFS-RT (8 line-group feeds: ACE, BDFM, G, JZ, NQRW, L, 1–7, SIR) | None | poll 30s | VehiclePositions lack GPS coords (stop-relative only); uses NYCT protobuf extension |
| New York | MTA Bus Time | Bus (optional, Phase 2+) | GTFS-RT | Free key | poll 60s | Adds ~5,800 vehicles if wanted |
| Chicago | CTA | Rail ('L') + bus | Official GTFS-RT: trip updates, vehicle positions, alerts (transitdata.transitchicago.com) | Free | poll 30–60s | Train Tracker / Bus Tracker JSON APIs (free key) kept as backup/enrichment |
| Washington DC | WMATA | Metrorail + Metrobus | GTFS-RT (rail + bus: TU/VP/Alerts) | Free key (developer.wmata.com, 50k calls/day default) | poll 30s | Well within quota at 6 endpoints × 30s |
| Boston | MBTA | Subway, light rail, bus, ferry | GTFS-RT (TU/VP/Alerts) + V3 JSON:API | Free key | poll 30s | Cleanest docs of the US agencies |
| SF Bay Area | 511.org (regional) | ~30 agencies: BART, Muni, Caltrain, AC Transit… | Aggregated GTFS-RT (TU/VP/ServiceAlerts), `agency=RG` = whole region in one call | Free token | poll 90–120s | Default limit 60 req/hr — use the RG feed + slower cadence, or email for a limit increase |
| Toronto | TTC | Bus + streetcar (largest streetcar network in the Americas) | GTFS-RT at bustime.ttc.ca/gtfsrt/{trips,vehicles,alerts} | None (attribution required: Open Government Licence – Toronto) | poll 30–60s | **Subway = alerts only, no RT positions/predictions.** Scoped honestly: Toronto is scored on surface modes; subway gap recorded in the completeness matrix |
| Zurich | opentransportdata.swiss | All operators nationally (SBB, VBZ trams/buses, …) | GTFS-RT (national feed) | Free token | poll 30–60s | Filter to Zurich: agency allow-list (VBZ + ZVV-area operators + SBB S-Bahn) + stop bounding box. National static GTFS is big — parse with Spark, not pandas |
| Helsinki | HSL / Digitransit | Metro, tram, bus, ferry, commuter rail | GTFS-RT trip updates + service alerts over HTTPS; **vehicle positions via MQTT (HFP feed, mqtt.hsl.fi) at ~1s frequency** | Free registration (Digitransit API key where required) | MQTT = push; TU poll 30s | The one city with true push streaming — MQTT→Kafka bridge is a showcase component. HFP firehose gets downsampled in Spark |
| Tokyo | ODPT (Public Transportation Open Data Center) | Tokyo Metro (9 lines) + Toei subway/tram/liner (all permanent); Toei bus | GTFS-RT **and** ODPT JSON (`odpt:Train` — includes operator-stated delay in seconds; `odpt:TrainInformation` status) | Free developer registration → consumerKey | poll 30–60s | Full Tokyo subway proper is permanently open. JR East (Yamanote etc.) GTFS-RT is Challenge-window-limited → optional stretch, not core. Prefer `odpt:delay` (authoritative) over prediction-derived delay where both exist |

### 2.2 Static / reference sources

| Source | What | Format | Refresh |
|---|---|---|---|
| Static GTFS per agency (all 9 cities) | Schedules: routes, trips, stops, stop_times, calendars, shapes | GTFS zip (CSV) | Weekly pull, versioned in S3, SCD2 via dbt snapshots |
| Open-Meteo | Hourly weather per city centroid: temp, precip, snowfall, wind, weather code. Historical archive API goes back decades → multi-year weather backfill on day one | JSON REST, **no key, free** | Hourly + one-time backfill |
| MLIT (Japan transport ministry) delay statistics | Monthly delay-certificate days per Tokyo-area line — the official reliability record | CSV/manual → dbt seed | Quarterly manual |
| dim_city seed | IANA timezone, country, population, currency of modes, peak-hour definitions | dbt seed | Static |
| Agency licensing/attribution registry | TTC, ODPT, 511, Swiss terms all require attribution or have terms | ATTRIBUTION.md + seed | Static |

**On 5–10 years of history:** no one publishes free granular multi-year real-time archives (Sydney's TfNSW is the rare exception and is on the stretch list partly for that reason). The plan: your own S3 bronze layer becomes the granular archive from day one, and the MLIT seed provides the multi-year official benchmark for Tokyo. After 3 months of running, you own a dataset that doesn't exist publicly — that's a feature, not a gap.

---

## 3. Source formats (what the raw data looks like)

**Family A — GTFS-RT protobuf (7 of 9 cities + Tokyo partially).** Binary protobuf, three entity types:
- `TripUpdate`: per trip, an array of `stop_time_update{stop_id, stop_sequence, arrival{time, delay}, departure{...}, schedule_relationship}`. These are *predictions* that revise every poll — each trip×stop accumulates ~10–20 snapshots before the vehicle passes. Finalization logic (picking the "actual") is a core pipeline feature, not an afterthought.
- `VehiclePosition`: vehicle_id, trip_id, lat/lon (except NYC subway), bearing, speed, current_status (STOPPED_AT / IN_TRANSIT_TO), stop_id, occupancy where supported.
- `Alert`: effect, cause, active periods, informed routes/stops, text.
- Quirks: NYC uses the NYCT extension (extra fields; parse or ignore explicitly). Trip_id matching between RT and static GTFS is imperfect in NYC — matcher falls back to (route, direction, origin departure time).

**Family B — ODPT JSON (Tokyo).** JSON-LD-ish REST objects. Key type `odpt:Train`: one object per active train with `odpt:delay` (seconds, operator-stated), current/next station, direction, line. Plus `odpt:TrainInformation` (per-line status text). Cleaner than predictions — the delay is authoritative — but stop/line IDs are ODPT URNs that need a mapping table to GTFS stop_ids (a small, honest data-integration task; you already accepted Tokyo costs extra work).

**Family C — MQTT stream (Helsinki HFP).** JSON messages pushed per vehicle per second on topic hierarchy `/hfp/v2/journey/...`: position, speed, odometer, door status, delay (seconds, signed). A small bridge service subscribes and republishes into Kafka; Spark downsamples 1s → 15s and extracts stop events from door/stop transitions.

**Canonical envelope** (what every adapter emits to Kafka, regardless of family):

```json
{
  "city": "tokyo", "agency": "TokyoMetro", "feed": "trip_updates",
  "fetched_at": "2026-08-22T13:31:04Z", "source_format": "odpt_json",
  "schema_version": 2, "payload": { ...normalized records... }
}
```

Raw bytes (protobuf/JSON as fetched) are also archived to S3 `raw/` hourly for replayability.

---

## 4. Time zones — you're right, and here's the exact policy

Your instinct is correct: **comparisons happen in local clock time.** 3am in Tokyo is compared to 3am in NYC, never to "the same UTC instant." Concretely:

1. **Store UTC everywhere** in bronze/silver (`event_ts_utc`). UTC is for correctness, joins, and dedup — never for analysis.
2. `dim_city` carries the IANA timezone (`America/New_York`, `Asia/Tokyo`, …). A dbt macro derives `*_local` timestamps, `local_date`, `local_hour`, `local_dow`, `is_weekend`, `is_peak` on every fact. All rankings, heatmaps, and the scorecard group by these local fields.
3. **DST** is handled automatically by IANA conversion (Tokyo has none; the others shift — the two 1am hours on fall-back night are a known, documented edge case, excluded from hour-level comparisons twice a year).
4. **GTFS service-day rule:** static schedules use times like `25:30:00` (1:30am belonging to the *previous* service day). Standard convention applied: `service_date` = local time minus 12h, date part. Every fact carries both `service_date` and `local_date` so late-night trips aggregate correctly.
5. Peak windows default to 07:00–10:00 and 16:00–19:00 local, overridable per city in `dim_city` (Tokyo's peak skews earlier/longer — that's a config value, not a code change).

The only UTC-aligned view in the whole project is the ops monitoring dashboard (feed freshness), where "now" actually means now.

---

## 5. Architecture

```
                ┌─────────────────────────── INGESTION (Docker, one small EC2 / ECS) ───────────────────────────┐
 GTFS-RT feeds ─┤ gtfs_rt_adapter (config-driven: nyc, chi, dc, bos, sf, tor, zrh, tokyo-gtfsrt)                 │
 ODPT JSON ─────┤ odpt_adapter (tokyo trains: odpt:Train + TrainInformation)                                     │
 HSL MQTT ──────┤ hsl_mqtt_bridge (subscribe /hfp/v2/#  → Kafka)                                                 │
                └───────────────┬────────────────────────────────────────────────────────────────────────────────┘
                                ▼
                        KAFKA (3 topics, keyed by city)
                        transit.trip_updates | transit.vehicle_positions | transit.alerts
                        + raw bytes archived hourly → s3://…/raw/
                                ▼
                SPARK STRUCTURED STREAMING (EMR Serverless)
                  job 1: bronze_writer   — Kafka → Iceberg bronze (append, checkpointed, exactly-once)
                  job 2: silver_normalize — canonical schema, dedupe identical consecutive predictions,
                                            enrich with static GTFS (broadcast join), HFP downsample 1s→15s
                                ▼
                S3 + APACHE ICEBERG (Glue catalog)  — bronze.* and silver.* tables,
                partitioned by city / service_date
                                ▼
                SNOWFLAKE  — external Iceberg tables over silver → dbt builds gold
                                ▼
                dbt (staging → intermediate → marts)  +  Dagster (schedules, sensors,
                city×date partitions, asset checks, dagster-dbt lineage)
                                ▼
                STREAMLIT + pydeck dashboard  ·  monthly auto-report (Dagster job → md/PDF)
```

**Component decisions (and the one-line why):**

| Component | Choice | Why |
|---|---|---|
| Warehouse | **Snowflake** | Best dbt DX, external Iceberg tables, auto-suspend keeps cost ~$25/mo; Terraform Snowflake provider makes the IaC multi-cloud (strong signal). Redshift Serverless is the drop-in alternative if you want an all-AWS story — everything else in the plan is unchanged. |
| Lake format | S3 + **Apache Iceberg**, Glue catalog | Lakehouse résumé signal, schema evolution, time travel, and both Spark and Snowflake speak it natively. Fallback if it fights you: plain Parquet + COPY, one day of rework. |
| Kafka | Local dev: **Redpanda** in Compose. Cloud: single-broker Kafka (KRaft) on a t4g.small (~$12/mo). | MSK Serverless is a one-variable Terraform swap if you want the managed-service line item; not worth $40+/mo by default. |
| Spark | **EMR Serverless** | Pay-per-second, zero cluster management, native Iceberg. |
| Ingestors + Dagster | Docker Compose on one t4g.medium EC2 (or ECS Fargate) | Nine pollers are tiny; one box runs them all + Dagster webserver/daemon/postgres. |
| Local dev | Full stack in Docker Compose: Redpanda, MinIO (S3 stand-in), Spark local, **dbt-duckdb** target | Entire pipeline runs on a laptop for free; cloud is the same code with different profiles. |
| CI/CD | GitHub Actions: terraform fmt/validate/plan on PR, apply on main; dbt **slim CI** (state deferral — only build changed models); docker build+push | dbt slim CI is a differentiator interviewers notice. |
| k8s | **Deferred to Phase 8** | Moving ingestors to EKS later is a clean, contained migration story; don't let it eat the project. |

---

## 6. Data model

### 6.1 Layers

- **raw/** (S3): exact bytes fetched, hourly folders. Replay insurance.
- **bronze** (Iceberg): decoded envelopes, no dedup beyond exact duplicates. Partition: `feed / city / service_date`.
- **silver** (Iceberg): canonical, typed, GTFS-enriched:
  - `silver.stop_time_predictions` — every prediction snapshot (city, trip_id, route_id, direction_id, stop_id, stop_sequence, vehicle_id, arr_pred_ts_utc, dep_pred_ts_utc, sched_arr_ts_utc, schedule_relationship, source_format, fetched_at)
  - `silver.vehicle_positions` — downsampled pings (city, ts_utc, vehicle_id, trip_id, route_id, lat, lon, bearing, speed, status, stop_id, occupancy)
  - `silver.alerts`
  - `silver.gtfs_static_*` — versioned schedule tables with `gtfs_version_id`
- **gold** (Snowflake, dbt): star schema below.

### 6.2 Dimensions

```
dim_city            city_key, city_name, country, iana_tz, population,
                    peak_am_start/end, peak_pm_start/end, modes_covered[],
                    launch_date, attribution_text
dim_agency          agency_key, city_key, agency_id, agency_name
dim_route   (SCD2)  route_key, city_key, agency_key, route_id, short_name,
                    long_name, mode (subway|lrt|bus|tram|ferry|commuter),
                    is_frequent_service (sched headway ≤10min flag), color,
                    valid_from, valid_to, is_current
dim_stop    (SCD2)  stop_key, city_key, stop_id, stop_name, lat, lon,
                    parent_station, h3_r8, h3_r9, valid_from, valid_to
dim_date            standard calendar
dim_time_local      local_hour, daypart (early_am|am_peak|midday|pm_peak|
                    evening|overnight), is_peak
dim_weather         weather_key, condition_bucket (clear|rain|heavy_rain|
                    snow|heavy_snow|fog|extreme), from Open-Meteo WMO codes
```
SCD2 on routes/stops via **dbt snapshots** over the versioned static GTFS — schedules change quarterly and delay math must join the schedule *that was in force that day*.

### 6.3 Facts

```
fct_stop_events        -- THE atomic fact. Grain: one finalized arrival per
                       -- city × trip × stop × service_date. ~2–4M rows/day.
  city_key, agency_key, route_key, stop_key, trip_id (degenerate),
  direction_id, stop_sequence, vehicle_id,
  service_date, local_date, local_hour, local_dow, is_peak,
  sched_arr_ts_utc, sched_dep_ts_utc, actual_arr_ts_utc, actual_dep_ts_utc,
  delay_arr_sec (signed; negative = early),
  delay_dep_sec, early_departure_flag (timepoint stops only),
  otp_band (early|on_time|late|very_late),
  schedule_relationship, cancelled_flag, skipped_flag,
  finalization_method (status_transition | last_prediction | vp_passage | odpt_stated),
  prediction_count, first_seen_utc, last_seen_utc,
  gtfs_version_id, source_format, data_quality_score

fct_headways           -- Grain: consecutive-vehicle gap per city × route ×
                       -- direction × stop.
  ..., prev_vehicle_id, vehicle_id, actual_gap_sec, sched_headway_sec,
  gap_ratio, bunched_flag (ratio<0.5), big_gap_flag (ratio>2.0)

fct_vehicle_activity_hourly   -- answers "how many are running & when"
  city_key, mode, service_date, local_hour,
  distinct_vehicles, distinct_trips_active, distinct_routes_active

fct_service_delivery_daily
  city_key, route_key, service_date,
  trips_scheduled, trips_observed, trips_cancelled, completeness_pct

fct_weather_hourly
  city_key, local_date, local_hour, temp_c, precip_mm, snowfall_cm,
  wind_kph, weather_key

fct_alerts_daily
  city_key, route_key, service_date, alerts_active, alert_minutes,
  worst_effect

fct_route_reliability_daily    -- first aggregate mart
  city_key, route_key, service_date, mode, is_frequent_service,
  otp_pct, early_pct, late_pct, early_departure_pct,
  mean_delay_sec, median_delay_sec, p90_delay_sec,
  ewt_sec (frequent routes), bunching_pct, big_gap_pct,
  cancel_pct, completeness_pct

fct_city_scorecard_monthly     -- the headline
  city_key, month, score_0_100, rank,
  s_wait, s_otp, s_cancel, s_bunching (components),
  frequent_service_share, completeness_pct, excluded_days

fct_benchmark_mlit_monthly     -- Tokyo official stats (seed) for validation
```

### 6.4 dbt project shape

```
models/
  staging/         stg_gtfsrt__trip_updates, stg_gtfsrt__vehicle_positions,
                   stg_gtfsrt__alerts, stg_odpt__trains, stg_hfp__positions,
                   stg_gtfs__{routes,stops,trips,stop_times,calendar},
                   stg_weather__hourly            (one interface per format family)
  intermediate/    int_stop_events_finalized      (the dedup/finalization logic:
                                                   window over predictions, pick by
                                                   status transition else last pred
                                                   before passage; incremental with
                                                   48h late-arriving lookback)
                   int_headways, int_trip_matching_nyc, int_odpt_stop_map
  marts/           the facts/dims above
seeds/             dim_city, weather_condition_map, mlit_tokyo_benchmark,
                   otp_band_definitions
snapshots/         routes_snapshot, stops_snapshot
tests/             generic: delay within [-30min, +6h]; completeness ≥ threshold;
                   headway sanity; uniqueness of (city,trip,stop,service_date)
```
Showcase features: incremental merge with lookback var, source freshness SLAs per feed, custom generic tests, macros (`to_local()`, `service_date()`), score weights as dbt vars, exposures pointing at the dashboard, dbt-utils + dbt-expectations, full docs site.

---

## 7. Metric definitions (the analytical contract)

- **delay_sec** = actual − scheduled arrival, signed. Buses at timepoints: departure more than 60s *early* = `early_departure_flag` (a failure — the rider missed it).
- **OTP bands** (defaults; dbt vars): early < −60s · on_time −60s..+299s · late +300s..+899s · very_late ≥ +900s. Cancelled/skipped tracked separately, never counted as on-time.
- **Excess Wait Time** (frequent services only, sched headway ≤ 10 min):
  `AWT = Σ(gap_i²) / (2·Σ gap_i)` computed on actual and on scheduled gaps; `EWT = AWT_actual − AWT_sched`. This is the fair metric where riders don't use timetables — schedule adherence is meaningless on a 3-minute-headway line.
- **Bunching** = gap < 0.5× scheduled; **big gap** = > 2× scheduled.
- **Completeness** = observed trips / scheduled trips; days below 70% are excluded from a city's score and reported as excluded (data honesty is part of the product).
- **Composite score v1** (weights are dbt vars, documented as tunable):
  `score = 100 · [ w1·S_wait + w2·S_otp + w3·S_cancel + w4·S_bunch ]`, defaults w = (0.35, 0.30, 0.20, 0.15), where
  `S_wait = clamp(1 − EWT_min/3, 0, 1)` (3 min excess wait → 0),
  `S_otp = otp_pct` on scheduled (infrequent) services,
  `S_cancel = clamp(1 − 10·cancel_rate, 0, 1)`,
  `S_bunch = clamp(1 − 2·bunching_rate, 0, 1)`.
  Blended per city by its mix of frequent vs scheduled service, mode-weighted by trip counts. Compare like-for-like (rail vs rail, bus vs bus) on the drill-down page; the composite is the headline.
- **Validation:** monthly correlation of our Tokyo per-line delay measurements vs MLIT official delay-certificate stats. If the shapes agree, the methodology holds.

---

## 8. Maps & dashboard (Streamlit + pydeck)

Four pages, one deployed app (Streamlit Community Cloud free tier, or the same EC2), read-only Snowflake key:

1. **Scorecard (headline).** Ranked bar of the 9 cities with score trend sparklines; component breakdown (wait / OTP / cancellations / bunching); completeness badge per city. Filter: month, mode.
2. **Delay hexmap.** Stop-level mean delay aggregated to **H3 hexagons** — Snowflake has native H3 functions (`H3_LATLNG_TO_CELL`), so the aggregation is a dbt model, and pydeck's `H3HexagonLayer` renders it. Nine small-multiple city maps on a shared color scale is the money screenshot: Zurich pale, and whoever's worst glowing red.
3. **Route explorer.** GTFS `shapes.txt` → GeoJSON paths colored by route reliability score (`PathLayer`); click-through to that route's delay-by-hour heatmap, headway distribution, and worst stops.
4. **Live-ish map.** Latest vehicle position per city (`ScatterplotLayer`, 1–5 min lag, labeled honestly) with an alerts sidebar — mostly a demo-wow page, powered by a small `current_state` table Dagster refreshes every few minutes.

Plus an **ops page**: feed freshness, completeness by source, Dagster asset status — reliability of the pipeline itself.

## 9. Dagster asset graph

```
(streaming side runs continuously; Dagster owns everything batch)

gtfs_static_raw[city]  ──►  gtfs_static_parsed[city]  ──►  (dbt) stg_gtfs__* ──► snapshots ──► dims
weather_backfill  ──►  weather_hourly[city]  ──►  (dbt) fct_weather_hourly
mlit_benchmark_seed  ──►  (dbt) fct_benchmark_mlit_monthly

s3_silver_sensor (new partition detected)
   └─►  (dbt) int_stop_events_finalized  ──►  fct_stop_events ──► fct_headways
              ──►  fct_route_reliability_daily  ──►  fct_city_scorecard_monthly
                        └─►  dashboard_current_state   └─►  monthly_report_md

Partitions: MultiPartition(city × service_date) on everything from silver down —
backfilling one city for one week is a one-click Dagster backfill.
Asset checks: freshness per feed, completeness ≥ threshold, delay-bounds sanity.
Schedules: hourly incremental dbt · daily marts · weekly GTFS refresh · monthly report.
Alerting: failed checks → Discord/Slack webhook.
```

## 10. Repo structure

```
transit-pulse/
├── infra/                        # Terraform
│   ├── modules/
│   │   ├── network/              # VPC, subnets, SGs
│   │   ├── lake/                 # S3 buckets (raw, lakehouse, artifacts), lifecycle rules
│   │   ├── catalog/              # Glue databases
│   │   ├── kafka/                # EC2-KRaft default; msk_serverless variant behind a flag
│   │   ├── spark/                # EMR Serverless app + IAM
│   │   ├── snowflake/            # snowflake provider: db, schemas, wh, roles,
│   │   │                         # storage integration, external Iceberg tables
│   │   ├── services/             # EC2/ECS for ingestors + Dagster + dashboard
│   │   └── monitoring/           # CloudWatch alarms, budgets alert
│   └── envs/{dev,prod}/          # thin roots; S3 backend + DynamoDB lock
├── ingestion/
│   ├── adapters/                 # base.py, gtfs_rt.py, odpt.py, hsl_mqtt.py
│   ├── config/cities/*.yaml      # nyc.yaml, chicago.yaml, ... tokyo.yaml
│   └── Dockerfile
├── spark_jobs/                   # bronze_writer.py, silver_normalize.py, hfp_downsample.py
├── dbt/transit/                  # project as in §6.4
├── orchestration/transit_dagster/
├── dashboard/app/                # Streamlit + pydeck
├── docker-compose.yml            # redpanda, minio, spark, dagster, duckdb-dbt — full local stack
├── .github/workflows/            # terraform.yml, dbt-ci.yml (slim CI), build.yml
├── ATTRIBUTION.md                # TTC / ODPT / 511 / Swiss license notices
└── README.md                     # architecture diagram, findings, demo GIF, cost notes
```

City yaml example (the "adding a GTFS-RT city is config" story):

```yaml
# ingestion/config/cities/chicago.yaml
city: chicago
timezone: America/Chicago
adapter: gtfs_rt
feeds:
  trip_updates:      https://transitdata.transitchicago.com/.../tripupdates
  vehicle_positions: https://transitdata.transitchicago.com/.../vehiclepositions
  alerts:            https://transitdata.transitchicago.com/.../alerts
poll_seconds: 30
static_gtfs: https://www.transitchicago.com/downloads/sch_data/google_transit.zip
```

## 11. Build roadmap

| Phase | Weeks | Deliverable |
|---|---|---|
| 0 — Scaffold | 1 | Repo, Docker Compose local stack (Redpanda/MinIO/Dagster/dbt-duckdb), Terraform backend bootstrap, CI skeleton |
| 1 — Vertical slice, one city | 1–2 | NYC subway end-to-end **locally**: poller → Kafka → Spark → silver → dbt → `fct_stop_events` → one OTP chart. Prove the finalization logic here — it's the hardest 20% |
| 2 — Cloud deploy | 1 | `terraform apply` stands up S3/Iceberg, Kafka EC2, EMR Serverless, Snowflake, services box; NYC flowing in prod |
| 3 — GTFS-RT fan-out | 1–2 | Chicago, DC, Boston, SF (RG feed), Toronto, Zurich via config + per-city quirk fixes (NYC trip matcher, Zurich filter) |
| 4 — Custom adapters | 1–2 | Tokyo (ODPT JSON + GTFS-RT, stop-ID mapping, MLIT seed) and Helsinki (MQTT bridge + Spark downsampler) |
| 5 — Metrics layer | 1–2 | Headways/EWT, activity, alerts, weather backfill + hourly, completeness; dbt tests/docs/snapshots; Dagster partitions, sensors, checks |
| 6 — Scorecard + dashboard | 1–2 | Composite score, 4-page Streamlit app with H3 maps, MLIT validation notebook, README + architecture diagram + demo GIF |
| 7 — Run & write | ongoing | Let history accrue; monthly auto-report; blog-style writeup "I measured 9 cities' transit for N months — here's who's actually reliable" |
| 8 — Optional flex | later | EKS migration of ingestors (the k8s box), JR East during a Challenge window, Sydney (TfNSW — which also publishes *historical* GTFS-RT archives, rare and worth grabbing), MTA buses |

Realistic calendar at nights-and-weekends pace: **8–10 weeks to a demoable product**, then it improves by existing (data accrues).

## 12. Cost

| Item | Full cloud | Frugal mode |
|---|---|---|
| Kafka EC2 t4g.small | ~$12 | local only |
| Services EC2 t4g.medium (ingestors+Dagster+dashboard) | ~$25 | ~$12 (t4g.small) |
| EMR Serverless | $10–30 | $5 (batch micro-runs) |
| S3 + Glue | <$5 | <$3 |
| Snowflake (XS, auto-suspend 60s) | ~$25 | trial credits → ~$15 |
| **Total/month** | **~$60–95** | **~$25–35** |

Terraform makes teardown/rebuild a non-event: destroy between demo periods, keep S3 (the history is the asset). Set an AWS Budgets alarm at $50 in the monitoring module on day one.

## 13. Known risks & gotchas (planned for, not discovered later)

1. **Prediction finalization** is the crux: last-prediction-before-passage vs status-transition vs ODPT-stated delay, unified behind `finalization_method`. Budget real time in Phase 1; everything downstream depends on it.
2. **NYC trip matching** — RT trip_ids ≠ static trip_ids; matcher on (route, direction, origin dep time). Isolated in `int_trip_matching_nyc`.
3. **511 rate limit** (60/hr default) — RG feed + 90–120s cadence, or request an increase.
4. **Zurich national feed** — agency allow-list + bbox filter; national static GTFS is large → Spark parses static, never pandas.
5. **HSL MQTT volume** — downsample early in Spark; never let 1s pings hit the warehouse.
6. **Tokyo ID mapping** (ODPT URNs ↔ GTFS stop_ids) — dedicated mapping model + tests; JR East explicitly out of core scope (Challenge-gated).
7. **TTC subway** has no RT predictions (surface only + alerts) — scoped honestly in `dim_city.modes_covered` and the completeness matrix.
8. **Feed outages happen** — completeness metric + Dagster freshness checks turn outages into recorded data-quality facts instead of silent score corruption.
9. **Midnight/service-day, DST, `25:30:00` times** — handled once in shared macros, tested.
10. **Ghost vehicles / duplicate vehicle_ids / stale predictions** — dedup rules in silver + delay-bounds tests in dbt.

## 14. Definition of done

A recruiter clicks the README: architecture diagram, a GIF of the 9-city hexmap, the scorecard table with a real answer to "who's most reliable," a validation note against Tokyo's official stats, and a cost line showing the whole thing runs for under $100/mo. An interviewer gets three deep-dive stories: mode-aware scoring (EWT vs OTP), the prediction-finalization pipeline, and "adding city #10 is a yaml file — here's the PR."
