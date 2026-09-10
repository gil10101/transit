# Transit Pulse — Warehouse Schema (gold layer, Snowflake)

Star schema. All timestamps stored as `TIMESTAMP_NTZ` with explicit `_utc` / `_local` suffixes. Surrogate keys are dbt hash keys (`dbt_utils.generate_surrogate_key`). Changes from the first draft are marked **[rev]**.

```
                    dim_date   dim_time_local   dim_weather
                         \          |            /
 dim_city ── dim_agency ── fct_stop_events ── dim_route (SCD2)
     |            \            |        \         |
     |             \      fct_headways   \    dim_stop (SCD2)
     |              \          |          \
     |          fct_service_delivery_daily fct_vehicle_activity_hourly
     |                         |
     |            fct_route_reliability_daily ── fct_alerts_daily
     |                         |                fct_weather_hourly
     └──────────── fct_city_scorecard ── fct_benchmark_mlit_monthly
```

---

## Dimensions

### dim_city (seed-driven)
| Column | Type | Notes |
|---|---|---|
| city_key | varchar PK | 'nyc','chicago','dc','boston','sf','toronto','zurich','helsinki','tokyo' — **[rev P3b]** SF Bay's key is `sf` (was sketched 'sfbay'; ingestion yamls, silver, and the seed all use 'sf') |
| city_name, country | varchar | |
| iana_tz | varchar | e.g. 'Asia/Tokyo' — drives every `_local` derivation |
| population | int | context/per-capita |
| peak_am_start, peak_am_end, peak_pm_start, peak_pm_end | time | per-city override (Tokyo skews) |
| modes_covered | array | **[rev]** honest coverage: toronto = ['bus','tram'] (subway alerts-only); tokyo = ['metro','tram','bus'] |
| rt_delay_source | varchar | **[rev P3b]** 'computed' \| 'feed' \| 'mixed' \| 'odpt_stated'(P4) — documents where delay comes from per city ('feed' is the implemented spelling of the earlier 'feed_delay' sketch; 'mixed' = per-operator, sf: SF/SM/BA/GG feed, rest computed — fixtures 2026-08-23) |
| crowding_usable | boolean | **[rev P3b]** occupancy_status from the city's VPs is a real crowding signal (P6 crowding metric input): boston/toronto/sf true; nyc/helsinki/dc/zurich false (dc present-but-noninformative, zurich has no VP product) |
| attribution_text | varchar | TTC/ODPT/511/Swiss license notices |

### dim_agency
| Column | Type |
|---|---|
| agency_key | varchar PK (hash city+agency_id) |
| city_key FK, agency_id, agency_name, agency_tz | |

### dim_route — SCD2 derived from versioned static GTFS **[rev P6: NOT a dbt snapshot]**
> [rev P6, 2026-08-26] Sketched as "via dbt snapshot"; built as a pure derivation from
> silver instead. Silver keeps every `gtfs_version_id` as its own partition, so snapshot
> state is strictly worse: a snapshot table IS the history (lose it, it's gone; this dim
> rebuilds from silver), its valid_from is whenever the snapshot job ran (ours is the
> version's real download date, parsed from the id), and history would stop accruing
> whenever dbt doesn't run. Intervals: new on first appearance or attribute change;
> CLOSED when a route leaves the static (re-appearance opens a fresh one); re-downloads
> of unchanged zips (new version_id, same content) collapse by attribute comparison.
> valid_to exclusive, NULL = current. `int_gtfs_versions` is the registry;
> `assert_scd2_intervals_disjoint` guards join uniqueness.
| Column | Type | Notes |
|---|---|---|
| route_key | varchar PK (hash city+route_id+valid_from) |
| city_key FK, agency_key FK, route_id | | natural id |
| route_short_name, route_long_name, route_color | | |
| mode | varchar | derived from route_type: metro/tram/bus/rail/ferry |
| is_frequent_service_typical | boolean | **[rev]** indicative flag only (weekday-peak sched headway ≤ 10 min). The *scoring* classification lives at finer grain in `int_service_frequency` (route × direction × daypart × service_date) because a route can be frequent at peak and scheduled at night |
| valid_from, valid_to, is_current, gtfs_version_id | | SCD2 |

### dim_stop — SCD2
| Column | Type | Notes |
|---|---|---|
| stop_key | varchar PK |
| city_key FK, stop_id, stop_name, parent_station | | |
| lat, lon, geo (GEOGRAPHY) | | |
| h3_r8, h3_r9 | varchar | Snowflake `H3_LATLNG_TO_CELL` at build time → hexmap is a plain GROUP BY |
| is_timepoint_default | boolean | **[rev P6: DEFERRED]** exists to drive the early-departure rule, which is itself unimplemented (fct_stop_events header); computing bus timepoints per version means walking every stop_times version. Add when the rule lands, not before |
| valid_from, valid_to, is_current, gtfs_version_id | | same derived-SCD2 mechanism as dim_route |

> [rev P6] Zurich (national feed): a stop is present in a version only if an
> allow-listed trip serves it there, or it parents one that does (`int_stops_served`,
> incremental per version so the 66.7M-row stop_times is walked once per new static,
> not per chain run). Other cities keep full stops.txt including station records.

### dim_date, dim_time_local
Standard calendar; `dim_time_local`: local_hour PK, daypart, is_peak. **[rev P5]** daypart buckets locked as implemented in `macros/daypart.sql` (baked into int_service_frequency, fct_headways, EWT slices): am_peak 5–9 / midday 10–15 / pm_peak 16–19 / evening 20–23 / overnight 0–4 — replaces the earlier six-bucket sketch; dim_time_local (P6) must match.

### dim_weather (seed)
weather_key PK, wmo_code_range, condition_bucket ∈ {clear, cloudy, rain, heavy_rain, snow, heavy_snow, fog, extreme}.

---

## Facts

### fct_stop_events — atomic fact
**Grain:** one finalized stop visit per `(city_key, service_date, trip_uid, stop_sequence)` ← **[rev]** unique key uses `trip_uid` + `stop_sequence` (stop_id alone breaks on loop routes that visit a stop twice; trip_id alone breaks on HSL empty ids and NYC re-matching).
**Volume:** ~2–4M rows/day. Incremental merge, 48h lookback. Cluster by `(service_date, city_key)`.

| Column | Type | Notes |
|---|---|---|
| city_key, route_key, stop_key | FK | **[rev P6]** point-in-time against the SCD2 interval covering service_date (`macros/scd2_join.sql`); a date before a route/stop's first interval falls back to that first interval (several cities' first static load postdates their first observed days); NULL where the static never described the id — Toronto's disjoint stop namespace is the standing case. Natural ids stay on the fact. agency_key dropped with dim_agency (agency_id rides on dim_route) |
| trip_uid | varchar | degenerate; see data dictionary §F |
| trip_id_raw, vehicle_id | varchar | as-received (nullable) |
| direction_id, stop_sequence | int | |
| service_date, local_date, local_hour, local_dow, is_peak, daypart | | derived via dim_city.iana_tz + GTFS noon−12h service-day rule |
| sched_arr_ts_utc, sched_dep_ts_utc | ts | from versioned static |
| actual_arr_ts_utc, actual_dep_ts_utc | ts | finalized |
| delay_arr_sec | int signed | negative = early |
| delay_dep_sec | int signed | |
| early_departure_flag | bool | dep >60s early AND stop is timepoint AND mode=bus **[rev]** (rule scoped to where it's a real failure) |
| otp_band | varchar | early / on_time / late / very_late (vars) |
| schedule_relationship | varchar | SCHEDULED/ADDED/CANCELED/SKIPPED — **ADDED excluded from OTP, included in volume [rev]** |
| cancelled_flag, skipped_flag | bool | |
| finalization_method | varchar | status_transition \| last_prediction \| vp_passage \| odpt_stated |
| prediction_count, first_seen_utc, last_seen_utc | | audit |
| data_quality_score | float | uncertainty + method + staleness composite |
| gtfs_version_id, source_format | | lineage |

### fct_headways
**Grain:** consecutive observed arrivals per `(city_key, route_key, direction_id, stop_key, service_date)`, ordered by actual_arr.
| Column | Notes |
|---|---|
| …grain keys…, local_hour, daypart | |
| prev_trip_uid, trip_uid, prev_vehicle_id, vehicle_id | |
| actual_gap_sec | arr − prev arr |
| sched_headway_sec | **[rev]** from `frequencies.txt` when present, else derived from consecutive *scheduled* arrivals at the same (route, direction, stop) — most agencies don't publish frequencies |
| gap_ratio, bunched_flag (<0.5), big_gap_flag (>2.0) | |

### int_service_frequency (intermediate, feeds scoring) **[rev — new]**
Grain: (city_key, route_key, direction_id, daypart, service_date) → median_sched_headway_sec, is_frequent (≤600s). Determines EWT-vs-OTP treatment per route-slice, so a line scored by EWT at peak is scored by OTP overnight.

### fct_vehicle_activity_hourly
Grain: (city_key, mode, service_date, local_hour). distinct_vehicles, distinct_trips_active, distinct_routes_active, **vehicle_id_reliable** bool **[rev]** (Helsinki TU lacks stable vehicle ids on some modes → trips_active is the primary volume measure there; column flags comparability).

### fct_service_delivery_daily
(city_key, route_key, service_date): trips_scheduled (from static calendar), trips_observed, trips_added, trips_cancelled, completeness_pct = observed_scheduled / scheduled.

### fct_weather_hourly
(city_key, local_date, local_hour) PK → joins fct_stop_events on exactly those three columns **[rev]** (explicit join contract). temp_c, precip_mm, snowfall_cm, wind_kph, weather_key FK. **[rev P5]** sourced from silver.weather_hourly written direct-to-Snowflake (1 row/city/hr; lake unnecessary).

### fct_alerts_daily
(city_key, route_key, service_date): alerts_active, alert_minutes (overlap-deduped per alert_id **[rev]**), worst_effect.

### fct_route_reliability_daily
(city_key, route_key, service_date, direction_id): mode, otp_pct, early_pct, late_pct, very_late_pct, early_departure_pct, mean/median/p90_delay_sec, ewt_sec (frequent slices), bunching_pct, big_gap_pct, cancel_pct, completeness_pct, scheduled_trips, observed_trips.

### fct_city_scorecard  *(window grain since 2026-09-10; was fct_city_scorecard_monthly — same columns plus window_start/window_end, month dropped, 20-day floor unchanged)*
(city_key, month): score_0_100, s_wait, s_otp, s_cancel, s_bunch, frequent_service_share, completeness_pct, excluded_route_days, judged_days, route_days, trip totals, weights, methodology_version **[rev]** (scores are versioned — weight changes create v2, never silently rewrite history).

> [rev P6, 2026-08-26] As built, three rules the sketch didn't spell out:
> **(1) refuses thin history** — no row under `scorecard_min_judged_days` (20) closed
> judged days in the month; an empty table on 4 days of data is the correct output.
> **(2) grain discipline** — delivery quantities (trips/cancel/completeness) come from
> route-grain fct_service_delivery_daily; reliability ratios come from direction-grain
> fct_route_reliability_daily weighted by their own evidence counts (banded_events,
> rated_gaps, ewt_gap_count). Summing trip counts across direction rows double-counts
> two-direction routes; `assert_scorecard_totals_match_route_grain` pins this.
> **(3) missing evidence is not a score** — an absent input leaves its sub-score NULL
> and the composite renormalises over the weights present (missing OTP is not 0;
> missing EWT is not perfection).
> Dropped from the sketch: `rank` (meaningless until ≥2 cities clear the guard — a
> dashboard concern, not a fact), `mode_mix` variant (deferred; mode splits live in
> fct_route_reliability_daily.mode), `excluded_days[]` array → `excluded_route_days`
> count + the completeness floor var (`scorecard_min_completeness` 0.50).

### fct_benchmark_mlit_monthly
(railway_urn, month): delay_certificate_days, our_measured_p50_delay, correlation context. Tokyo-only validation table.

---

## Silver layer (Iceberg, Spark-managed — dbt sources)
| Table | Partition | Grain |
|---|---|---|
| silver.stop_time_predictions | city / service_date | every prediction snapshot (post exact-dup removal) |
| silver.vehicle_positions | city / service_date | pings (HSL n/a in core; other cities 15–60s cadence) |
| silver.alerts | city / service_date | alert versions |
| silver.odpt_trains | service_date | Tokyo odpt:Train observations **[rev — new]** |
| silver.gtfs_static_{routes,trips,stops,stop_times,calendar,calendar_dates,shapes} | city / gtfs_version_id | versioned schedule |
| silver.weather_hourly | city | Open-Meteo pulls **[rev P5]** direct-to-Snowflake native table, not Iceberg (1 row/city/hr; lake unnecessary; Dagster MERGE on city+local_date+local_hour) |

## Capacity check (double-checked against live sizes ✅)
Observed per-poll payloads: MTA 8 feeds ≈ 1MB total, MBTA 0.8MB, TTC 0.7MB, HSL 0.75MB; CTA/WMATA similar; 511 RG est. 1–3MB; Zurich national est. 5–20MB (filtered to Zurich in silver); Tokyo JSON <0.5MB. Total ingest ≈ **5–30 MB/min (<0.5 MB/s)** → single-broker Kafka is loafing; EMR Serverless micro-batches trivial; Snowflake XS handles 2–4M finalized rows/day with seconds-long incremental merges. Storage ≈ 1–3 GB/day Parquet ≈ <$3/mo/yr-of-history at S3 prices. The stack accommodates with an order of magnitude of headroom.
