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
     └──────────── fct_city_scorecard_monthly ── fct_benchmark_mlit_monthly
```

---

## Dimensions

### dim_city (seed-driven)
| Column | Type | Notes |
|---|---|---|
| city_key | varchar PK | 'nyc','chicago','dc','boston','sfbay','toronto','zurich','helsinki','tokyo' |
| city_name, country | varchar | |
| iana_tz | varchar | e.g. 'Asia/Tokyo' — drives every `_local` derivation |
| population | int | context/per-capita |
| peak_am_start, peak_am_end, peak_pm_start, peak_pm_end | time | per-city override (Tokyo skews) |
| modes_covered | array | **[rev]** honest coverage: toronto = ['bus','tram'] (subway alerts-only); tokyo = ['metro','tram','bus'] |
| rt_delay_source | varchar | **[rev]** 'computed' \| 'feed_delay' \| 'odpt_stated' — documents where delay comes from per city |
| attribution_text | varchar | TTC/ODPT/511/Swiss license notices |

### dim_agency
| Column | Type |
|---|---|
| agency_key | varchar PK (hash city+agency_id) |
| city_key FK, agency_id, agency_name, agency_tz | |

### dim_route — SCD2 via dbt snapshot on versioned static GTFS
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
| is_timepoint_default | boolean | **[rev]** rail→true, bus→from stop_times.timepoint; drives early-departure rule |
| valid_from, valid_to, is_current, gtfs_version_id | | |

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
| city_key, agency_key, route_key, stop_key | FK | route/stop resolved against the SCD2 version valid on service_date **[rev]** |
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

### fct_city_scorecard_monthly
(city_key, month): score_0_100, rank, s_wait, s_otp, s_cancel, s_bunch, frequent_service_share, mode_mix (variant object), completeness_pct, excluded_days[], methodology_version **[rev]** (scores are versioned — weight changes create v2, never silently rewrite history).

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
