# Transit Pulse — dbt Specification

Project: `dbt/transit`, target Snowflake (prod) / DuckDB (local dev profile). Packages: `dbt_utils`, `dbt_expectations`, `dbt_date`.

## 1. Sources (`models/staging/_sources.yml`)
Silver Iceberg tables exposed to Snowflake as external tables; freshness measured on `fetched_at`.

```yaml
sources:
  - name: silver
    schema: silver
    tables:
      - name: stop_time_predictions
        loaded_at_field: fetched_at
        freshness: {warn_after: {count: 30, period: minute},
                    error_after: {count: 2, period: hour}}
      - name: vehicle_positions   # same freshness
      - name: alerts
      - name: odpt_trains         # Tokyo rail real-time
      - name: gtfs_static_routes  # + trips, stops, stop_times, calendar,
      - name: weather_hourly      #   calendar_dates, shapes (no freshness SLA)
```

## 2. Model list & materializations

| Model | Mat. | Key config |
|---|---|---|
| `stg_gtfsrt__trip_updates` | view | typing, canonical delay rule `coalesce(arrival_delay, arr_time − sched)` deferred to int (needs schedule) |
| `stg_gtfsrt__vehicle_positions` | view | |
| `stg_gtfsrt__alerts` | view | |
| `stg_odpt__trains` | view | URN parsing (`odpt.Railway:TokyoMetro.Ginza` → operator, line) |
| `stg_gtfs__{routes,trips,stops,stop_times,calendar,calendar_dates}` | view | GTFS `HH:MM:SS >24h` → seconds-after-noon-minus-12h int |
| `stg_weather__hourly` | view | WMO code → weather_key |
| `int_gtfs_scheduled_stop_times` | incremental | exploded schedule per service_date (calendar × stop_times); cluster (service_date, city) |
| `int_trip_matching_nyc` | incremental | RT trip_id → static trip via (route, direction, service_date, origin-time prefix/100 min); emits match_confidence |
| `int_trip_matching_generic` **[rev P3]** | table | P3 cities. boston/toronto: RT trip_id **is** the static trip_id → exact equijoin; helsinki: trip_id empty (dict §B) → (route_id, direction_id, origin-time seconds) among trips active on the service_date via `int_service_dates`. Confidence 1.0 exact only, no fuzzy tiers; unmatched RT trips (e.g. TTC ADDED runs) absent → events flow with null schedule, OTP null |
| `int_trip_matching` **[rev P3]** | table | union of nyc + generic matchers with identical columns — the **only** matcher downstream models reference (`int_stop_events_finalized` joins it on city_key/service_date/trip_uid). Grain 1 row per (city_key, service_date, trip_uid): best-confidence kept via qualify, guarded by `tests/assert_trip_matching_unique.sql` |
| `int_odpt_stop_map` | table | ODPT URN ↔ GTFS stop_id/route_id; `dbt_utils.relationships` tested both ways |
| `int_stop_events_finalized` | **incremental (merge)** | THE model — see §3. unique_key `(city_key, service_date, trip_uid, stop_sequence)`, lookback var 48h |
| `int_service_frequency` | ~~incremental~~ **table [rev P5]** (small; rebuilt with static) | median sched headway per (route, direction, daypart, service_date); `is_frequent = headway ≤ var('freq_headway_threshold_sec')` |
| ~~`int_headways`~~ **`fct_headways` [rev P5]** (marts, per docs/02 §fct_headways) | incremental (delete+insert, 48h lookback) | LAG(actual_arr) per (city, route, dir, stop, service_date); sched headway from frequencies.txt else LAG(sched_arr) |
| `fct_*`, `dim_*` | incremental / table per schema doc | facts: merge + cluster (service_date, city_key); dims from snapshots |
| `fct_city_scorecard_monthly` | table | full-refresh each run; methodology_version stamped |

## 3. `int_stop_events_finalized` — the core logic (sketch)

```sql
with preds as (
  select *, row_number() over (
    partition by city_key, service_date, trip_uid, stop_sequence
    order by
      case when source_format = 'odpt_json' then 0        -- stated delay wins
           when finalization_signal = 'status_transition' then 1
           when finalization_signal = 'vp_passage' then 2
           else 3 end,                                     -- last prediction
      fetched_at desc
  ) as rn
  from {{ ref('stg_all_predictions_unioned') }}
  where service_date >= dateadd(hour, -{{ var('lookback_hours', 48) }}, current_date)
    and fetched_at <= coalesce(passage_ts, arr_pred_ts + interval '30 min')
), final as (select * from preds where rn = 1)
select f.*,
  coalesce(f.stated_delay_sec,
           datediff('second', s.sched_arr_ts_utc, f.actual_arr_ts_utc)) as delay_arr_sec,
  ...otp_band via {{ otp_band('delay_arr_sec') }}...
from final f
left join {{ ref('int_gtfs_scheduled_stop_times') }} s using (city_key, service_date, trip_uid, stop_sequence)
```
Trips scheduled but never observed within lookback → emitted as `cancelled_flag` per `schedule_relationship` or `missing` (counts against completeness, not OTP).

## 4. Snapshots
`routes_snapshot`, `stops_snapshot`: strategy=check on all columns, invalidated by new `gtfs_version_id` → SCD2 backing dim_route/dim_stop.

## 5. Seeds
`dim_city`, `weather_condition_map` (WMO→bucket), `mlit_tokyo_benchmark`, `otp_band_definitions`, `zurich_route_allowlist` (generated helper, checked in).

## 6. Macros
`to_local(ts, city_key)` (join dim_city, convert_timezone), `service_date(local_ts)` (noon−12h rule), `otp_band(delay_col)`, `h3_cell(lat, lon, res)`, `clamp(x, lo, hi)`.

## 7. Tests
Generic on every fact: `unique` + `not_null` on grain keys; `relationships` to dims.
Custom generic tests:
- `delay_within_bounds` (−30 min … +6 h; failures land in an audit table, not dropped silently)
- `completeness_above` (warn <85%, error <50% per city/day)
- `headway_sane` (actual_gap between 30s and 4h)
- `local_time_consistency` (local_hour matches to_local(ts))
- `frequent_service_has_ewt` / `scheduled_service_has_otp` (scoring path routed correctly)
dbt_expectations: row-count deltas day-over-day within ±40% per city (feed-outage tripwire, pairs with Dagster freshness checks).
**[rev P5]** implemented via the custom `value_within_range` generic (dbt_utils/dbt_expectations not installed): delay bounds, headway sanity, completeness warn<0.85/error<0.50, gap_ratio/EWT ranges. Still deferred: `local_time_consistency`, `frequent_service_has_ewt`/`scheduled_service_has_otp` (land with the P6 scoring path they guard), row-count delta expectations (Dagster freshness checks cover the outage tripwire today).
**[rev P3]** stg_gtfs__routes maps Google extended route types (HSL: 701/702/704 bus, 109 suburban rail, 900 tram — 92% of its routes) alongside base 0-4. Per-city singular tests: `assert_toronto_added_normalized` (no toronto row may still read `NEW`, and no negative-id row `SCHEDULED` — the staging NEW→ADDED normalization, dict §B TTC) and `assert_trip_matching_unique` (matcher grain guard). `dim_city.rt_delay_source` accepted_values [computed, feed]; all four live cities are `computed` (fixtures 2026-08-23: 0 arrivals carry `arrival.delay` in boston/toronto/helsinki). All models tolerate cities with zero local rows — duckdb dev runs green with only NYC data.

## 8. Vars (`dbt_project.yml`)
```yaml
vars:
  lookback_hours: 48
  freq_headway_threshold_sec: 600
  otp_bands: {early_max: -60, on_time_max: 299, late_max: 899}
  early_departure_grace_sec: 60
  score_weights: {wait: 0.35, otp: 0.30, cancel: 0.20, bunch: 0.15}
  completeness_exclusion_threshold: 0.70
  methodology_version: "1.0"
```

## 9. Exposures
`transit_pulse_dashboard` (Streamlit URL) depending on scorecard/reliability/hexmap models; `monthly_report`.

## 10. CI & conventions
- **Slim CI** (GitHub Actions): `dbt build --select state:modified+ --defer --state prod-artifacts` on PR; full `dbt build` nightly via Dagster.
- Naming: `stg_<source>__<entity>`, `int_<concept>`, `fct_/dim_`. One model per file; every mart column documented in yml (docs site published via GitHub Pages).
- Dagster owns execution via `dagster-dbt`; every model is an asset with city×date partitions mapped through.
```
