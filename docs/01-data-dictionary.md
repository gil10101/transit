# Transit Pulse — Data Dictionary

Every field we ingest, by source. Verified live 2026-08-22 (all open feeds fetched and protobuf-decoded; keyed feeds existence-checked). ✅ = confirmed by live fetch.

---

## A. GTFS-RT (common structure) — NYC, Chicago, DC, Boston, SF, Toronto, Zurich, Helsinki (+ Tokyo alerts/bus)

Binary protobuf `FeedMessage`. One file = `header` + repeated `entity`, each entity holding exactly one of `trip_update` / `vehicle` / `alert`.

### A.1 FeedHeader
| Field | Type | Meaning | Our use |
|---|---|---|---|
| `gtfs_realtime_version` | string | Spec version ("2.0"; NYC reports "1.0") | sanity check |
| `timestamp` | uint64 epoch | When feed snapshot was generated | `feed_ts_utc`; freshness metric (observed lag 1–18s ✅) |

### A.2 TripUpdate (→ `silver.stop_time_predictions`)
| Field | Type | Meaning | Our use |
|---|---|---|---|
| `trip.trip_id` | string | Trip identifier — **matches static GTFS only sometimes** | join key after per-city matcher; see quirks |
| `trip.route_id` | string | Route | `route_id` |
| `trip.direction_id` | uint | 0/1 | `direction_id` |
| `trip.start_date` | string YYYYMMDD | Service date of the trip | `service_date` anchor |
| `trip.start_time` | string HH:MM:SS | Origin departure (may be >24:00) | trip resolution (HSL, NYC matcher) |
| `trip.schedule_relationship` | enum | SCHEDULED / ADDED / CANCELED / UNSCHEDULED | `cancelled_flag`; ADDED excluded from OTP, counted in volume |
| `stop_time_update[].stop_id` | string | Stop | `stop_id` |
| `stop_time_update[].stop_sequence` | uint | Order within trip | dedup key component |
| `stop_time_update[].arrival.time` | int64 epoch | Predicted arrival | `arr_pred_ts_utc` |
| `stop_time_update[].arrival.delay` | int32 sec | Predicted delay vs schedule (signed) | preferred when present; else computed vs static |
| `stop_time_update[].arrival.uncertainty` | int32 | Prediction confidence | `data_quality_score` input |
| `stop_time_update[].departure.{time,delay}` | — | Same for departure | `dep_pred_ts_utc`, early-departure detection |
| `stop_time_update[].schedule_relationship` | enum | SCHEDULED / SKIPPED / NO_DATA | `skipped_flag` |
| `vehicle.id`, `vehicle.label` | string | Vehicle serving trip | `vehicle_id` (activity counts) |
| `timestamp` | uint64 | Per-trip measurement time | tie-breaker in finalization |

**Canonical delay rule:** `delay_pred_sec = COALESCE(arrival.delay, arrival.time − scheduled_arrival_epoch)`.

### A.3 VehiclePosition (→ `silver.vehicle_positions`)
| Field | Type | Our use |
|---|---|---|
| `trip.*` | as above | link ping→trip |
| `vehicle.id` / `label` / `license_plate` | string | `vehicle_id` |
| `position.latitude`, `position.longitude` | float | map layers, H3 |
| `position.bearing`, `position.speed` | float | map arrows; speed sanity tests |
| `current_stop_sequence`, `stop_id` | — | passage detection (finalization method `vp_passage`) |
| `current_status` | enum INCOMING_AT / STOPPED_AT / IN_TRANSIT_TO | finalization method `status_transition` |
| `timestamp` | uint64 | `ts_utc` |
| `occupancy_status` | enum | optional crowding metric (MBTA ✅ supports) |
| `congestion_level` | enum | rarely populated; ignore |

### A.4 Alert (→ `silver.alerts`)
| Field | Our use |
|---|---|
| `id` (entity id), `active_period[].{start,end}` | `alert_id`, duration → `alert_minutes` |
| `informed_entity[].{agency_id,route_id,route_type,stop_id,trip}` | scope alert to routes/stops |
| `cause`, `effect` (e.g. SIGNIFICANT_DELAYS, DETOUR, NO_SERVICE) | `fct_alerts_daily.worst_effect` |
| `severity_level` | severity |
| `header_text`, `description_text` (translated strings) | display only |

---

## B. Per-city specifics (verified)

| City | Endpoints | Auth | Verified | Quirks that affect the pipeline |
|---|---|---|---|---|
| **NYC (MTA subway)** | `api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs[-ace,-bdfm,-g,-jz,-nqrw,-l,-si]` (8 feeds; base feed covers 1–7+GS) | none | ✅ 200, feed age 1–11s observed; at fixture capture 2026-08-22: ACE TU=79/VP=79, base TU=199/VP=117 (counts drift poll to poll) | Fixture-verified 2026-08-22 (single-snapshot evidence; re-verify patterns as data accrues): ① `arrival.delay` not set in 7 of 8 feeds → we compute vs schedule. Exception: the L feed (CBTC) sets `stop_sequence` on all STUs and `arrival.delay` (+`uncertainty`, always 0) on every STU that has an arrival; departure-only STUs carry `departure.delay`. The COALESCE delay rule absorbs this. ② trip_id is origin-time-encoded (✅ `070950_A..S58R`) ≠ static trip_id → NYC matcher on (route, direction, start_date, origin time = prefix/100 min after midnight). `trip.start_time` is empty in base+L, populated in the other six, and where populated can differ from the prefix by up to ~2 min → the prefix is used unconditionally. RT trip_ids also carry path variants absent from static (`..S07X003` vs `..S07R`; L drops the track: `084600_L..N`) and origin times can drift ±minutes intraday → token-exact static match covers only ~60%; (route, direction, origin-time) matching is primary, token equality just raises confidence. ③ VP has NO lat/lon for subway (stop-relative only); `current_status`/`current_stop_sequence` missing on ~half of VPs (unassigned trains) and VP timestamps range from +58 min future-dated to hours stale → v1 finalization uses last_prediction, not vp_passage/status_transition. ④ NYCT protobuf extension present (`nyct_trip_descriptor`: train_id, is_assigned, direction; `nyct_stop_time_update`: scheduled_track, actual_track) — parse train_id, ignore rest. `vehicle.id`/`label` are NEVER set; train identity exists only in ext `train_id`. ⑤ `stop_sequence` absent outside the L feed → derived from matched static `stop_times` by stop_id. ⑥ `trip.direction_id` absent in 7 feeds and always 0 in L (even northbound) → direction comes from the trip_id `..N/..S` suffix (or ext `direction`), never from `direction_id`. ⑦ Alerts: observed only in the base feed and contentless (no informed_entity/active_period/cause/effect); NYC entity ids are per-snapshot ordinals → entity-id-as-alert_id is unstable for NYC. ⑧ ~1% of STUs are departure-only (origin stops) → arrival logic falls back to departure. ADDED/CANCELED/SKIPPED not yet observed in NYC fixtures. |
| **Chicago (CTA)** | `transitdata.transitchicago.com/GtfsRealtime/{TripUpdates,VehiclePositions,ServiceAlerts}.pb` (+`.json`) | **free key required**, `?key=` (✅ keyless request returns HTML page, not a feed) | endpoint live | Beta program; register via CTA Developer Center. Train Tracker/Bus Tracker JSON APIs remain as backup. Rail + bus in one program. |
| **DC (WMATA)** | `api.wmata.com/gtfs/{rail,bus}-gtfsrt-{tripupdates,vehiclepositions,alerts}.pb` | free key, header `api_key`; default 50k calls/day | ✅ 200 with key 2026-08-23 (rail TU ~94KB, protobuf decodes) | 6 endpoints × 30s = 17k/day, inside quota. tz America/New_York. |
| **Boston (MBTA)** | `cdn.mbta.com/realtime/{TripUpdates,VehiclePositions,Alerts}.pb` | none for GTFS-RT (V3 JSON:API key optional) | ✅ 200, TU=1671, VP=522, age 1–2s | Occupancy populated on many vehicles (fixture 2026-08-23: 276/394 VPs, values MANY/FEW_SEATS + FULL). Cleanest feed of the set: trip_id (static-joinable), direction_id and stop_sequence set on every TU; all VPs carry lat/lon. Fixture 2026-08-23: `arrival.delay` NOT set (0/16.8k arrivals; `uncertainty` on some) → delay computed vs static (`rt_delay_source=computed`); ADDED (55) + CANCELED (4) trips and SKIPPED STUs observed; alerts carry effect/severity/active_period/informed_entity throughout. |
| **SF Bay (511.org)** | `api.511.org/transit/{tripupdates,vehiclepositions,servicealerts}?agency=RG&api_key=…` | free token; **60 req/hr default** | ✅ 200 with token 2026-08-23 (~1.5MB TU) | `agency=RG` = all ~30 operators in one response. **[rev P3 batch 2 2026-08-23]** The 60 req/hr limit is per key TOTAL across endpoints, so until the increase is granted all 3 endpoints poll at `poll_seconds: 200` → 3 × 18 = 54 req/hr, 10% headroom (supersedes the earlier 90–120s note, which fit only 2 endpoints inside the limit). When 511 grants the increase, drop toward 60–90s in the yaml. Multi-agency: every record carries agency prefix → `agency_id`. tz America/Los_Angeles. |
| **Toronto (TTC)** | `bustime.ttc.ca/gtfsrt/{trips,vehicles,alerts}` | none; **attribution required** (Open Government Licence – Toronto) | ✅ 200, TU=1876, VP=1458, age 2–18s | Surface only (bus+streetcar); subway = alerts only → recorded in completeness matrix. ✅ 59/1883 trips have negative trip_ids = ADDED/unscheduled runs: count in service volume, exclude from OTP. Fixture 2026-08-23 (44/1522 = 2.9%): the feed marks every negative-id trip `schedule_relationship=NEW` (the spec's successor to deprecated ADDED, not unset) → staging treats NEW as ADDED. Trip descriptor is trip_id+route_id only (`direction_id`/`start_date`/`start_time` never set → direction via static trips join). `arrival.delay` NOT set → computed vs static. `occupancy_status` on 1163/1177 VPs. Alerts: informed_entity always set; no active_period/severity, effect=UNKNOWN_EFFECT (fixture 2026-08-23). |
| **Zurich (opentransportdata.swiss)** | `api.opentransportdata.swiss/la/gtfs-rt` (TU; `gtfsrt2020` is dead, ✅ 404) + `…/la/gtfs-sa` (service alerts) | free token, RAW `Authorization: <token>` header — **NO "Bearer " prefix** — and the client MUST follow redirects (curl -L): both endpoints 302 cross-host to a pre-signed `largeapi.opentransportdata.swiss` URL. Verified 2026-08-23: requests strips the Authorization header on that cross-host hop and the signed target needs no auth, so default redirect-following works. **One token per API product**: `/la/gtfs-rt` uses `SWISS_OTD_TOKEN`, `/la/gtfs-sa` its own `SWISS_OTD_SA_TOKEN`. Tokens are NOT re-issuable if revoked — .env + SSM only. | ✅ 200 with token 2026-08-23 (~6MB, gtfs-rt v2.0) | National feed (all CH operators). Filter to Zurich in silver via route allow-list built from static GTFS (agencies: VBZ + ZVV-area + SBB S-Bahn lines serving canton ZH stops). National static GTFS is large → Spark parses it as-is (canonical projection drops extra columns; silver stays partition-pruned by city+version). tz Europe/Zurich. |
| **Helsinki (HSL)** | `realtime.hsl.fi/realtime/trip-updates/v2/hsl`, `…/service-alerts/v2/hsl` | **none** | ✅ 200, TU=942, Alerts=21, age 13–15s | **KEPT — now a zero-key generic GTFS-RT city.** ✅ `trip_id` is EMPTY: trips resolved by (route_id, direction_id, start_date, start_time) per GTFS-RT spec — all four populated on every TU; the canonical `trip_uid` covers this. **AMENDED 2026-08-23**: `arrival.delay` is NOT set (0 of ~20k arrivals in two independent snapshots 35 min apart; the 2026-08-22 note said populated — no longer observed). `arrival.time`/`departure.time` always present → delay computed vs static via the canonical COALESCE (`rt_delay_source=computed`, same path as NYC/BOS/TOR). If delay reappears, tests/test_helsinki_fixtures.py flags it — flip back then. CANCELED trips observed (6/851). MQTT HFP vehicle stream (1s pings) demoted to optional stretch (live-map only); not needed for any reliability metric. |

---

## C. Tokyo — ODPT (corrected after catalog verification)

**Key correction:** the Metro/Toei *rail* GTFS-RT datasets contain **Alerts only**. Real-time train state + delay comes from the **ODPT JSON API** — so the ODPT adapter is Tokyo's primary rail source, not a fallback. All endpoints ✅ 403 without key (auth wall confirmed); free developer registration → `acl:consumerKey` query param.

### C.1 `odpt:Train` (GET `api.odpt.org/api/v4/odpt:Train?odpt:operator=odpt.Operator:TokyoMetro` / `:Toei`) → primary rail real-time
| Field | Type | Meaning | Our use |
|---|---|---|---|
| `@id`, `owl:sameAs` | URN | Unique train object id | dedup |
| `dc:date` | ISO ts | Observation timestamp | `ts_utc` |
| `odpt:operator` | URN | TokyoMetro / Toei | `agency_id` |
| `odpt:railway` | URN e.g. `odpt.Railway:TokyoMetro.Ginza` | Line | → `route_id` via mapping |
| `odpt:trainNumber` | string | Train run number | `trip_uid` component |
| `odpt:trainType` | URN (Local/Express/…) | Service class | attribute |
| `odpt:delay` | int seconds | **Operator-stated delay** | `delay_arr_sec` directly; `finalization_method='odpt_stated'` |
| `odpt:fromStation` / `odpt:toStation` | URN | Current position between stations (`toStation` null = stopped at station) | station passage events; arrival detection |
| `odpt:railDirection` | URN | Direction | `direction_id` via mapping |
| `odpt:originStation[]` / `odpt:destinationStation[]` | URN | Endpoints | headsign-equivalent |
| `odpt:carComposition` | int | Cars | optional attribute |

### C.2 `odpt:TrainInformation` — per-line status text (delay/suspension notices) → `silver.alerts` equivalent. Fields: `odpt:railway`, `odpt:trainInformationStatus`, `odpt:trainInformationText`, `dc:date`.

### C.3 GTFS-RT on the permanent center: rail Alerts (`…/gtfs/realtime/tokyometro_odpt_train_alert`, `toei_odpt_train_alert`) + **ToeiBus** full GTFS-RT (`…/gtfs/realtime/ToeiBus` ✅ exists) → decoded by the generic adapter.

### C.4 Static: Tokyo Metro publishes GTFS static on the center; ODPT static types (`odpt:Station`, `odpt:Railway`, `odpt:StationTimetable`, `odpt:TrainTimetable`) provide the schedule for delay context and the **URN↔GTFS id mapping table** (`int_odpt_stop_map` — a required, tested model).

### C.5 MLIT benchmark seed (manual quarterly): `line_name`, `odpt_railway_urn`, `month`, `delay_certificate_days`, `avg_delay_minutes` (where published), `source_url`.

---

## D. Static GTFS (all cities) — weekly versioned pulls → `silver.gtfs_static_*`

Verified static URLs (add per city as verified): **NYC subway** = `https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip` (✅ 200 verified 2026-08-22, 19.4 MB, Last-Modified same day; **in use since P5**. Supplemented includes service-change trips, which fixes the audited weekend delay-bound warns. The base `gtfs_subway.zip` is also 200 and remains the fallback; the legacy `web.mta.info/developers/data/nyct/subway/google_transit.zip` 301-redirects to the base zip).

- **Boston (MBTA)** = `https://cdn.mbta.com/MBTA_GTFS.zip` (✅ 200 verified 2026-08-23, 18.6 MB, `x-amz-meta-feed-version: Summer 2026 … version D`, Last-Modified 2026-08-19).
- **Toronto (TTC)** = `https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/7795b45e-e65a-4465-81fc-c36b9dfff169/resource/cfb6b2b8-6191-41e3-bda1-b175c51148cb/download/opendata_ttc_schedules.zip` (✅ 200 verified 2026-08-23, 35.0 MB, Last-Modified 2026-07-13). Resolved via the open.toronto.ca CKAN API — package `ttc-routes-and-schedules`, resource "TTC Routes and Schedules Data"; the resource id is stable and the zip is replaced in place. If the direct URL ever 404s, re-resolve: `GET ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=ttc-routes-and-schedules`.
- **Helsinki (HSL)** = `https://dev.hsl.fi/gtfs/hsl.zip` (✅ verified 2026-08-23: 301 → `infopalvelut.storage.hsldev.com//gtfs/hsl.zip`, final 200, 65.5 MB, Last-Modified 2026-08-21. Use the stable `dev.hsl.fi` URL; HTTP clients follow the redirect).
- **DC (WMATA)** ships the schedule as TWO zips, both **GET with the `api_key` header** (HEAD 404s — WMATA's gateway matches GET only): rail `https://api.wmata.com/gtfs/rail-gtfs-static.zip` (✅ 200 verified 2026-08-23, 2.5 MB) and bus `https://api.wmata.com/gtfs/bus-gtfs-static.zip` (✅ 200, 49.8 MB). [rev P3b, integrator 2026-08-24] `static_gtfs` now takes a **source map** (`{name: {url, auth}}`, normalized by `ingestion/city_static.py`) so both zips load under ONE `gtfs_version_id` — versioning them separately would hide one mode behind the staging models' `max(gtfs_version_id)` per city.
- **SF Bay (511)** = `https://api.511.org/transit/datafeeds?operator_id=RG` + `api_key` query param (✅ 200 verified 2026-08-24, **68.2 MB**, 25 members). Costs 1 of the key's 60 req/hr, once a week. **Request it gzip-capable** (`curl --compressed`; `requests` sends `Accept-Encoding` by default) — an uncompressed request returns 500. Every RT `trip_id` in the fixture joins this zip exactly (2,494/2,494), which moved sf to the matcher's exact branch.
- **Zurich (Swiss NATIONAL static)** = `https://data.opentransportdata.swiss/en/dataset/timetable-2026-gtfs2020/permalink` (✅ verified 2026-08-23: 302 → `gtfs_fp2026_20260819.zip` → signed R2 URL, final 200, **235.1 MB**, Last-Modified 2026-08-20, NO auth on any hop — HTTP clients follow). Permalink is timetable-YEAR-scoped: expect an annual rollover to `…/timetable-2027-gtfs2020/permalink` each December. Largest zip of the set — Spark parses it as-is (canonical projection drops extra columns; silver partition-pruned by city+version).

| File | Key fields we use |
|---|---|
| `agency.txt` | agency_id, agency_name, **agency_timezone** (cross-check vs dim_city) |
| `routes.txt` | route_id, agency_id, route_short_name, route_long_name, **route_type** (0 tram, 1 metro, 2 rail, 3 bus, 4 ferry), route_color |
| `trips.txt` | trip_id, route_id, **service_id**, direction_id, trip_headsign, shape_id, block_id |
| `stops.txt` | stop_id, stop_name, stop_lat, stop_lon, **parent_station**, location_type, zone_id |
| `stop_times.txt` | trip_id, stop_id, stop_sequence, **arrival_time / departure_time (may exceed 24:00:00)**, **timepoint** (1 = scheduled timepoint → early-departure rule applies; absent → rail defaults all-timepoint, bus defaults none) |
| `calendar.txt` / `calendar_dates.txt` | service_id → active service dates (+exceptions) |
| `shapes.txt` | shape_id, lat/lon sequence → route map paths |
| `frequencies.txt` | rarely present; when present, headway_secs is authoritative — otherwise **scheduled headway is derived** from consecutive scheduled arrivals per (route, direction, stop) |

## E. Open-Meteo (✅ both APIs verified 200, no key)
Forecast `api.open-meteo.com/v1/forecast` (hourly go-forward) + Archive `archive-api.open-meteo.com/v1/archive` (multi-year backfill). Per city centroid, hourly: `time`, `temperature_2m` (°C), `precipitation` (mm), `rain`, `snowfall` (cm), `wind_speed_10m`, `weather_code` (WMO → `dim_weather.condition_bucket` via seed map).

## F. Canonical Kafka envelope (every adapter emits)
```json
{"city":"helsinki","agency":"HSL","feed":"trip_updates",
 "fetched_at":"2026-08-22T20:11:04Z","source_format":"gtfs_rt",
 "schema_version":2,"payload":[ ...records in A.2 shape... ]}
```
`trip_uid` (computed in silver, used everywhere downstream) = `hash(city, service_date, COALESCE(trip_id, route_id||'-'||direction_id||'-'||start_time))` — handles HSL's empty trip_ids, NYC's matched trips, and TTC's ADDED runs with one rule.

`service_date` in silver = feed `start_date` when set, else fallback from `fetched_at` local time minus a cutover offset. **[rev P3 2026-08-23]** Default offset −12h (GTFS noon rule); cities whose feed NEVER sets start_date get a per-city offset because −12h misdates every record fetched between local midnight and the cutover: **TTC −4h** (service day rolls ~04:00; fixture-verified 0/1,522 trips carry start_date). Map lives in `spark_jobs/silver_normalize.py` `CITY_FALLBACK_CUTOVER_HOURS`.
