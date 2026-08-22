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
| **NYC (MTA subway)** | `api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs[-ace,-bdfm,-g,-jz,-nqrw,-l,-si]` (8 feeds, TU+VP+Alert combined) | none | ✅ 200, feed_age 3–8s, ACE: TU=79/VP=79; 1-7: TU=201/VP=114 | ① `arrival.delay` NOT set (✅ `delay_set=False`) → we compute vs schedule. **Exception (fixtures 2026-08-22): the L feed (CBTC) DOES set `arrival.delay` and `stop_time_update.stop_sequence` (197/201 STUs); the other 7 feeds set neither.** The COALESCE delay rule absorbs this. ② trip_id is origin-time-encoded (✅ `070950_A..S58R`) ≠ static trip_id → NYC matcher on (route, direction, start_date, origin time = prefix/100 min after midnight); `trip.start_time` is sometimes empty → origin time always taken from the trip_id prefix. ③ VP has NO lat/lon for subway (stop-relative only). ④ NYCT protobuf extension present (`nyct_trip_descriptor`: train_id, is_assigned, direction; `nyct_stop_time_update`: scheduled_track, actual_track) — parse train_id, ignore rest. ⑤ `stop_sequence` absent outside the L feed → derived from matched static `stop_times` by stop_id. |
| **Chicago (CTA)** | `transitdata.transitchicago.com/GtfsRealtime/{TripUpdates,VehiclePositions,ServiceAlerts}.pb` (+`.json`) | **free key required**, `?key=` (✅ keyless request returns HTML page, not a feed) | endpoint live | Beta program; register via CTA Developer Center. Train Tracker/Bus Tracker JSON APIs remain as backup. Rail + bus in one program. |
| **DC (WMATA)** | `api.wmata.com/gtfs/{rail,bus}-gtfsrt-{tripupdates,vehiclepositions,alerts}.pb` | free key, header `api_key`; default 50k calls/day | ✅ 401 (auth wall confirmed) | 6 endpoints × 30s = 17k/day, inside quota. |
| **Boston (MBTA)** | `cdn.mbta.com/realtime/{TripUpdates,VehiclePositions,Alerts}.pb` | none for GTFS-RT (V3 JSON:API key optional) | ✅ 200, TU=1671, VP=522, age 1–2s | Occupancy populated on many vehicles. Cleanest feed of the set. |
| **SF Bay (511.org)** | `api.511.org/transit/{tripupdates,vehiclepositions,servicealerts}?agency=RG&api_key=…` | free token; **60 req/hr default** | ✅ 401 (auth wall confirmed) | `agency=RG` = all ~30 operators in one response; poll at 90–120s (3 endpoints × 40/hr = 120 → request limit increase, or 2 endpoints at 60s + alerts at 5min). Multi-agency: every record carries agency prefix → `agency_id`. |
| **Toronto (TTC)** | `bustime.ttc.ca/gtfsrt/{trips,vehicles,alerts}` | none; **attribution required** (Open Government Licence – Toronto) | ✅ 200, TU=1876, VP=1458, age 2–18s | Surface only (bus+streetcar); subway = alerts only → recorded in completeness matrix. ✅ 59/1883 trips have negative trip_ids = ADDED/unscheduled runs: count in service volume, exclude from OTP. |
| **Zurich (opentransportdata.swiss)** | `api.opentransportdata.swiss/la/gtfs-rt` (✅ 401 = correct endpoint; `gtfsrt2020` is dead, ✅ 404) | free token, `Authorization` header | ✅ | National feed (all CH operators). Filter to Zurich in silver via route allow-list built from static GTFS (agencies: VBZ + ZVV-area + SBB S-Bahn lines serving canton ZH stops). National static GTFS is large → Spark parses it. |
| **Helsinki (HSL)** | `realtime.hsl.fi/realtime/trip-updates/v2/hsl`, `…/service-alerts/v2/hsl` | **none** | ✅ 200, TU=942, Alerts=21, age 13–15s | **KEPT — now a zero-key generic GTFS-RT city.** ✅ `trip_id` is EMPTY: trips resolved by (route_id, direction_id, start_date, start_time) per GTFS-RT spec — the canonical `trip_uid` covers this. ✅ `arrival.delay` populated directly. MQTT HFP vehicle stream (1s pings) demoted to optional stretch (live-map only); not needed for any reliability metric. |

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
