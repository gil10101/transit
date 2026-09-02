# CLAUDE.md — Transit Pulse handoff

You are building **Transit Pulse**: a multi-city transit reliability warehouse answering
"Which cities run the most reliable public transit?" across 9 cities
(NYC, Chicago, DC, Boston, SF Bay, Toronto, Zurich, Helsinki, Tokyo).
Stack: Terraform · Kafka · Spark Structured Streaming · S3/Iceberg · Snowflake · dbt · Dagster · Docker · Streamlit+pydeck.

The planning phase is DONE. Sources were live-verified on 2026-08-22 (feeds fetched and
protobuf-decoded). Your job is execution, not re-planning.

## Doc map (read before writing code — these are the spec)
| File | Answers |
|---|---|
| `docs/transit-pulse-plan.md` | Architecture, component choices + rationale, roadmap, cost, risks |
| `docs/01-data-dictionary.md` | Every field from every source, verified per-city quirks, canonical envelope, `trip_uid` rule |
| `docs/02-warehouse-schema.md` | Gold star schema (dims/facts, grains, unique keys), silver tables, capacity math |
| `docs/03-dbt-spec.md` | Models, materializations, incremental configs, tests, macros, vars, slim CI |
| `docs/04-deliverables-todo.md` | Phase checklists with acceptance criteria, keys to obtain, open decisions |
| `docs/05-pipeline-walkthrough.md` | End-to-end onboarding: every stage + table fields, all filters and why, what's running |
| `docs/06-business-answers.md` | The 8 sub-questions with their CURRENT measured answers, mode coverage per source, and what each feed does not give us. Numbers come from `analysis/business_questions.sql` — re-run it, never hand-edit them |

Docs are the source of truth. When reality diverges (a feed changes, a field is missing),
**amend the doc in the same PR** — never silently code around it.

## Locked decisions — do not re-open without asking Jake
Snowflake (dbt-duckdb for local dev) · S3 + Iceberg + Glue · Redpanda in local compose,
single-broker Kafka on EC2 in prod (MSK behind a Terraform flag) · EMR Serverless for Spark ·
Dagster (dagster-dbt, city×date partitions) · Streamlit + pydeck · k8s deferred to Phase 8 ·
Helsinki KEPT (polled GTFS-RT, no key; MQTT is stretch-only) · Tokyo rail via **odpt:Train JSON**
(its GTFS-RT is alerts-only) · Toronto scored on surface modes only.

## Verified facts you must respect (from live testing — don't "fix" these)
1. **NYC**: no API key; 8 feeds; `arrival.delay` is ABSENT (exception: the L feed sets delay + stop_sequence; other 7 set neither — fixtures 2026-08-22) → delay computed vs static schedule, COALESCE picks up L's feed delay; trip_ids are origin-time-encoded (`070950_A..S58R`) → `int_trip_matching_nyc` on (route, direction, service_date, origin-time). Subway VP has no lat/lon. NYCT protobuf extension present. Supplemented static (`gtfs_supplemented.zip`, verified 200 on 2026-08-22) in use since P5 — includes service-change trips.
2. **HSL**: no key; `trip_id` is EMPTY → trips resolve via (route_id, direction_id, start_date, start_time). `arrival.delay` is **NOT** set — the 2026-08-22 note claimed it was; amended in the dictionary 2026-08-23 and re-confirmed against live bytes 2026-08-24 (0 of 8,021 arrivals). `arrival.time` is always present → delay computed vs static via the canonical COALESCE, same path as NYC/BOS/TOR. Its OTP therefore rests entirely on the static join: per-route RT-vs-static stop overlap measured 99.6% on 2026-08-24, so the join is sound.
3. **TTC**: no key, attribution required; ~3% of trips are ADDED (negative ids) → count in service volume, exclude from OTP.
4. **CTA**: keyless request returns HTML, not protobuf → key required (`?key=`).
5. **Zurich**: correct endpoint is `api.opentransportdata.swiss/la/gtfs-rt`; `gtfsrt2020` is dead. National feed → filter to Zurich allow-list in silver.
6. **511**: 60 req/hr default → `agency=RG` + 90–120s cadence until limit increase granted.
7. **Tokyo**: `odpt:delay` (seconds, operator-stated) is authoritative → `finalization_method='odpt_stated'`; URN↔GTFS id mapping (`int_odpt_stop_map`) is required and tested. [rev 2026-09-01, key live-verified] The center serves odpt:Train for **Toei only** — Metro publishes none (status text + statics only), so Tokyo OTP is scored on Toei rail; ToeiBus GTFS-RT is VP-only. Arrival detection is transition-based on fromStation (toStation-null is unreliable). Details docs/01 §C.
8. All feed URLs live in `docs/01-data-dictionary.md` §B–C. **Never invent or "remember" a URL** — if it's not in the dictionary, ask or verify first.

## Canonical rules (implement exactly once, in shared code/macros)
- `trip_uid = hash(city, service_date, COALESCE(trip_id, route_id||'-'||direction_id||'-'||start_time))`
- `delay_pred_sec = COALESCE(arrival.delay, arrival.time − scheduled_arrival)` ; Tokyo uses stated delay. A stated delay counts only if `abs(delay) <= max_plausible_delay_sec` (86400) — feeds emit corrupt values and an impossible one is treated as absent (docs/01 §A.2).
- `service_date` = feed `start_date` when set, else local_ts − 12h, date part (GTFS noon rule; handles `25:30:00`). [rev P3] Feeds that never set start_date get a per-city fallback cutover instead of −12h (TTC: −4h — its feed sets start_date on 0 trips, and −12h misdates the midnight–noon half-day); map in `spark_jobs/silver_normalize.py`.
- Store UTC; **all analysis in local time** via `dim_city.iana_tz` (3am Tokyo compares to 3am NYC).
- Atomic fact unique key: `(city_key, service_date, trip_uid, stop_sequence)`.
- ADDED trips: volume yes, OTP no. Early bus departure at a timepoint (>60s) = failure flag.

## Engineering conventions
- Python 3.12, `uv` for deps, `ruff` + `pytest`. One Docker image for all ingestion adapters, config-driven via `ingestion/config/cities/*.yaml`.
- Repo layout exactly per `docs/transit-pulse-plan.md` §10.
- **Fixture-first development**: `make record-fixtures` pulls ONE live snapshot per open feed into `tests/fixtures/*.pb` and all unit tests decode fixtures. Live polling only via explicit `make poll-<city>` or integration flag. Minimum poll interval 30s per feed — never hammer agency endpoints from tests or loops.
- Secrets in `.env` (gitignored): `CTA_API_KEY`, `WMATA_API_KEY`, `BAY511_API_TOKEN`, `SWISS_OTD_TOKEN`, `ODPT_CONSUMER_KEY`. `.env.example` checked in. Never print or commit keys.
- Makefile targets: `up` (compose), `record-fixtures`, `poll-nyc`, `spark-local`, `dbt-build`, `test`, `lint`.
- Small commits; each PR-sized change runs `make test` green. When touching dbt, run `dbt build --select state:modified+` against duckdb.

## Guardrails
- No `terraform apply` without showing the plan and getting explicit approval; nothing cloud-side before Phase 2. AWS budget alarm at $50 is part of the first apply.
- Don't add cities, tools, or metrics beyond the docs without asking — scope creep is the main project risk.
- If a dbt test fails on real data, investigate the data first; loosening a test threshold requires a doc amendment explaining why.

## Build order & current status
- [ ] **P0 Scaffold**: repo tree, compose stack (Redpanda, MinIO, Dagster, dbt-duckdb), Makefile, CI skeleton, fixtures recorded. DoD: `docker compose up` → working local stack.
- [ ] **P1 NYC vertical slice**: MTA poller → Kafka → Spark bronze/silver (local) → NYC trip matcher → `int_stop_events_finalized` v1 → OTP-by-hour chart. DoD: one day of NYC data yields believable delays; 10 trips spot-checked by hand.
- [ ] P2 cloud deploy → P3 GTFS-RT fan-out (BOS, TOR, HEL, CHI, DC, SF, ZRH) → P4 Tokyo → P5 metrics → P6 scorecard/dashboard. Details + acceptance criteria in `docs/04-deliverables-todo.md`.

Owner: Jake. Communication style: short, direct; propose → build → show evidence. When blocked on a judgment call, present the 2 options with a recommendation in ≤5 lines and keep moving on what isn't blocked.
