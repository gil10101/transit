# Transit Pulse — Deliverables & TODO

## Verification status (live-tested 2026-08-22)

| Source | Status | Evidence |
|---|---|---|
| NYC MTA subway (8 GTFS-RT feeds) | ✅ WORKS, no key | 200 OK, feed age 3–8s, TU+VP populated; trip_id format + missing delay field confirmed → NYC matcher + computed delay required |
| Boston MBTA | ✅ WORKS, no key | 200 OK, 1,671 trip updates, 522 vehicles, age 1–2s |
| Toronto TTC (surface) | ✅ WORKS, no key | 200 OK, 1,876 TU / 1,458 VP; 59 ADDED trips observed → handled in schema |
| Helsinki HSL | ✅ WORKS, no key | 200 OK, 942 TU + 21 alerts; empty trip_id + populated delay field confirmed → trip_uid rule covers it |
| Chicago CTA | ✅ endpoint live, key required | keyless request returns HTML, not a feed → register (free) |
| DC WMATA | ✅ exists behind auth | 401 as expected |
| SF Bay 511 | ✅ exists behind auth | 401 as expected; 60 req/hr default limit |
| Zurich opentransportdata.swiss | ✅ exists behind auth, correct endpoint = `/la/gtfs-rt` | 401; legacy `gtfsrt2020` is dead (404) |
| Tokyo ODPT (odpt:Train + ToeiBus GTFS-RT) | ✅ exists behind auth | 403 both; **rail GTFS-RT = alerts only → odpt:Train JSON is the primary rail source (plan corrected)** |
| Open-Meteo forecast + historical archive | ✅ WORKS, no key | 200 both — multi-year weather backfill confirmed available |

**Helsinki decision: KEPT.** It turned out to be one of the *easiest* cities — zero keys, generic polled GTFS-RT, delay provided directly in the feed. The MQTT/HFP stream is demoted to optional stretch (live-map eye candy only). Custom adapters are now down to exactly one: Tokyo.

**Stack capacity: confirmed.** Total ingest <0.5 MB/s across all nine cities (measured payload sizes), ~2–4M finalized fact rows/day. Single-broker Kafka, EMR Serverless micro-batches, and Snowflake XS all have ≥10× headroom. Only sizing rule: filter the Zurich national feed to the Zurich allow-list at the silver step.

## Keys & registrations to obtain (all free — do in week 1)
- [ ] CTA Developer Center → GTFS-RT key
- [ ] WMATA developer.wmata.com → api key (default 50k/day is enough)
- [ ] 511.org token request form; reply asking for rate-limit increase (else poll at 90–120s)
- [ ] opentransportdata.swiss account → token
- [ ] ODPT developer site → consumerKey (permanent center, not the Challenge)
- [ ] MBTA V3 key (optional, GTFS-RT is keyless)
- [ ] AWS account guardrails: budget alarm at $50, IAM user for Terraform

## Deliverables (definition of done per artifact)
1. **Repo** — layout per plan §10; README with architecture diagram, demo GIF, findings, cost note, ATTRIBUTION.md (TTC/ODPT/511/Swiss).
2. **Terraform** — `apply` from empty account stands up lake, Kafka, EMR Serverless, Snowflake (provider), services box; `destroy` leaves only S3. CI: plan on PR, apply on main.
3. **Ingestion** — one Docker image; 8 cities via `config/cities/*.yaml` on the generic GTFS-RT adapter, +1 ODPT adapter (Tokyo). Health metrics per poller.
4. **Spark** — bronze_writer + silver_normalize (Structured Streaming, checkpointed, Iceberg); Zurich allow-list filter; exactly-once semantics documented.
5. **dbt** — per spec doc: all models, snapshots, seeds, 100% mart columns documented, tests green, slim CI wired.
6. **Dagster** — city×date partitioned assets, S3 sensor → incremental dbt, freshness/completeness asset checks, weekly GTFS refresh, monthly report job, webhook alerting.
7. **Dashboard** — 4 pages (scorecard, H3 hexmap small-multiples, route explorer, live-ish map) + ops page; deployed and linkable.
8. **Analysis writeup** — "Which city runs the most reliable transit?" with the composite score table, EWT-vs-OTP methodology section, MLIT validation chart, weather-sensitivity chart.

## TODO by phase (acceptance criteria in bold)
**P0 Scaffold (wk 1)** — [ ] repo + compose stack (Redpanda, MinIO, Dagster, dbt-duckdb) · [ ] Terraform backend · [ ] CI skeleton · [ ] all keys above. **`docker compose up` gives a working local stack.**
**P1 NYC vertical slice (wk 1–2)** — [ ] MTA poller → Kafka · [ ] Spark bronze+silver · [ ] NYC trip matcher · [ ] `int_stop_events_finalized` v1 · [ ] OTP-by-hour chart. **One day of NYC data produces believable delay numbers (spot-check 10 trips by hand against a live tracker).**
**P2 Cloud deploy (wk 3)** — [ ] terraform apply prod · [ ] NYC flowing e2e in cloud. **Fresh clone → running pipeline in <1 hr.**
**P3 GTFS-RT fan-out (wk 3–4)** — [ ] Boston · [ ] Toronto (+ADDED handling) · [ ] Helsinki (trip_uid path) · [ ] Chicago · [ ] DC · [ ] SF (RG cadence) · [ ] Zurich (allow-list). **Each city = yaml + ≤1 quirk PR; completeness ≥85% after 48h.**
**P4 Tokyo (wk 5)** — [ ] ODPT adapter (odpt:Train + TrainInformation) · [ ] URN↔GTFS mapping + tests · [ ] ToeiBus via generic adapter · [ ] MLIT seed. **Ginza-line delays match Metro's own status page during a disruption.**
**P5 Metrics layer (wk 5–6)** — [x] headways + EWT · [x] int_service_frequency · [x] activity, alerts, weather (backfill 2yr + hourly) · [x] completeness · [x] all dbt tests/docs · [x] Dagster partitions/sensors/checks. **`dbt build` green; a killed feed shows up in completeness within an hour.**
> [rev P5] SCD2 dims (dim_route/dim_stop snapshots) moved to P6 — P5 marts join on natural keys (city, route_id, stop_id); NYC-only data makes them unambiguous until fan-out.
> [rev P5] city×date partitioning of the dbt assets (docs/03 §10) deferred to P3 fan-out (single city, unpartitioned assets until then). The 2yr weather backfill asset is built but pending DAGSTER_SVC creds — run `make p5-backfill-weather` once enabled.
**P6 Scorecard + dashboard (wk 7–8)** — [ ] SCD2 dims + `*_key` FK swap in marts (moved from P5) · [ ] composite score (weights as vars, methodology_version) · [ ] 4 pages + ops · [ ] MLIT validation notebook · [ ] README polish + GIF. **A stranger can answer "which city is most reliable at 8am?" in two clicks.**
**P7 Run & write (ongoing)** — [ ] 30+ days accrued · [ ] monthly auto-report · [ ] blog writeup.
**P8 Optional** — [ ] HSL MQTT live layer · [ ] EKS migration · [ ] JR East (Challenge window) · [ ] Sydney TfNSW (has official *historical* GTFS-RT archives — instant multi-year backfill for city #10) · [ ] MTA buses.

## Open decisions (fine to defer)
- Snowflake trial → paid vs Redshift Serverless switch after trial credits (Terraform module swap either way).
- MSK Serverless vs EC2 Kafka in prod (flag in kafka module; EC2 default for cost).
- Alerting target: Slack vs Discord webhook.
