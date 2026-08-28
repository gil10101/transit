# Transit Pulse — Deliverables & TODO

## Verification status (live-tested 2026-08-22)

| Source | Status | Evidence |
|---|---|---|
| NYC MTA subway (8 GTFS-RT feeds) | ✅ WORKS, no key | 200 OK, feed age 3–8s, TU+VP populated; trip_id format + missing delay field confirmed → NYC matcher + computed delay required |
| Boston MBTA | ✅ WORKS, no key | 200 OK, 1,671 trip updates, 522 vehicles, age 1–2s |
| Toronto TTC (surface) | ✅ WORKS, no key | 200 OK, 1,876 TU / 1,458 VP; 59 ADDED trips observed → handled in schema |
| Helsinki HSL | ✅ WORKS, no key | 200 OK, 942 TU + 21 alerts; empty trip_id + populated delay field confirmed → trip_uid rule covers it |
| Chicago CTA | ✅ endpoint live, key required | keyless request returns HTML, not a feed → register (free) |
| DC WMATA | ✅ WORKS with key (verified 2026-08-23) | 200, rail TU ~94KB decodes; 6 endpoints × 30s fits 50k/day |
| SF Bay 511 | ✅ WORKS with token (verified 2026-08-23) | 200, ~1.5MB; 60 req/hr TOTAL until increase → poll 200s (docs/01 §B) |
| Zurich opentransportdata.swiss | ✅ WORKS with token (verified 2026-08-23), endpoints `/la/gtfs-rt` + `/la/gtfs-sa` | 200, ~6MB national feed; RAW Authorization header (no Bearer), follow redirects; separate SA token; legacy `gtfsrt2020` dead (404) |
| Tokyo ODPT (odpt:Train + ToeiBus GTFS-RT) | ✅ exists behind auth | 403 both; **rail GTFS-RT = alerts only → odpt:Train JSON is the primary rail source (plan corrected)** |
| Open-Meteo forecast + historical archive | ✅ WORKS, no key | 200 both — multi-year weather backfill confirmed available |

**Helsinki decision: KEPT.** It turned out to be one of the *easiest* cities — zero keys, generic polled GTFS-RT, delay provided directly in the feed. The MQTT/HFP stream is demoted to optional stretch (live-map eye candy only). Custom adapters are now down to exactly one: Tokyo.

**Stack capacity: confirmed.** Total ingest <0.5 MB/s across all nine cities (measured payload sizes), ~2–4M finalized fact rows/day. Single-broker Kafka, EMR Serverless micro-batches, and Snowflake XS all have ≥10× headroom. Only sizing rule: filter the Zurich national feed to the Zurich allow-list at the silver step.

## Keys & registrations to obtain (all free — do in week 1)
- [x] CTA Developer Center → GTFS-RT key — key issued (in `.env`) but **beta activation pending**; chicago stays out of LIVE_CITIES until it works
- [x] WMATA developer.wmata.com → api key (default 50k/day is enough) — in `.env`, verified 2026-08-23
- [x] 511.org token request form — in `.env`, verified 2026-08-23; **rate-limit increase still pending** → poll at 200s (docs/01 §B rev), not 90–120s
- [x] opentransportdata.swiss account → **two** tokens, one per API product (gtfs-rt + gtfs-sa) — in `.env`, verified 2026-08-23; irreplaceable if revoked
- [ ] ODPT developer site → consumerKey (permanent center, not the Challenge)
- [ ] MBTA V3 key (optional, GTFS-RT is keyless)
- [x] AWS account guardrails: budget alarm, IAM user for Terraform (P2)

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
**P3 GTFS-RT fan-out (wk 3–4)** — [x] Boston · [x] Toronto (+ADDED handling) · [x] Helsinki (trip_uid path) · [ ] Chicago · [x] DC · [x] SF (RG cadence) · [~] Zurich (feed + matcher built; **allow-list NOT built** — see below). **Each city = yaml + ≤1 quirk PR; completeness ≥85% after 48h.**
> [rev P3 batch 1, 2026-08-23] Boston/Toronto/Helsinki BUILT (yaml+fixtures, dbt generic matcher, 4-city TP_CITY_TZS/pollers/Dagster fan-out) but PENDING the 48h completeness ≥85% acceptance — rollout checklist in docs/operations.md "P3 batch 1 rollout"; re-verify completeness there before calling the cities done. Batch 2: DC/SF/Zurich keys verified live in `.env`; Chicago pending GTFS-RT beta key activation.
> [rev P3 batch 2b, integrator 2026-08-24] **Zurich's poller is deliberately NOT deployed with DC and SF.** The `/la/gtfs-rt` feed is national, and CLAUDE.md's locked decision filters it to a Zurich route allow-list in silver — that allow-list needs the 235 MB national static parsed first, and `spark_jobs/silver_normalize.py` has no filter yet. Deploying Zurich before both land would file every Swiss operator under `city='zurich'` (wrong answers to the reliability question) and archive ~0.5–1 TB/month of mostly-irrelevant raw protobuf against the <$100/mo budget. Order: static refresh (Zurich zip parses) → generate `zurich_route_allowlist` from the parsed stops/routes → silver filter → deploy the Zurich poller. DC + SF ship now.
> [rev P3 batch 2, 2026-08-23] DC/SF/Zurich BUILT (yaml+fixtures+keyed auth, 7-city TP_CITY_TZS/pollers/Dagster fan-out, keys as SSM SecureStrings via write-only terraform args) but PENDING the 48h completeness ≥85% acceptance — rollout checklist in docs/operations.md "P3 batch 2 rollout" (deploy images → export TF_VARs → apply → re-land user_data). SF polls at 200s until 511 grants the rate increase. Chicago = batch 2b on key activation.
> [rev P3 batch 1] TTC quirks beyond ADDED: RT trip_ids share no namespace with the CKAN static (matcher falls back to route+origin-time, confidence 0.7); feed sets start_date on 0 trips (service-date fallback cutover −4h, see docs/01 §F); static omits `timepoint`, so the early-departure bus rule is live for MBTA/HSL but structurally false for TTC (bus-defaults-none, §D).
**P4 Tokyo (wk 5)** — [ ] ODPT adapter (odpt:Train + TrainInformation) · [ ] URN↔GTFS mapping + tests · [ ] ToeiBus via generic adapter · [ ] MLIT seed. **Ginza-line delays match Metro's own status page during a disruption.**
**P5 Metrics layer (wk 5–6)** — [x] headways + EWT · [x] int_service_frequency · [x] activity, alerts, weather (backfill 2yr + hourly) · [x] completeness · [x] all dbt tests/docs · [x] Dagster partitions/sensors/checks. **`dbt build` green; a killed feed shows up in completeness within an hour.**
> [rev P5] SCD2 dims (dim_route/dim_stop snapshots) moved to P6 — P5 marts join on natural keys (city, route_id, stop_id); NYC-only data makes them unambiguous until fan-out.
> [rev P5] city×date partitioning of the dbt assets (docs/03 §10) deferred to P3 fan-out (single city, unpartitioned assets until then). [rev P3 batch 1] still unpartitioned at 4 cities — whole-warehouse builds remain cheap; revisit at batch 2/P6. The 2yr weather backfill asset is built but pending DAGSTER_SVC creds — run `make p5-backfill-weather` once enabled. [rev P6 2026-08-26] LAUNCHED on the services box (detached, via SSM); and weather is finally READ: seed `dim_weather` + `stg_weather__hourly` + `fct_weather_hourly` ship the docs/02 join contract (city_key, local_date, local_hour), and business Q7 is a runnable query in `analysis/business_questions.sql` instead of a placeholder.
**P6 Scorecard + dashboard (wk 7–8)** — [x] SCD2 dims + `*_key` FK swap in marts (2026-08-26: derived from silver versions, NOT dbt snapshots — docs/02 amended; dim_route/dim_stop/dim_date/dim_time_local + point-in-time FKs on 4 facts via `scd2_join`, interval-disjoint contract test) · [x] composite score (2026-08-26: weights as vars, methodology_version, refuses <`scorecard_min_judged_days` judged days — 0 rows on 4 days of data IS the acceptance state; grain-pinned by `assert_scorecard_totals_match_route_grain`; NULL inputs renormalise, never score) · [x] 4 pages + ops (2026-08-26: Streamlit+pydeck — scorecard w/ honest empty state, H3 delay hexmap small-multiples on one shared scale, route explorer (path via ordered stops — shapes.txt not ingested), live map w/ per-city lag labels, ops w/ gold-staleness deploy-trap detector; `make dashboard`) · [ ] MLIT validation notebook (blocked: needs Tokyo/ODPT key) · [ ] README polish (rewritten 2026-08-26) + GIF (pending). **A stranger can answer "which city is most reliable at 8am?" in two clicks** — city → hour column on the route-explorer heatmap.
> [rev P6 2026-08-28] Public face split from the internal dashboard: `site/` is a static page (deck.gl + hand-rolled SVG charts, gillu.me design system, light/dark) deployed to Vercel as the portfolio wildcard link. Streamlit cannot run on Vercel (long-lived websocket server), so the static site snapshots the warehouse instead: `make site-data` writes `site/data/*.json` (standings use the exact provisional-metric SQL from Home.py; maps reuse the proof-render extraction incl. the Zurich stop-times path derivation and the high-volume-day guard) and the data files are committed so a deploy is a plain static publish. Streamlit stays the internal tool (`make dashboard`).
**P7 Run & write (ongoing)** — [ ] 30+ days accrued · [ ] monthly auto-report · [ ] blog writeup.
**P8 Optional** — [ ] HSL MQTT live layer · [ ] EKS migration · [ ] JR East (Challenge window) · [ ] Sydney TfNSW (has official *historical* GTFS-RT archives — instant multi-year backfill for city #10) · [ ] MTA buses.

## Open decisions (fine to defer)
- Snowflake trial → paid vs Redshift Serverless switch after trial credits (Terraform module swap either way).
- MSK Serverless vs EC2 Kafka in prod (flag in kafka module; EC2 default for cost).
- Alerting target: Slack vs Discord webhook.
