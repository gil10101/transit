# Transit Pulse

A multi-city transit reliability warehouse. One question: **which cities run the most
reliable public transit — and what makes them reliable?**

**Live now: seven cities** (NYC, Boston, DC, SF Bay, Toronto, Helsinki, Zurich) polled
around the clock into one lakehouse, scored by one methodology. Chicago and Tokyo join
when their API keys clear. Full plan, verified per-feed facts, and the business answers
live in [`docs/`](docs/).

**Status: Phase 6 — scorecard + dashboard.** Phases 0–5 (NYC slice, cloud deploy,
7-city fan-out, metrics marts) are done and verified; the pipeline has run unattended
since 2026-08-23.

```
7 city pollers (GTFS-RT, per-endpoint cadence)          EC2, Docker Compose
        │ raw bytes ─────────────► S3 raw/ (replay archive)
        ▼
Kafka (KRaft, EC2) ── canonical envelopes ── transit.{trip_updates,vehicle_positions,alerts}
        ▼
EMR Serverless drains every 15 min (Spark Structured Streaming, availableNow, checkpointed)
    bronze_writer     Kafka ──► Iceberg bronze.envelopes
    silver_normalize  Kafka ──► Iceberg silver.* — typing · exact-dup drop ·
                                canonical trip_uid · national-feed allow-list
        ▼
Iceberg on S3 (Glue catalog) ── Snowflake external tables (TRANSIT.SILVER)
        ▼
dbt (dagster-dbt, 2-hourly chain: drain → refresh → build → test)
    staging → per-city trip matching → finalized stop events →
    TRANSIT.GOLD: fct_stop_events (atomic) · headways/EWT · service delivery ·
    route reliability · alerts · SCD2 dims (route/stop/date/time) ·
    fct_city_scorecard_monthly (0-100, refuses <20 judged days)
        ▼
Streamlit + pydeck dashboard (scorecard · H3 delay hexmaps · route explorer ·
live map · pipeline ops)          SNS alerting on chain failure
        ▼
site/ — static public page (deck.gl network maps · standings · hourly OTP),
snapshot data via `make site-data`, deployed on Vercel
```

Local dev is the same code against Redpanda + MinIO + duckdb (`make up`, dbt
`--target dev`); cloud is a profile switch. `infra/` is the Terraform for all of it,
behind a $50 budget alarm; real August cost ran **under $10/week** after the checkpoint-
churn and national-feed cost bugs were found and fixed (the hunt is documented in
`docs/08-production-gates.md`).

## What makes the numbers trustworthy

The design rule for gold is **contract over cleanup**: every defect that ever reached
gold got a contract test that fails on the bad shape before the fix counted as done —
degenerate `trip_uid`s, direction-grain double counts, misdated service days, schedule
echoes scored as observations. The scorecard itself refuses to emit a number for any
city with fewer than 20 closed, judged service days in a month; sub-scores with no
evidence stay NULL and their weight is renormalised rather than faked. Where a feed
simply does not publish something (Toronto's disjoint stop namespace, Helsinki's
AIS-only ferries, agencies that never emit CANCELED), the gap is measured, seeded as a
known limitation, and shown — not smoothed over.

## Quickstart (local, no keys, no cloud)

```sh
cp .env.example .env       # no keys needed for NYC
uv sync
make up                    # Redpanda(+console :8080), MinIO(:9001), Postgres, Dagster(:3070)
make record-fixtures       # one live snapshot per feed -> tests/fixtures/
make test                  # fixture-decode + unit tests, no network
make poll-nyc              # MTA poller -> Kafka + raw archive (leave running)
make spark-local           # bronze + silver streams (leave running)
make gtfs-static           # NYC static GTFS -> silver.gtfs_static_*
make dbt-build             # silver -> gold marts + tests against duckdb
make dashboard             # Streamlit app (against prod gold; needs Snowflake key)
```

Requires uv, Docker, Java 17 (`brew install openjdk@17`).

## Feed reality (fixture-verified; it shapes the whole pipeline)

Every feed lies differently. The complete field-level dictionary is
[`docs/01-data-dictionary.md`](docs/01-data-dictionary.md); the ones that bent the
architecture:

- **NYC**: 7 of 8 subway feeds omit `arrival.delay` — delay computes vs static schedule;
  trip identity is origin-time-encoded and needs its own matcher.
- **Helsinki**: `trip_id` is empty everywhere; trips resolve via
  (route, direction, start_date, start_time). Ferries have no GTFS-RT at all.
- **Toronto**: realtime and static stop ids live in different namespaces unless you read
  the *right* one of TTC's two static files; ~3% of trips are ADDED (counted in volume,
  excluded from OTP — locked rule).
- **Zurich**: the "city" feed is the whole Swiss national timetable (5,122 routes) —
  an allow-list cuts it to the 615 that are actually Zürich, on both the realtime and
  schedule sides.
- **DC**: WMATA marks ~30% of stop-time updates SKIPPED; they count as service volume,
  never as on-time observations.

## Docs

| Doc | What it answers |
|---|---|
| `docs/transit-pulse-plan.md` | architecture, component choices, roadmap |
| `docs/01-data-dictionary.md` | every field, every feed quirk, canonical rules |
| `docs/02-warehouse-schema.md` | star schema, grains, SCD2 dims, capacity |
| `docs/03-dbt-spec.md` | models, materializations, tests, macros |
| `docs/04-deliverables-todo.md` | phase checklists with acceptance criteria |
| `docs/05-pipeline-walkthrough.md` | end-to-end onboarding, every filter and why |
| `docs/06-business-answers.md` | the 8 sub-questions with measured answers |
| `docs/07-plain-english-guide.md` | the whole system in plain language |
| `docs/08-production-gates.md` | quality gates A–G, incident log, verification evidence |
| `docs/09-backfill-feasibility.md` | why GTFS-RT history cannot be backfilled, only accrued |

## Layout

| Path | Contents |
|---|---|
| `ingestion/` | config-driven adapters (`config/cities/*.yaml`), poller, fixture recorder |
| `spark_jobs/` | bronze/silver streaming, static GTFS parser, shared time semantics |
| `dbt/transit/` | staging → intermediate → marts; macros; contract tests |
| `orchestration/` | Dagster: 2-hourly chain, freshness tripwire, weather, failure alerting |
| `dashboard/` | Streamlit + pydeck, five pages (internal) |
| `site/` | Static public site for Vercel; data snapshots from `make site-data` |
| `tests/` | fixture-decode + unit tests; no live calls |
| `infra/` | Terraform: lake, network, kafka, EMR Serverless, services, monitoring, Snowflake |

## Data licenses

See [ATTRIBUTION.md](ATTRIBUTION.md). Data provided by the MTA, MBTA, WMATA, 511.org,
TTC (attribution required), HSL, opentransportdata.swiss.
