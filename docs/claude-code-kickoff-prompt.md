# Kickoff prompt for Claude Code — Session 1 (Phase 0 + start of Phase 1)

Paste everything below the line into Claude Code from the repo root. Before pasting: create the repo, put `CLAUDE.md` at the root, and copy the five planning docs into `docs/`.

---

Read CLAUDE.md and the five docs in docs/ before writing any code. They contain verified feed facts and locked decisions — treat them as the spec and don't re-plan.

This session: complete Phase 0 and get Phase 1 running locally. Scope is LOCAL ONLY — no Terraform, no AWS, no cloud anything, NYC only. Do not add other cities yet.

Build in this order, committing at each numbered step:

1. Scaffold the repo tree exactly per docs/transit-pulse-plan.md §10 (empty modules are fine where noted). Add pyproject (uv, Python 3.12), ruff, pytest, .env.example, .gitignore, Makefile with targets: up, record-fixtures, poll-nyc, spark-local, dbt-build, test, lint.

2. docker-compose.yml: Redpanda (+ console), MinIO, Postgres (Dagster storage), Dagster webserver+daemon. dbt targets duckdb locally — no warehouse container. Acceptance: `make up` brings everything healthy.

3. Ingestion: the generic GTFS-RT adapter (ingestion/adapters/gtfs_rt.py) driven by ingestion/config/cities/nyc.yaml — all 8 MTA subway feed URLs are in docs/01-data-dictionary.md §B. Emit the canonical envelope from §F to topics transit.trip_updates / transit.vehicle_positions / transit.alerts, keyed by city. Also write raw bytes to MinIO raw/ hourly. 30s poll floor.

4. `make record-fixtures`: one live snapshot per NYC feed into tests/fixtures/*.pb. All unit tests decode fixtures only — no live calls in tests. Write decode tests: entity counts > 0, trip_id regex matches the origin-time format, arrival.time present, arrival.delay ABSENT (this is expected — see CLAUDE.md verified fact #1).

5. Spark local (spark-local target): bronze_writer (Kafka → Iceberg-on-MinIO bronze, checkpointed) and silver_normalize (canonical typing, exact-dup drop, trip_uid computed per CLAUDE.md canonical rules). Partition city/service_date.

6. Static GTFS: downloader + Spark parse of NYC subway static into silver.gtfs_static_* with gtfs_version_id, including the >24:00:00 time handling and service_date noon−12h rule (shared util with tests).

7. dbt project per docs/03-dbt-spec.md: sources over silver (duckdb external), stg models for trip updates + static, int_trip_matching_nyc, int_gtfs_scheduled_stop_times, int_stop_events_finalized v1, fct_stop_events. Implement the delay coalesce rule and otp_band macro with the vars from the spec. dbt build green with tests.

8. Proof: after letting the poller run a few hours, a small script/notebook renders OTP% and median delay by route by local hour for NYC, and prints 10 sample finalized stop events (trip, stop, scheduled vs actual, delay) so I can spot-check them against a live tracker.

Working agreements: fixture-first, small verified commits, `make test` green before each commit. If a feed behaves differently than the data dictionary says, update the doc in the same commit and flag it to me. If you hit a judgment call, give me two options + a recommendation in ≤5 lines and continue on whatever isn't blocked. Start with step 1 and show me the tree before filling in code.
