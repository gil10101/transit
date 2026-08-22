# Transit Pulse

Multi-city transit reliability warehouse. Which cities run the most reliable public transit?

Current state: Phase 0/1 — local stack, NYC subway vertical slice.
Spec lives in `docs/` (plan, data dictionary, warehouse schema, dbt spec, deliverables).

## Local quickstart

```sh
cp .env.example .env
uv sync
make up              # Redpanda, MinIO, Postgres, Dagster
make record-fixtures # one live snapshot per NYC feed -> tests/fixtures/
make test
make poll-nyc        # MTA poller -> Kafka + MinIO raw/
make spark-local     # Kafka -> bronze/silver Iceberg on MinIO
make gtfs-static     # NYC static GTFS -> silver.gtfs_static_*
make dbt-build       # silver -> fct_stop_events (duckdb)
```

Requires: uv, Docker, Java 17 (`brew install openjdk@17`).

## Layout

Per `docs/transit-pulse-plan.md` §10. `infra/` is empty scaffolding until Phase 2 (cloud).
