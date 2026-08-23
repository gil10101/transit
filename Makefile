SHELL := /bin/bash
export JAVA_HOME ?= /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

.PHONY: up down record-fixtures poll-nyc spark-local gtfs-static dbt-build test lint \
	dagster-deploy p5-backfill-weather

up:
	docker compose up -d --wait

down:
	docker compose down

record-fixtures:
	uv run python -m ingestion.record_fixtures

poll-nyc:
	uv run python -m ingestion.poller nyc

spark-local:
	uv run python -m spark_jobs.run_local

gtfs-static:
	uv run python -m spark_jobs.gtfs_static_parse nyc

dbt-build:
	cd dbt/transit && uv run dbt build --profiles-dir .

test:
	uv run pytest

lint:
	uv run ruff check .
	uv run ruff format --check .

# --- cloud (Phase 2) ---

infra-bootstrap:
	cd infra/bootstrap && terraform init && terraform apply

infra-init:
	cd infra/envs/dev && terraform init \
	  -backend-config="bucket=$$(cd ../../bootstrap && terraform output -raw state_bucket)"

infra-plan:
	cd infra/envs/dev && terraform plan -out=tfplan

infra-apply:
	cd infra/envs/dev && terraform apply tfplan

deploy-images:
	bash scripts/deploy_images.sh

# rebuild/push only the Dagster image (fast orchestration iteration), then
# restart on the box: aws ssm ... 'systemctl restart transit.service'
dagster-deploy:
	bash scripts/deploy_images.sh dagster

emr-drain:
	bash scripts/submit_emr_drain.sh

snowflake-refresh:
	uv run python scripts/snowflake_register_iceberg.py

# --- P5 ---

# One-off 2-year Open-Meteo archive backfill into TRANSIT.SILVER.WEATHER_HOURLY.
# Manual-trigger by design (no schedule). Runs inside the dagster container, which
# needs Snowflake env (SNOWFLAKE_ACCOUNT/USER/ROLE + key) — see docs/operations.md
# "Dagster (P5)". On the services box run the same command via SSM shell.
p5-backfill-weather:
	docker compose exec dagster-daemon \
	  dagster asset materialize -m transit_dagster.definitions --select weather_backfill_2yr
