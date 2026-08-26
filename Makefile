SHELL := /bin/bash
export JAVA_HOME ?= /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

.PHONY: up down record-fixtures poll-nyc poll-boston poll-toronto poll-helsinki \
	poll-dc poll-sf poll-zurich \
	spark-local gtfs-static dbt-build test lint dagster-deploy p5-backfill-weather

up:
	docker compose up -d --wait

down:
	docker compose down

# Bare target re-records EVERY configured city, incl. sf (3 requests against the
# 60/hr-limited 511 key). Prefer per-city: uv run python -m ingestion.record_fixtures <city>
record-fixtures:
	uv run python -m ingestion.record_fixtures

poll-nyc:
	uv run python -m ingestion.poller nyc

poll-boston:
	uv run python -m ingestion.poller boston

poll-toronto:
	uv run python -m ingestion.poller toronto

poll-helsinki:
	uv run python -m ingestion.poller helsinki

poll-dc:
	uv run python -m ingestion.poller dc

# 511 hard limit 60 req/hr per key: sf polls 3 endpoints at 200s (54 req/hr).
# Never run this alongside another consumer of BAY511_API_TOKEN.
poll-sf:
	uv run python -m ingestion.poller sf

poll-zurich:
	uv run python -m ingestion.poller zurich

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

# [rev 2026-08-25] Both go through scripts/tf.sh. It writes the plan OUTSIDE the repo
# (a saved plan embeds variable values in cleartext — that is how four live API keys
# reached git) and sets the snowflake provider up for key-pair auth so `plan` can run at
# all. See the header of that script; both traps cost real incidents.
infra-plan:
	bash scripts/tf.sh plan

infra-apply:
	bash scripts/tf.sh apply

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

# P6 dashboard, local run against prod gold (read path only). Deployed variant
# needs the TRANSIT_READER role first — see dashboard/app/lib.py header.
dashboard:
	uv run --group dashboard streamlit run dashboard/app/Home.py
