SHELL := /bin/bash
export JAVA_HOME ?= /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

.PHONY: up down record-fixtures poll-nyc spark-local gtfs-static dbt-build test lint

up:
	docker compose up -d --wait

down:
	docker compose down

record-fixtures:
	uv run python -m ingestion.record_fixtures

poll-nyc:
	uv run python -m ingestion.poller nyc

spark-local:
	uv run python spark_jobs/run_local.py

gtfs-static:
	uv run python spark_jobs/gtfs_static_parse.py nyc

dbt-build:
	cd dbt/transit && uv run dbt build --profiles-dir .

test:
	uv run pytest

lint:
	uv run ruff check .
	uv run ruff format --check .
