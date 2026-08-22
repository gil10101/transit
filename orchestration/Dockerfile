FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "dagster>=1.9,<2" "dagster-webserver>=1.9,<2" "dagster-postgres"

ENV DAGSTER_HOME=/opt/dagster/home
WORKDIR /opt/dagster/app
COPY dagster.yaml /opt/dagster/home/dagster.yaml
COPY workspace.yaml /opt/dagster/app/workspace.yaml
COPY transit_dagster /opt/dagster/app/transit_dagster
ENV PYTHONPATH=/opt/dagster/app
