# Dagster webserver + daemon image. Build context is the REPO ROOT (it bakes
# the dbt project and the iceberg-register script): docker compose and
# scripts/deploy_images.sh must pass `-f orchestration/Dockerfile .`.
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl git && rm -rf /var/lib/apt/lists/*

# uv resolves the whole set in one shot. dagster-dbt drives dbt-snowflake for
# the prod target; boto3 talks to EMR Serverless / S3 / SSM; the snowflake
# connector serves the refresh + weather assets.
RUN pip install --no-cache-dir uv && uv pip install --system --no-cache \
    "dagster>=1.9,<2" "dagster-webserver>=1.9,<2" "dagster-postgres" \
    "dagster-dbt" "dbt-snowflake>=1.9" "boto3>=1.34" \
    "snowflake-connector-python" "requests>=2.32" "pyyaml>=6.0"

ENV DAGSTER_HOME=/opt/dagster/home
WORKDIR /opt/dagster/app
COPY orchestration/dagster.yaml /opt/dagster/home/dagster.yaml
COPY orchestration/workspace.yaml /opt/dagster/app/workspace.yaml
COPY orchestration/entrypoint.sh /opt/dagster/app/entrypoint.sh
# single source for the silver table list (imported by transit_dagster.lib)
COPY scripts/snowflake_register_iceberg.py /opt/dagster/app/scripts/snowflake_register_iceberg.py
# city yamls: the raw-feed freshness tripwire reads feed_groups per live city
# (transit_dagster.lib.city_feed_endpoints)
COPY ingestion/config /opt/dagster/app/ingestion/config
# the city yamls plus the two pure modules that read them (the static staging
# asset normalizes static_gtfs / auth through ingestion.city_static) — never the
# adapters, which need requests + confluent_kafka
COPY ingestion/__init__.py ingestion/city_static.py /opt/dagster/app/ingestion/
COPY dbt/transit /opt/dagster/app/dbt/transit
COPY orchestration/transit_dagster /opt/dagster/app/transit_dagster

# Bake the dbt manifest for @dbt_assets. `dbt parse` renders the profile (env
# defaults) but never opens a warehouse connection.
RUN cd dbt/transit \
    && rm -rf target \
    && (test -f packages.yml && dbt deps || true) \
    && dbt parse --profiles-dir . --target prod \
    && chmod +x /opt/dagster/app/entrypoint.sh

# run as non-root: the SSM key fetch, webserver, and daemon all work without
# root, and SNOWFLAKE_PRIVATE_KEY_PATH lives under this user's home
RUN useradd --create-home appuser && chown -R appuser /opt/dagster
USER appuser

ENV PYTHONPATH=/opt/dagster/app
ENTRYPOINT ["/opt/dagster/app/entrypoint.sh"]
