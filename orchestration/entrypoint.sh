#!/bin/sh
# Fetch DAGSTER_SVC's Snowflake key from SSM (chmod 600) before dagster starts.
# No-op when SNOWFLAKE_KEY_SSM_PARAM is unset (local dev with an empty env).
# A hard SSM failure exits nonzero on purpose: restart-looping beats running
# half-configured.
set -e
if [ -n "${SNOWFLAKE_KEY_SSM_PARAM:-}" ]; then
    python -m transit_dagster.fetch_snowflake_key
fi
exec "$@"
