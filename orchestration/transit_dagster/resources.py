"""Dagster resources: EMR Serverless submission and a key-pair Snowflake session.

Config is read from the environment at materialize time — the same names the
drain Lambda and the services-box compose use — so definitions import and the
UI loads with an empty env; cloud assets fail lazily with a clear message.
"""

from __future__ import annotations

import os
import time
from collections.abc import Sequence
from contextlib import contextmanager

from dagster import ConfigurableResource, get_dagster_logger

from .lib import drain_job_request, require_env, static_job_request

EMR_TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED"}


class EmrResource(ConfigurableResource):
    """Submits EMR Serverless job runs and polls to a terminal state."""

    poll_seconds: int = 30
    timeout_minutes: int = 25

    def run_drain(self) -> str:
        return self._run(drain_job_request(os.environ))

    def run_static(self, city: str) -> str:
        return self._run(static_job_request(os.environ, city=city))

    def _run(self, request: dict) -> str:
        import boto3

        log = get_dagster_logger()
        client = boto3.client(
            "emr-serverless", region_name=os.environ.get("AWS_REGION", "us-east-2")
        )
        run_id = client.start_job_run(**request)["jobRunId"]
        log.info(f"EMR {request['name']} submitted: run {run_id}")
        deadline = time.monotonic() + self.timeout_minutes * 60
        while True:
            job = client.get_job_run(applicationId=request["applicationId"], jobRunId=run_id)[
                "jobRun"
            ]
            state = job["state"]
            if state == "SUCCESS":
                log.info(f"EMR run {run_id} SUCCESS")
                return run_id
            if state in EMR_TERMINAL_STATES:  # FAILED / CANCELLED
                raise RuntimeError(f"EMR run {run_id} ended {state}: {job.get('stateDetails', '')}")
            if time.monotonic() > deadline:
                # a hung run holds the whole 4 vCPU app and queues every later
                # drain — free the capacity before giving up
                try:
                    client.cancel_job_run(applicationId=request["applicationId"], jobRunId=run_id)
                    log.warning(f"EMR run {run_id} cancelled after timeout")
                except Exception as e:  # noqa: BLE001 — cancel is best-effort
                    log.warning(f"cancel of EMR run {run_id} failed: {e!r}")
                raise TimeoutError(
                    f"EMR run {run_id} still {state} after {self.timeout_minutes} min"
                )
            log.debug(f"EMR run {run_id} {state}; polling again in {self.poll_seconds}s")
            time.sleep(self.poll_seconds)


WEATHER_TABLE = "TRANSIT.SILVER.WEATHER_HOURLY"
WEATHER_COLUMNS = (
    "city",
    "local_date",
    "local_hour",
    "temp_c",
    "precip_mm",
    "snowfall_cm",
    "wind_kph",
    "weather_code",
    "pulled_at",
)
# [rev P5] weather lands directly in Snowflake SILVER, not Iceberg (1 row/city/hr).
# pulled_at is NTZ holding UTC instants — the session below pins TIMEZONE=UTC.
WEATHER_DDL = f"""
create table if not exists {WEATHER_TABLE} (
    city varchar not null,
    local_date date not null,
    local_hour smallint not null,
    temp_c float,
    precip_mm float,
    snowfall_cm float,
    wind_kph float,
    weather_code smallint,
    pulled_at timestamp_ntz,
    primary key (city, local_date, local_hour)
)
"""


class SnowflakeResource(ConfigurableResource):
    """Key-pair Snowflake connection (DAGSTER_SVC / TRANSIT_PIPELINE by default).

    Session TIMEZONE is pinned to UTC: the account default is
    America/Los_Angeles, which silently skews NTZ<->TZ arithmetic (quirk 1 in
    docs/operations.md). The user param is also UTC; this is belt and braces.
    """

    warehouse: str = "TRANSFORM_XS"
    database: str = "TRANSIT"

    @contextmanager
    def connection(self, schema: str = "SILVER"):
        import snowflake.connector

        env = os.environ
        conn = snowflake.connector.connect(
            account=require_env(env, "SNOWFLAKE_ACCOUNT"),
            user=require_env(env, "SNOWFLAKE_USER"),
            private_key_file=require_env(env, "SNOWFLAKE_PRIVATE_KEY_PATH"),
            role=env.get("SNOWFLAKE_ROLE", "TRANSIT_PIPELINE"),
            warehouse=self.warehouse,
            database=self.database,
            schema=schema,
            session_parameters={"TIMEZONE": "UTC"},
        )
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, statements: Sequence[str], schema: str = "SILVER") -> None:
        with self.connection(schema=schema) as conn:
            cur = conn.cursor()
            for stmt in statements:
                cur.execute(stmt)

    def fetch_one(self, sql: str, schema: str = "SILVER") -> tuple:
        with self.connection(schema=schema) as conn:
            return conn.cursor().execute(sql).fetchone()

    def merge_weather(self, rows: Sequence[tuple]) -> int:
        """Upsert weather rows on (city, local_date, local_hour) via a session
        temp table (multi-row MERGE without SQL-splicing values). Creates the
        target on first run."""
        if not rows:
            return 0
        cols = ", ".join(WEATHER_COLUMNS)
        placeholders = ", ".join(["%s"] * len(WEATHER_COLUMNS))
        updates = ", ".join(f"{c} = s.{c}" for c in WEATHER_COLUMNS[3:])
        inserts = ", ".join(f"s.{c}" for c in WEATHER_COLUMNS)
        with self.connection(schema="SILVER") as conn:
            cur = conn.cursor()
            cur.execute(WEATHER_DDL)
            cur.execute(f"create temporary table weather_stage like {WEATHER_TABLE}")
            cur.executemany(
                f"insert into weather_stage ({cols}) values ({placeholders})", list(rows)
            )
            cur.execute(
                f"merge into {WEATHER_TABLE} t using weather_stage s "
                "on t.city = s.city and t.local_date = s.local_date "
                "and t.local_hour = s.local_hour "
                f"when matched then update set {updates} "
                f"when not matched then insert ({cols}) values ({inserts})"
            )
            return int(cur.rowcount or 0)
