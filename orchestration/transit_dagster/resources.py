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
    # statics wait for a drain gap first, then parse; both fit in 40 but not 25
    static_timeout_minutes: int = 40

    def run_drain(self) -> str:
        return self._run(drain_job_request(os.environ), adopt_in_flight=True)

    def run_static(self, city: str, version_id: str) -> str:
        return self._run(
            static_job_request(os.environ, city=city, version_id=version_id),
            timeout_minutes=self.static_timeout_minutes,
        )

    def _in_flight_run(self, client, request: dict) -> str | None:
        """Id of an already-active run that does this request's work, if any.

        Matching is by NAME PREFIX, not equality: the scheduled Lambda drain
        ("transit-drain") and this chain's drain ("transit-drain-dagster") write
        the SAME Spark checkpoint, and Structured Streaming permits one writer
        per checkpoint location. Exact-name matching let the pair run together
        on 2026-08-24 and Spark killed the loser with CONCURRENT_STREAM_LOG_UPDATE
        after it had done 58 minutes of work. Adopting also avoids the capacity
        pile-up, since one job holds half the app.
        """
        prefix = request["name"].split("-dagster")[0]
        runs = client.list_job_runs(
            applicationId=request["applicationId"],
            states=["SUBMITTED", "PENDING", "SCHEDULED", "RUNNING"],
        )["jobRuns"]
        for run in runs:
            if str(run.get("name", "")).startswith(prefix):
                return run["id"]
        return None

    def _any_active_run(self, client, request: dict) -> str | None:
        runs = client.list_job_runs(
            applicationId=request["applicationId"],
            states=["SUBMITTED", "PENDING", "SCHEDULED", "RUNNING"],
        )["jobRuns"]
        return runs[0]["id"] if runs else None

    def _resubmit_when_idle(self, client, request: dict, deadline: float, log) -> str:
        """Wait for the app to go idle, then submit with a FRESH clientToken.

        Two hard-won facts drive this shape:
        - reusing the original clientToken makes EMR return the same
          already-FAILED run id forever (idempotency), so the old fixed-90s
          resubmit could never succeed;
        - back-to-back 15-min drains can hold the 4 vCPU app nearly
          continuously, so blind resubmission mostly lands on a busy app.
        Polling for the idle gap (~20s) catches the minutes between drains.
        """
        import uuid

        while time.monotonic() < deadline:
            holder = self._any_active_run(client, request)
            if holder is None:
                fresh = {**request, "clientToken": str(uuid.uuid4())}
                try:
                    run_id = client.start_job_run(**fresh)["jobRunId"]
                except Exception as e:
                    if "maximumCapacity" not in str(e):
                        raise
                    continue  # lost the gap race; keep waiting
                log.info(f"EMR {request['name']} resubmitted in idle gap: run {run_id}")
                return run_id
            time.sleep(20)
        raise TimeoutError(f"EMR {request['name']}: no capacity gap before the deadline")

    def _run(
        self, request: dict, adopt_in_flight: bool = False, timeout_minutes: int | None = None
    ) -> str:
        import boto3

        log = get_dagster_logger()
        client = boto3.client(
            "emr-serverless", region_name=os.environ.get("AWS_REGION", "us-east-2")
        )
        deadline = time.monotonic() + (timeout_minutes or self.timeout_minutes) * 60
        run_id = self._in_flight_run(client, request) if adopt_in_flight else None
        if run_id is not None:
            log.info(f"EMR {request['name']}: adopting in-flight run {run_id}")
        else:
            try:
                run_id = client.start_job_run(**request)["jobRunId"]
            except Exception as e:
                if "maximumCapacity" not in str(e):
                    raise
                # lost the race: a drain started between our check and submit
                if adopt_in_flight:
                    run_id = self._in_flight_run(client, request)
                    if run_id is None:
                        raise
                    log.info(f"EMR {request['name']}: capacity race, adopting run {run_id}")
                else:
                    run_id = self._resubmit_when_idle(client, request, deadline, log)
            else:
                log.info(f"EMR {request['name']} submitted: run {run_id}")
        while True:
            job = client.get_job_run(applicationId=request["applicationId"], jobRunId=run_id)[
                "jobRun"
            ]
            state = job["state"]
            if state == "SUCCESS":
                log.info(f"EMR run {run_id} SUCCESS")
                return run_id
            if state in EMR_TERMINAL_STATES:  # FAILED / CANCELLED
                details = job.get("stateDetails", "")
                # a capacity rejection surfaces as an instantly-FAILED run (one
                # running job holds the whole 4 vCPU app)
                if "maximumCapacity" in details:
                    # same-name run holds the capacity -> it does our work: adopt
                    if adopt_in_flight:
                        other = self._in_flight_run(client, request)
                        if other is not None and other != run_id:
                            log.info(f"EMR run {run_id} lost capacity race; adopting {other}")
                            run_id = other
                            continue
                    # different job (e.g. static parse vs a scheduled drain):
                    # wait for an idle gap and resubmit, bounded by the deadline
                    if time.monotonic() < deadline:
                        log.info(f"EMR run {run_id} hit capacity; waiting for an idle gap")
                        run_id = self._resubmit_when_idle(client, request, deadline, log)
                        continue
                raise RuntimeError(f"EMR run {run_id} ended {state}: {details}")
            if time.monotonic() > deadline:
                # a hung run holds the whole 4 vCPU app and queues every later
                # drain — free the capacity before giving up
                try:
                    client.cancel_job_run(applicationId=request["applicationId"], jobRunId=run_id)
                    log.warning(f"EMR run {run_id} cancelled after timeout")
                except Exception as e:  # noqa: BLE001 — cancel is best-effort
                    log.warning(f"cancel of EMR run {run_id} failed: {e!r}")
                raise TimeoutError(f"EMR run {run_id} still {state} after the deadline")
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
