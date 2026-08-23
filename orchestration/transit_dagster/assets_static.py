"""Weekly static GTFS refresh: EMR parse -> Iceberg refresh -> dbt downstream."""

import os

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from dagster_dbt import DbtCliResource

from .lib import iceberg_refresh_statements, latest_metadata_path, require_env, silver_tables
from .resources import EmrResource, SnowflakeResource


@asset(group_name="static")
def gtfs_static_nyc(
    emr: EmrResource, snowflake: SnowflakeResource, dbt: DbtCliResource
) -> MaterializeResult:
    """Refresh the NYC static schedule end to end.

    1. EMR job runs spark_jobs/gtfs_static_parse.py (STATIC_ENTRY_POINT). The
       job reads static_gtfs from ingestion/config/cities/nyc.yaml inside the
       shipped code zip — supplemented zip since P5; never hardcoded here.
    2. Re-point the gtfs_static_* Iceberg tables in Snowflake.
    3. dbt build of the stg_gtfs__* staging models and everything downstream
       (invoked directly, so these dbt materializations are not recorded as
       dagster asset events — the 2h chain re-records them on its next run).
    """
    run_id = emr.run_static("nyc")

    import boto3

    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-2"))
    bucket = require_env(os.environ, "LAKE_BUCKET")
    statements: list[str] = []
    refreshed: list[str] = []
    for table in silver_tables():
        if not table.startswith("gtfs_static_"):
            continue
        meta = latest_metadata_path(s3, bucket, table)
        if meta is None:
            continue
        statements += iceberg_refresh_statements(table, meta)
        refreshed.append(table)
    if statements:
        snowflake.execute(statements)

    dbt.cli(["build", "--select", "stg_gtfs__*+"]).wait()
    return MaterializeResult(
        metadata={"emr_job_run_id": run_id, "refreshed": ", ".join(refreshed) or "(none)"}
    )


static_job = define_asset_job(
    "gtfs_static_refresh",
    selection=AssetSelection.assets(gtfs_static_nyc),
    description="weekly static GTFS: EMR parse -> iceberg refresh -> dbt stg_gtfs__*+",
)

static_schedule = ScheduleDefinition(
    default_status=DefaultScheduleStatus.RUNNING,
    job=static_job,
    cron_schedule="0 9 * * 0",  # Sun 09:00 UTC — off the 2h chain grid (:05) so the
    # static parse never queues behind a chain drain on the 4 vCPU app
    execution_timezone="UTC",
)
