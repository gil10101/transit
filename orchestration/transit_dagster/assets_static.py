"""Weekly static GTFS refresh: EMR parse per live city -> Iceberg refresh -> dbt."""

import os

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Failure,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from dagster_dbt import DbtCliResource

from .lib import (
    LIVE_CITIES,
    iceberg_refresh_statements,
    latest_metadata_path,
    require_env,
    silver_tables,
)
from .resources import EmrResource, SnowflakeResource


@asset(group_name="static")
def gtfs_static(
    emr: EmrResource, snowflake: SnowflakeResource, dbt: DbtCliResource
) -> MaterializeResult:
    """Refresh every live city's static schedule end to end.

    1. EMR runs spark_jobs/gtfs_static_parse.py once per LIVE_CITIES entry,
       sequentially — one job holds the whole 4 vCPU app and EMR Serverless
       rejects (not queues) over-capacity submits (quirk 3). Each job reads
       static_gtfs from ingestion/config/cities/<city>.yaml inside the shipped
       code zip — never hardcoded here. A failing city is recorded and the
       remaining cities still run; the asset fails at the end so one broken
       upstream zip cannot stall the other cities for a week.
    2. Re-point the gtfs_static_* Iceberg tables in Snowflake once, after the
       parses (they all append city partitions to the same tables).
    3. dbt build of the stg_gtfs__* staging models and everything downstream
       (invoked directly, so these dbt materializations are not recorded as
       dagster asset events — the 2h chain re-records them on its next run).
    """
    run_ids: dict[str, str] = {}
    failed: dict[str, str] = {}
    for city in LIVE_CITIES:
        try:
            run_ids[city] = emr.run_static(city)
        except Exception as e:  # noqa: BLE001 — keep the other cities refreshing
            failed[city] = repr(e)

    refreshed: list[str] = []
    if run_ids:  # at least one parse landed: re-pin + rebuild for those cities
        import boto3

        s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-2"))
        bucket = require_env(os.environ, "LAKE_BUCKET")
        statements: list[str] = []
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

    metadata = {
        "emr_job_run_ids": MetadataValue.json(run_ids),
        "refreshed": ", ".join(refreshed) or "(none)",
    }
    if failed:
        raise Failure(
            description=f"static GTFS parse failed for: {', '.join(sorted(failed))}",
            metadata={**metadata, "errors": MetadataValue.json(failed)},
        )
    return MaterializeResult(metadata=metadata)


static_job = define_asset_job(
    "gtfs_static_refresh",
    selection=AssetSelection.assets(gtfs_static),
    description="weekly static GTFS: EMR parse per city -> iceberg refresh -> dbt stg_gtfs__*+",
)

static_schedule = ScheduleDefinition(
    default_status=DefaultScheduleStatus.RUNNING,
    job=static_job,
    cron_schedule="0 9 * * 0",  # Sun 09:00 UTC — off the 2h chain grid (:05) so the
    # static parses never queue behind a chain drain on the 4 vCPU app
    execution_timezone="UTC",
)
