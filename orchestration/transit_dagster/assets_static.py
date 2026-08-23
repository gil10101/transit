"""Weekly static GTFS refresh: download + stage per live city -> EMR parse ->
Iceberg refresh -> dbt. The download runs here on the box (requests available);
EMR only parses the staged zip (its base Python has boto3 and nothing else)."""

import hashlib
import os
from datetime import UTC, datetime

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
    static_gtfs_url,
)
from .resources import EmrResource, SnowflakeResource


def stage_static_zip(city: str) -> str:
    """Download <city>'s static GTFS and archive it to
    raw/static/<city>/<version_id>/gtfs.zip; returns the version_id the EMR
    parse job reads back. Runs on the box: EMR base Python has no requests,
    and keeping agency egress here also keeps retries off the Spark bill."""
    import boto3
    import requests

    url = static_gtfs_url(city)
    resp = requests.get(url, timeout=180)
    resp.raise_for_status()
    blob = resp.content
    stamp = datetime.now(UTC).strftime("%Y%m%d")
    version_id = f"{city}-{stamp}-{hashlib.sha256(blob).hexdigest()[:8]}"
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-2"))
    s3.put_object(
        Bucket=require_env(os.environ, "RAW_BUCKET"),
        Key=f"static/{city}/{version_id}/gtfs.zip",
        Body=blob,
    )
    return version_id


@asset(group_name="static")
def gtfs_static(
    emr: EmrResource, snowflake: SnowflakeResource, dbt: DbtCliResource
) -> MaterializeResult:
    """Refresh every live city's static schedule end to end.

    1. Download + stage each LIVE_CITIES zip (stage_static_zip), then EMR runs
       spark_jobs/gtfs_static_parse.py <city> <version_id> per city,
       sequentially — one job holds the whole 4 vCPU app and EMR Serverless
       rejects (not queues) over-capacity submits (quirk 3). static_gtfs URLs
       come from ingestion/config/cities/<city>.yaml — never hardcoded here.
       A failing city is recorded and the remaining cities still run; the
       asset fails at the end so one broken upstream zip cannot stall the
       other cities for a week.
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
            version_id = stage_static_zip(city)
            run_ids[city] = emr.run_static(city, version_id)
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
