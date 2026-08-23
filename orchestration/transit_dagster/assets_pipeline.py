"""The 2-hourly warehouse chain: EMR drain -> Snowflake Iceberg refresh -> dbt.

City x date partitioning of the dbt assets (docs/03 §partitioning) is
deliberately deferred: dagster-dbt partitioned assets need per-partition dbt
vars and backfill policies that buy nothing while NYC is the only live city.
Unpartitioned assets until the P3 fan-out; docs/03 remains the goal state.
"""

import os

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    define_asset_job,
    multi_asset,
)
from dagster_dbt import DbtCliResource, dbt_assets

from .lib import (
    iceberg_refresh_statements,
    latest_metadata_path,
    require_env,
    silver_tables,
)
from .project import dbt_manifest_path
from .resources import EmrResource, SnowflakeResource

SILVER_TABLES = silver_tables()


@asset(group_name="pipeline")
def emr_drain(emr: EmrResource) -> MaterializeResult:
    """One availableNow drain (Kafka -> bronze -> silver Iceberg), identical to
    what the 15-min EventBridge Lambda submits. EventBridge stays; this run
    guarantees silver is freshly committed when the chain refreshes Snowflake
    (the app's 4 vCPU cap queues, not corrupts, any overlap with a scheduled
    drain)."""
    run_id = emr.run_drain()
    return MaterializeResult(metadata={"emr_job_run_id": run_id})


# One output per silver table, keyed exactly like the dbt 'silver' sources
# (["silver", <table>]), so the default dagster-dbt translator chains the dbt
# DAG off this asset with no key remapping.
@multi_asset(
    name="snowflake_iceberg_refresh",
    specs=[
        AssetSpec(
            AssetKey(["silver", table]), deps=[emr_drain], skippable=True, group_name="pipeline"
        )
        for table in SILVER_TABLES
    ],
)
def snowflake_iceberg_refresh(snowflake: SnowflakeResource):
    """Point every TRANSIT.SILVER external Iceberg table at its latest metadata
    json (port of scripts/snowflake_register_iceberg.py onto the DAGSTER_SVC /
    TRANSIT_PIPELINE identity — the role owns the SILVER tables, which ALTER
    ICEBERG TABLE ... REFRESH requires). Tables without a version-hint yet are
    skipped (skippable specs)."""
    import boto3

    s3 = boto3.client("s3")
    bucket = require_env(os.environ, "LAKE_BUCKET")
    statements: list[str] = []
    refreshed: list[tuple[str, str]] = []
    for table in SILVER_TABLES:
        meta = latest_metadata_path(s3, bucket, table)
        if meta is None:
            continue
        statements += iceberg_refresh_statements(table, meta)
        refreshed.append((table, meta))
    if statements:
        snowflake.execute(statements)
    for table, meta in refreshed:
        yield MaterializeResult(
            asset_key=AssetKey(["silver", table]), metadata={"metadata_file_path": meta}
        )


@dbt_assets(manifest=dbt_manifest_path())
def transit_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


warehouse_chain_job = define_asset_job(
    "warehouse_chain",
    selection=AssetSelection.assets(emr_drain, snowflake_iceberg_refresh, transit_dbt_assets),
    description="drain -> iceberg refresh -> dbt build (+ warehouse asset checks)",
)

pipeline_schedule = ScheduleDefinition(
    job=warehouse_chain_job,
    cron_schedule="5 */2 * * *",  # :05 keeps clear of the :00 EventBridge drain submit
    execution_timezone="UTC",
)
