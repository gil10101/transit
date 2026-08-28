"""The 2-hourly warehouse chain: EMR drain -> Snowflake Iceberg refresh -> dbt.

City x date partitioning of the dbt assets (docs/03 §partitioning) is
deliberately deferred: dagster-dbt partitioned assets need per-partition dbt
vars and backfill policies. P3 batch 1 (4 cities) still runs whole-warehouse
unpartitioned builds — volumes stay small enough; revisit at P3 batch 2/P6.
docs/03 remains the goal state.
"""

import os

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    Backoff,
    DefaultScheduleStatus,
    MaterializeResult,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
    multi_asset,
)
from dagster_dbt import DbtCliResource, dbt_assets

from .lib import (
    latest_metadata_path,
    require_env,
    silver_tables,
)
from .project import dbt_manifest_path
from .resources import EmrResource, SnowflakeResource

SILVER_TABLES = silver_tables()

# Infra steps retry; the dbt step never does. An EMR admission blip, a Spark
# eventlog collision, a Snowflake XX000 are transient and self-heal on resubmit
# (each emr retry is a NEW job run id, so per-run scratch dirs start clean; the
# EmrResource in-flight guard still prevents overlap). A dbt failure is a fact
# about the data and must stay loud — retrying it only delays the page.
INFRA_RETRY = RetryPolicy(max_retries=2, delay=120, backoff=Backoff.EXPONENTIAL)


@asset(group_name="pipeline", retry_policy=INFRA_RETRY)
def emr_drain(emr: EmrResource) -> MaterializeResult:
    """One availableNow drain (Kafka -> bronze -> silver Iceberg), identical to what the
    hourly EventBridge Lambda submits. The EventBridge schedule is DISABLED since
    2026-08-26 (cost cut) — this chain is the only scheduled drain; the run guarantees
    silver is freshly committed when the chain refreshes Snowflake.

    [rev 2026-08-25] This used to claim "the app's 4 vCPU cap queues, not corrupts, any
    overlap with a scheduled drain". Both halves were wrong. The app is provisioned at
    **8 vCPU / 32 GB** (spark module, deliberately — its comment opens "8, not 4"), and
    one drain uses 6, so a second job's driver fits and EMR admits it rather than queuing
    it. And an admitted overlap does not queue harmlessly: two writers on one Spark
    checkpoint killed a query with CONCURRENT_STREAM_LOG_UPDATE and destroyed 58 minutes
    of drained data on 2026-08-24. Overlap is prevented by the in-flight guard in
    EmrResource (and the drain Lambda), never by capacity."""
    run_id = emr.run_drain()
    return MaterializeResult(metadata={"emr_job_run_id": run_id})


# One output per silver table, keyed exactly like the dbt 'silver' sources
# (["silver", <table>]), so the default dagster-dbt translator chains the dbt
# DAG off this asset with no key remapping.
@multi_asset(
    name="snowflake_iceberg_refresh",
    retry_policy=INFRA_RETRY,
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

    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-2"))
    bucket = require_env(os.environ, "LAKE_BUCKET")
    refreshed: list[tuple[str, str]] = []
    for table in SILVER_TABLES:
        meta = latest_metadata_path(s3, bucket, table)
        if meta is None:
            continue
        refreshed.append((table, meta))

    # [rev 2026-08-25] Was `snowflake.execute(<all 10 tables' statements>)`, a bare loop
    # with no error handling, and every yield below sits AFTER it. When one table failed
    # the whole call aborted: the remaining tables never refreshed, zero materializations
    # were recorded, and dbt was skipped as a downstream of a failed step — so gold went
    # quietly stale while the job just looked broken. That is not hypothetical; Snowflake
    # query history shows it firing twice on 2026-08-24 (91391 UUID mismatch on
    # GTFS_STATIC_ROUTES at 18:08 and 18:32, no statement after it in either session).
    #
    # refresh_iceberg already handles exactly this — it rebinds a table whose Iceberg
    # UUID changed under a reparse, and isolates per-table failures. It was written for
    # this bug in commit 0e65311 and wired into assets_static only; this second call site
    # was missed. Same fix, one call.
    results = snowflake.refresh_iceberg(refreshed) if refreshed else {}
    for table, meta in refreshed:
        yield MaterializeResult(
            asset_key=AssetKey(["silver", table]),
            metadata={
                "metadata_file_path": meta,
                "refresh": results.get(table, "skipped"),
            },
        )


@dbt_assets(manifest=dbt_manifest_path())
def transit_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


warehouse_chain_job = define_asset_job(
    "warehouse_chain",
    selection=AssetSelection.assets(emr_drain, snowflake_iceberg_refresh, transit_dbt_assets),
    description="drain -> iceberg refresh -> dbt build (+ warehouse asset checks)",
    # This tag exists solely for the QueuedRunCoordinator's tag_concurrency_limits
    # in dagster.yaml. It must be an explicit job tag: Dagster stores the job name
    # as a run COLUMN, never as a tag, so a `dagster/job_name` limit key silently
    # matches nothing (config validates, limits nothing — found in review after
    # the 2026-08-28 overlap wedge shipped with exactly that no-op key).
    tags={"transit/serialize": "warehouse_chain"},
)

pipeline_schedule = ScheduleDefinition(
    default_status=DefaultScheduleStatus.RUNNING,
    job=warehouse_chain_job,
    cron_schedule="5 */2 * * *",  # :05 keeps clear of the :00 EventBridge drain submit
    execution_timezone="UTC",
)
