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
    city_static_sources,
    latest_metadata_path,
    require_env,
    silver_tables,
)
from .resources import EmrResource, SnowflakeResource


def stage_static_zip(city: str) -> str | None:
    """Download every static GTFS source for <city> and archive them to
    raw/static/<city>/<version_id>/<source>/gtfs.zip; returns the version_id the
    EMR parse job reads back, or None when the city has no static feed.

    Runs on the box: EMR base Python has no requests, and keeping agency egress
    here also keeps download retries off the Spark bill. Zips stream to a temp
    file (the Swiss national zip is ~235 MB — never hold that in the daemon's
    heap) and all of a city's sources share ONE version_id so the staging
    models' max(gtfs_version_id) per city cannot hide one source's schedule.
    """
    import tempfile

    import boto3
    import requests

    from ingestion.city_static import resolve_auth

    sources = city_static_sources(city)
    if not sources:
        return None
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-2"))
    bucket = require_env(os.environ, "RAW_BUCKET")
    digest = hashlib.sha256()
    staged = []
    try:
        for source in sources:
            headers, params = resolve_auth(source.auth)
            with requests.get(
                source.url,
                headers=headers or None,
                params=params or None,
                timeout=300,
                stream=True,
            ) as resp:
                resp.raise_for_status()
                tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
                with tmp:
                    for chunk in resp.iter_content(chunk_size=1 << 20):
                        digest.update(chunk)
                        tmp.write(chunk)
            staged.append((source.name, tmp.name))
        version_id = f"{city}-{datetime.now(UTC):%Y%m%d}-{digest.hexdigest()[:8]}"
        for name, path in staged:
            s3.upload_file(path, bucket, f"static/{city}/{version_id}/{name}/gtfs.zip")
    finally:
        for _, path in staged:
            os.unlink(path)
    return version_id


ODPT_DUMP_TYPES = ("Station", "Railway", "TrainTimetable", "Calendar")
ODPT_DUMP_URL = "https://api.odpt.org/api/v4/odpt:{rdf_type}.json"


def stage_odpt_dumps() -> str:
    """Download the four ODPT static dumps and archive them to
    raw/static/tokyo/<version_id>/odpt/<Type>.json; returns the version_id the
    EMR parse reads back.

    Dump API only (retrieval API caps static types at 1000 entries); it
    301-redirects to the dump file and requests follows by default. The
    TrainTimetable dump is ~63 MB (every center operator) — streamed to a temp
    file like the zips; the parse filters to TokyoMetro/Toei. Runs on the box:
    EMR base Python has no requests, and the consumerKey stays out of EMR env.
    """
    import tempfile

    import boto3
    import requests

    key = require_env(os.environ, "ODPT_CONSUMER_KEY")
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-2"))
    bucket = require_env(os.environ, "RAW_BUCKET")
    digest = hashlib.sha256()
    staged = []
    try:
        for rdf_type in ODPT_DUMP_TYPES:
            with requests.get(
                ODPT_DUMP_URL.format(rdf_type=rdf_type),
                params={"acl:consumerKey": key},
                timeout=300,
                stream=True,
            ) as resp:
                resp.raise_for_status()
                tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
                with tmp:
                    for chunk in resp.iter_content(chunk_size=1 << 20):
                        digest.update(chunk)
                        tmp.write(chunk)
            staged.append((rdf_type, tmp.name))
        version_id = f"tokyo-odpt-{datetime.now(UTC):%Y%m%d}-{digest.hexdigest()[:8]}"
        for rdf_type, path in staged:
            s3.upload_file(path, bucket, f"static/tokyo/{version_id}/odpt/{rdf_type}.json")
    finally:
        for _, path in staged:
            os.unlink(path)
    return version_id


@asset(group_name="static")
def odpt_static(
    emr: EmrResource, snowflake: SnowflakeResource, dbt: DbtCliResource
) -> MaterializeResult:
    """Weekly refresh of the tokyo ODPT JSON statics (docs/01 §C.4): stage the
    four dumps, EMR runs spark_jobs/odpt_static_parse.py, re-pin the odpt_*
    Iceberg tables, dbt build of stg_odpt__*+ (a no-op until those models land).
    Separate from gtfs_static because the source is the dump API, not zips —
    tokyo's GTFS zips still refresh through gtfs_static like every other city.
    """
    version_id = stage_odpt_dumps()
    run_id = emr.run_odpt_static(version_id)

    import boto3

    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-2"))
    bucket = require_env(os.environ, "LAKE_BUCKET")
    pins: list[tuple[str, str]] = []
    refreshed: list[str] = []
    for table in silver_tables():
        if not table.startswith("odpt_") or table == "odpt_trains":
            continue  # odpt_trains is the streaming table; the 2h chain re-pins it
        meta = latest_metadata_path(s3, bucket, table)
        if meta is None:
            continue
        pins.append((table, meta))
        refreshed.append(table)
    rebound: list[str] = []
    if pins:
        outcomes = snowflake.refresh_iceberg(pins)
        rebound = [t for t, how in outcomes.items() if how != "refreshed"]
    dbt.cli(["build", "--select", "stg_odpt__*+"]).wait()

    return MaterializeResult(
        metadata={
            "emr_job_run_id": run_id,
            "odpt_version_id": version_id,
            "refreshed": ", ".join(refreshed) or "(none)",
            "rebound_new_uuid": ", ".join(rebound) or "(none)",
        }
    )


@asset(group_name="static")
def gtfs_static(
    emr: EmrResource, snowflake: SnowflakeResource, dbt: DbtCliResource
) -> MaterializeResult:
    """Refresh every live city's static schedule end to end.

    1. Download + stage each LIVE_CITIES zip (stage_static_zip), then EMR runs
       spark_jobs/gtfs_static_parse.py <city> <version_id> per city,
       sequentially — one job holds most of the 8 vCPU app and EMR Serverless
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
    skipped: list[str] = []
    for city in LIVE_CITIES:
        try:
            version_id = stage_static_zip(city)
            if version_id is None:
                # RT-only for now (an upstream static URL is never guessed —
                # CLAUDE.md). Skipping keeps the other cities' weekly refresh
                # green; docs/01 §D tracks the open verification.
                skipped.append(city)
                continue
            run_ids[city] = emr.run_static(city, version_id)
        except Exception as e:  # noqa: BLE001 — keep the other cities refreshing
            failed[city] = repr(e)

    refreshed: list[str] = []
    rebound: list[str] = []
    if run_ids:  # at least one parse landed: re-pin + rebuild for those cities
        import boto3

        s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-2"))
        bucket = require_env(os.environ, "LAKE_BUCKET")
        pins: list[tuple[str, str]] = []
        for table in silver_tables():
            if not table.startswith("gtfs_static_"):
                continue
            meta = latest_metadata_path(s3, bucket, table)
            if meta is None:
                continue
            pins.append((table, meta))
            refreshed.append(table)
        if pins:
            # refresh_iceberg, not a plain statement list: a parse that hit
            # schema drift recreates the Iceberg table under a new UUID, and a
            # bare `alter ... refresh` then fails and leaves Snowflake pointing
            # at deleted parquet files
            outcomes = snowflake.refresh_iceberg(pins)
            rebound = [t for t, how in outcomes.items() if how != "refreshed"]
        dbt.cli(["build", "--select", "stg_gtfs__*+"]).wait()

    metadata = {
        "emr_job_run_ids": MetadataValue.json(run_ids),
        "refreshed": ", ".join(refreshed) or "(none)",
        "skipped_no_static": ", ".join(skipped) or "(none)",
        "rebound_new_uuid": ", ".join(rebound) or "(none)",
    }
    if failed:
        raise Failure(
            description=f"static GTFS parse failed for: {', '.join(sorted(failed))}",
            metadata={**metadata, "errors": MetadataValue.json(failed)},
        )
    return MaterializeResult(metadata=metadata)


static_job = define_asset_job(
    "gtfs_static_refresh",
    selection=AssetSelection.assets(gtfs_static, odpt_static),
    description=(
        "weekly statics: GTFS zips per city + tokyo ODPT dumps -> EMR parse -> "
        "iceberg refresh -> dbt staging"
    ),
)

static_schedule = ScheduleDefinition(
    default_status=DefaultScheduleStatus.RUNNING,
    job=static_job,
    # [rev 2026-09-11] 09:00Z -> 12:30Z Sunday, AFTER the 11:05Z chain. A new static
    # becomes the delay path's schedule, and a day it does not cover is frozen at its
    # last computation (int_stop_events_finalized rotation freeze). At 09:00Z Saturday's
    # late evening (drained after the 23:05Z chain) was not computed yet and would
    # freeze incomplete; after 11:05Z every city's Saturday is closed and computed on
    # its own static (SF closes last, 10:00Z).
    cron_schedule="30 12 * * 0",  # Sun 12:30 UTC — off the :05 drain grid so static
    # parses never queue behind a drain on the 8 vCPU app
    execution_timezone="UTC",
)
