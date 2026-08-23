"""Health tripwires: raw-feed freshness (hourly, boto3-only) and post-dbt
warehouse asset checks that ride along in the 2h chain."""

import os
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetSelection,
    Failure,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

from .lib import evaluate_feed_freshness, require_env
from .resources import SnowflakeResource


@asset(group_name="checks")
def raw_feed_freshness() -> MaterializeResult:
    """The killed-feed tripwire (DoD: trips within the hour): the newest raw
    object under each of the 8 NYC endpoint prefixes must be younger than
    40 min. boto3 listing only — never wakes the warehouse."""
    import boto3

    s3 = boto3.client("s3")
    bucket = require_env(os.environ, "RAW_BUCKET")
    report = evaluate_feed_freshness(s3, bucket, now=datetime.now(UTC))
    age_min = {
        ep: (round(r["age_sec"] / 60, 1) if r["age_sec"] is not None else None)
        for ep, r in report.items()
    }
    stale = sorted(ep for ep, r in report.items() if r["stale"])
    if stale:
        raise Failure(
            description=f"raw feeds stale (>40 min or no objects): {', '.join(stale)}",
            metadata={"age_min": MetadataValue.json(age_min)},
        )
    return MaterializeResult(metadata={"age_min": MetadataValue.json(age_min)})


freshness_job = define_asset_job(
    "raw_feed_freshness_job", selection=AssetSelection.assets(raw_feed_freshness)
)

freshness_schedule = ScheduleDefinition(
    job=freshness_job,
    # every 15 min + 40-min threshold => worst-case detection ~55 min after a
    # kill (meets the docs/04 "within an hour" DoD); boto3 LISTs only, no cost
    cron_schedule="10,25,40,55 * * * *",
    execution_timezone="UTC",
)


# NYC runs 24/7, but overnight volume is too thin to gate on: enforce growth
# only during local service hours (local time per canonical rules).
SERVICE_HOURS_LOCAL = range(6, 24)


@asset_check(asset=AssetKey("fct_stop_events"), name="gold_rows_growing")
def gold_rows_growing(snowflake: SnowflakeResource) -> AssetCheckResult:
    """Rows finalized since the previous chain run (3h window = 2h cadence plus
    slack) must be > 0 during NYC service hours. last_seen_utc = last prediction
    snapshot backing the finalized event; sysdate() is UTC (quirk 1)."""
    (rows,) = snowflake.fetch_one(
        "select count(*) from TRANSIT.GOLD.FCT_STOP_EVENTS "
        "where last_seen_utc >= dateadd(hour, -3, sysdate())",
        schema="GOLD",
    )
    rows = int(rows)  # connector may hand back Decimal; keep metadata serializable
    local_hour = datetime.now(ZoneInfo("America/New_York")).hour
    enforced = local_hour in SERVICE_HOURS_LOCAL
    return AssetCheckResult(
        passed=bool(rows > 0 or not enforced),
        metadata={"rows_finalized_last_3h": rows, "local_hour": local_hour, "enforced": enforced},
    )


@asset_check(asset=AssetKey("stg_gtfsrt__trip_updates"), name="silver_predictions_fresh")
def silver_predictions_fresh(snowflake: SnowflakeResource) -> AssetCheckResult:
    """max(fetched_at) across silver stop_time_predictions must be < 3h old —
    catches a stalled drain/refresh even when the poller is healthy."""
    (age_sec,) = snowflake.fetch_one(
        "select datediff('second', max(fetched_at), sysdate()) "
        "from TRANSIT.SILVER.STOP_TIME_PREDICTIONS"
    )
    age_sec = int(age_sec) if age_sec is not None else None
    return AssetCheckResult(
        passed=bool(age_sec is not None and age_sec < 3 * 3600),
        metadata={
            "age_min": round(age_sec / 60, 1) if age_sec is not None else "no rows in silver"
        },
    )
