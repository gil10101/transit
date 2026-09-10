"""Health tripwires: raw-feed freshness (hourly, boto3-only) and post-dbt
warehouse asset checks that ride along in the 2h chain."""

import os
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    Failure,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

from .lib import city_feed_endpoints, evaluate_feed_freshness, kafka_listener_down, require_env
from .resources import SnowflakeResource


@asset(group_name="checks")
def raw_feed_freshness() -> MaterializeResult:
    """The killed-feed tripwire (DoD: trips within the hour): the newest raw
    object under every POLLED city's endpoint prefixes must be younger than
    40 min. Endpoints are the feed_groups keys in ingestion/config/cities/*.yaml
    (nyc 8, boston 3, toronto 3, helsinki 2, dc 6, sf 3, zurich 2 = 27 —
    config-driven, so a new city yaml joins the tripwire without touching this
    code). The list is POLLED_CITIES, not LIVE_CITIES: a city can be live for
    static/weather before its poller ships, and asserting on prefixes nothing
    writes to fails forever. boto3 listing only — never wakes the warehouse."""
    import boto3

    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-2"))
    bucket = require_env(os.environ, "RAW_BUCKET")
    now = datetime.now(UTC)
    age_min: dict[str, float | None] = {}
    stale: list[str] = []
    for city, endpoints in city_feed_endpoints().items():
        report = evaluate_feed_freshness(s3, bucket, now=now, city=city, endpoints=endpoints)
        for ep, r in report.items():
            age_min[f"{city}/{ep}"] = (
                round(r["age_sec"] / 60, 1) if r["age_sec"] is not None else None
            )
            if r["stale"]:
                stale.append(f"{city}/{ep}")
    if stale:
        raise Failure(
            description=f"raw feeds stale (>40 min or no objects): {', '.join(sorted(stale))}",
            metadata={"age_min": MetadataValue.json(age_min)},
        )
    # Broker reachability rides along: the pollers dual-write raw S3 + Kafka, so
    # a dead broker leaves every raw prefix fresh and this tripwire blind while
    # silver quietly stops — exactly the 2026-08-28 disk-full outage, which no
    # check saw until the 2h chain failed. A TCP connect to the bootstrap
    # listener pages within one 15-min tick instead.
    broker = kafka_listener_down(os.environ.get("KAFKA_BOOTSTRAP", ""))
    if broker:
        raise Failure(description=f"kafka bootstrap unreachable: {broker}")
    return MaterializeResult(metadata={"age_min": MetadataValue.json(age_min)})


freshness_job = define_asset_job(
    "raw_feed_freshness_job", selection=AssetSelection.assets(raw_feed_freshness)
)

freshness_schedule = ScheduleDefinition(
    default_status=DefaultScheduleStatus.RUNNING,
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
    """Rows finalized since the previous chain run (5h window = 4h cadence plus
    slack) must be > 0 during NYC service hours. last_seen_utc = last prediction
    snapshot backing the finalized event; sysdate() is UTC (quirk 1).

    [rev 2026-09-09] Window widened 3h -> 5h with the cadence change 2h -> 4h.
    The happy path never needed it — this runs straight after a drain, so the
    freshest finalized events are minutes old — but a single skipped chain now
    leaves an 8h gap instead of 4h, and a check that fires on the recovery run
    rather than the failure is worse than no check."""
    (rows,) = snowflake.fetch_one(
        "select count(*) from TRANSIT.GOLD.FCT_STOP_EVENTS "
        "where last_seen_utc >= dateadd(hour, -5, sysdate())",
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
    """max(fetched_at) across silver stop_time_predictions must be < 5h old —
    catches a stalled drain/refresh even when the poller is healthy.

    [rev 2026-09-09] 3h -> 5h alongside the 2h -> 4h cadence change, for the same
    reason as gold_rows_growing: this runs after the drain so the happy path is
    always minutes old, but the threshold has to clear one missed cycle or it
    reports the recovery instead of the outage."""
    (age_sec,) = snowflake.fetch_one(
        "select datediff('second', max(fetched_at), sysdate()) "
        "from TRANSIT.SILVER.STOP_TIME_PREDICTIONS"
    )
    age_sec = int(age_sec) if age_sec is not None else None
    return AssetCheckResult(
        passed=bool(age_sec is not None and age_sec < 5 * 3600),
        metadata={
            "age_min": round(age_sec / 60, 1) if age_sec is not None else "no rows in silver"
        },
    )
