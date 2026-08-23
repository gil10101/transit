"""Dagster definitions for Transit Pulse (P5).

Wires: the 2h warehouse chain (EMR drain -> Snowflake Iceberg refresh -> dbt),
weekly static GTFS, hourly weather merges, the hourly raw-feed freshness
tripwire, and warehouse asset checks. Cloud resources read env at materialize
time, so this module imports (and the UI loads) with an empty environment.
"""

from __future__ import annotations

import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets_pipeline import (
    emr_drain,
    pipeline_schedule,
    snowflake_iceberg_refresh,
    transit_dbt_assets,
    warehouse_chain_job,
)
from .assets_static import gtfs_static_nyc, static_job, static_schedule
from .assets_weather import (
    weather_backfill_2yr,
    weather_hourly,
    weather_job,
    weather_schedule,
)
from .checks import (
    freshness_job,
    freshness_schedule,
    gold_rows_growing,
    raw_feed_freshness,
    silver_predictions_fresh,
)
from .project import dbt_project_dir
from .resources import EmrResource, SnowflakeResource

defs = Definitions(
    assets=[
        emr_drain,
        snowflake_iceberg_refresh,
        transit_dbt_assets,
        gtfs_static_nyc,
        weather_hourly,
        weather_backfill_2yr,  # manual trigger only: no job/schedule
        raw_feed_freshness,
    ],
    asset_checks=[gold_rows_growing, silver_predictions_fresh],
    jobs=[warehouse_chain_job, static_job, weather_job, freshness_job],
    schedules=[pipeline_schedule, static_schedule, weather_schedule, freshness_schedule],
    resources={
        "emr": EmrResource(),
        "snowflake": SnowflakeResource(),
        "dbt": DbtCliResource(
            project_dir=os.fspath(dbt_project_dir()),
            profiles_dir=os.fspath(dbt_project_dir()),
            target=os.environ.get("DBT_TARGET", "prod"),
        ),
    },
)
