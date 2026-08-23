"""Hourly weather pulls (open-meteo, no key) merged into TRANSIT.SILVER.WEATHER_HOURLY."""

import time
from datetime import UTC, datetime, timedelta

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

# LIVE_CITIES + CITY_WEATHER live in lib.py since P3 (single source across the
# weather/static/freshness assets, unit-testable without dagster installed).
from .lib import CITY_WEATHER, LIVE_CITIES, OPEN_METEO_HOURLY_VARS, weather_rows, year_chunks
from .resources import SnowflakeResource

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def _fetch(url: str, params: dict) -> dict:
    import requests

    resp = requests.get(url, params=params, timeout=120)
    resp.raise_for_status()
    return resp.json()


@asset(group_name="weather")
def weather_hourly(snowflake: SnowflakeResource) -> MaterializeResult:
    """Forecast API pull (past_days=2 heals late revisions; future hours get
    overwritten by later pulls as forecasts settle into observations), merged
    on (city, local_date, local_hour)."""
    counts = {}
    for city in LIVE_CITIES:
        spec = CITY_WEATHER[city]
        payload = _fetch(
            FORECAST_URL,
            {
                "latitude": spec["lat"],
                "longitude": spec["lon"],
                "hourly": OPEN_METEO_HOURLY_VARS,
                "past_days": 2,
                "timezone": "UTC",
            },
        )
        rows = weather_rows(city, spec["tz"], payload, datetime.now(UTC))
        snowflake.merge_weather(rows)
        counts[city] = len(rows)
    return MaterializeResult(metadata={"rows_merged": MetadataValue.json(counts)})


@asset(group_name="weather")
def weather_backfill_2yr(snowflake: SnowflakeResource) -> MaterializeResult:
    """Manual trigger only (no schedule): ~2 years of hourly history from the
    archive API, merged in calendar-year chunks. The archive settles ~5 days
    behind real time; the hourly forecast pull owns the recent edge."""
    end = datetime.now(UTC).date() - timedelta(days=6)
    start = end - timedelta(days=730)
    counts = {}
    for city in LIVE_CITIES:
        spec = CITY_WEATHER[city]
        total = 0
        for lo, hi in year_chunks(start, end):
            payload = _fetch(
                ARCHIVE_URL,
                {
                    "latitude": spec["lat"],
                    "longitude": spec["lon"],
                    "start_date": lo.isoformat(),
                    "end_date": hi.isoformat(),
                    "hourly": OPEN_METEO_HOURLY_VARS,
                    "timezone": "UTC",
                },
            )
            rows = weather_rows(city, spec["tz"], payload, datetime.now(UTC))
            snowflake.merge_weather(rows)
            total += len(rows)
            time.sleep(1)  # be polite between archive chunks
        counts[city] = total
    return MaterializeResult(metadata={"rows_merged": MetadataValue.json(counts)})


weather_job = define_asset_job(
    "weather_hourly_job", selection=AssetSelection.assets(weather_hourly)
)

weather_schedule = ScheduleDefinition(
    default_status=DefaultScheduleStatus.RUNNING,
    job=weather_job,
    cron_schedule="20 * * * *",
    execution_timezone="UTC",
)
