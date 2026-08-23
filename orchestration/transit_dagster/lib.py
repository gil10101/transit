"""Pure logic behind the Dagster assets: EMR job-request assembly, open-meteo row
transforms, raw-feed freshness evaluation, Iceberg refresh statements.

No dagster or boto3 imports here — clients are passed in — so the repo venv can
unit-test this module without the orchestration image's dependency set. The
dagster wiring lives in resources.py / assets_*.py / checks.py.
"""

from __future__ import annotations

import uuid
import warnings
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from importlib import util as importlib_util
from pathlib import Path
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# EMR Serverless job requests
# ---------------------------------------------------------------------------


def require_env(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "")
    if not value:
        raise RuntimeError(
            f"{name} is not set. Cloud assets need the EMR/Snowflake env block "
            "(docker-compose.yml locally, the services-module user_data on EC2)."
        )
    return value


def drain_job_request(env: Mapping[str, str], client_token: str | None = None) -> dict:
    """StartJobRun kwargs for one silver drain.

    Same env contract and request shape as infra/modules/spark/lambda/drain.py;
    the param strings are single-sourced from the spark module's
    locals.spark_params via module outputs -> services user_data env.
    """
    return {
        "applicationId": require_env(env, "APP_ID"),
        "executionRoleArn": require_env(env, "EXEC_ROLE_ARN"),
        "clientToken": client_token or str(uuid.uuid4()),
        "name": "transit-drain-dagster",
        "jobDriver": {
            "sparkSubmit": {
                "entryPoint": require_env(env, "ENTRY_POINT"),
                "sparkSubmitParameters": require_env(env, "SPARK_PARAMS"),
            }
        },
        "configurationOverrides": {
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": require_env(env, "LOG_URI")}
            }
        },
    }


def static_job_request(
    env: Mapping[str, str], city: str = "nyc", client_token: str | None = None
) -> dict:
    """StartJobRun kwargs for the weekly static GTFS parse.

    Drain params plus: entryPoint swapped to gtfs_static_parse.py, the city as
    argv, and RAW_BUCKET exported to the driver (the job archives the zip from
    the driver; the streaming drain never needs that env so SPARK_PARAMS lacks it).
    """
    request = drain_job_request(env, client_token)
    params = request["jobDriver"]["sparkSubmit"]["sparkSubmitParameters"]
    raw_bucket = require_env(env, "RAW_BUCKET")
    request["name"] = "transit-gtfs-static"
    request["jobDriver"]["sparkSubmit"] = {
        "entryPoint": require_env(env, "STATIC_ENTRY_POINT"),
        "entryPointArguments": [city],
        "sparkSubmitParameters": (
            f"{params} --conf spark.emr-serverless.driverEnv.RAW_BUCKET={raw_bucket}"
        ),
    }
    return request


# ---------------------------------------------------------------------------
# Open-meteo -> TRANSIT.SILVER.WEATHER_HOURLY rows
# ---------------------------------------------------------------------------

OPEN_METEO_HOURLY_VARS = "temperature_2m,precipitation,rain,snowfall,wind_speed_10m,weather_code"


def weather_rows(city: str, tz_name: str, payload: Mapping, pulled_at: datetime) -> list[tuple]:
    """open-meteo hourly payload -> (city, local_date, local_hour, temp_c,
    precip_mm, snowfall_cm, wind_kph, weather_code, pulled_at) tuples.

    Times arrive in UTC (requests pass timezone=UTC); bucketing to the city's
    local calendar happens here with zoneinfo, never in the warehouse (the
    account's session tz defaults to America/Los_Angeles — quirk 1).
    `rain` is fetched but not stored: `precipitation` already includes it.
    DST fall-back maps two UTC hours onto one local hour; last one wins so the
    MERGE source never carries a duplicate key.
    """
    hourly = payload["hourly"]
    tz = ZoneInfo(tz_name)
    pulled = pulled_at.astimezone(UTC).replace(tzinfo=None)
    deduped: dict[tuple, tuple] = {}
    for i, stamp in enumerate(hourly["time"]):
        local = datetime.fromisoformat(stamp).replace(tzinfo=UTC).astimezone(tz)
        key = (city, local.date(), local.hour)
        deduped[key] = (
            *key,
            hourly["temperature_2m"][i],
            hourly["precipitation"][i],
            hourly["snowfall"][i],
            hourly["wind_speed_10m"][i],
            hourly["weather_code"][i],
            pulled,
        )
    return list(deduped.values())


def year_chunks(start: date, end: date) -> list[tuple[date, date]]:
    """Inclusive [start, end] split on calendar-year boundaries (archive API
    backfill requests stay comfortably sized)."""
    chunks = []
    lo = start
    while lo <= end:
        hi = min(date(lo.year, 12, 31), end)
        chunks.append((lo, hi))
        lo = hi + timedelta(days=1)
    return chunks


# ---------------------------------------------------------------------------
# Raw-feed freshness (the killed-feed tripwire)
# ---------------------------------------------------------------------------

NYC_FEED_ENDPOINTS = ("base", "ace", "bdfm", "g", "jz", "nqrw", "l", "si")


def hour_prefixes(city: str, endpoint: str, now: datetime) -> list[str]:
    """Current and previous UTC hour prefixes in the poller's raw layout
    (<city>/<endpoint>/<YYYY-MM-DD>/<HH>/ — ingestion RawArchiver). A 45-min
    staleness window never spans more than these two."""
    return [f"{city}/{endpoint}/{t:%Y-%m-%d}/{t:%H}/" for t in (now, now - timedelta(hours=1))]


def newest_age_sec(s3, bucket: str, prefixes: Sequence[str], now: datetime) -> float | None:
    """Age of the newest object under any prefix, None when all are empty.
    Hour prefixes hold at most ~120 objects (30s poll), one page."""
    newest = None
    for prefix in prefixes:
        for obj in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", []):
            if newest is None or obj["LastModified"] > newest:
                newest = obj["LastModified"]
    if newest is None:
        return None
    return (now - newest).total_seconds()


def evaluate_feed_freshness(
    s3,
    bucket: str,
    now: datetime | None = None,
    city: str = "nyc",
    endpoints: Sequence[str] = NYC_FEED_ENDPOINTS,
    max_age_min: int = 40,
) -> dict[str, dict]:
    """Per endpoint: newest raw object age vs the staleness threshold.
    Returns {endpoint: {"age_sec": float | None, "stale": bool}}."""
    now = now or datetime.now(UTC)
    report = {}
    for endpoint in endpoints:
        age = newest_age_sec(s3, bucket, hour_prefixes(city, endpoint, now), now)
        report[endpoint] = {"age_sec": age, "stale": age is None or age > max_age_min * 60}
    return report


# ---------------------------------------------------------------------------
# Snowflake Iceberg refresh (ported from scripts/snowflake_register_iceberg.py)
# ---------------------------------------------------------------------------

EXTERNAL_VOLUME = "TRANSIT_LAKEHOUSE"
CATALOG_INTEGRATION = "TRANSIT_OBJ_STORE"

# Verbatim copy of TABLES in scripts/snowflake_register_iceberg.py, used only
# when that script cannot be loaded (silver_tables() prefers the script so the
# list stays single-sourced).
FALLBACK_SILVER_TABLES = [
    "stop_time_predictions",
    "vehicle_positions",
    "alerts",
    "gtfs_static_routes",
    "gtfs_static_trips",
    "gtfs_static_stops",
    "gtfs_static_stop_times",
    "gtfs_static_calendar",
    "gtfs_static_calendar_dates",
    "gtfs_static_shapes",
]


def silver_tables() -> list[str]:
    """TABLES from scripts/snowflake_register_iceberg.py. The script is not a
    package, so load it by path from the repo or image root; fall back to the
    verbatim copy above."""
    here = Path(__file__).resolve()
    for root in (here.parents[1], here.parents[2]):
        script = root / "scripts" / "snowflake_register_iceberg.py"
        if script.exists():
            try:
                spec = importlib_util.spec_from_file_location("_tp_register_iceberg", script)
                module = importlib_util.module_from_spec(spec)
                spec.loader.exec_module(module)
                return list(module.TABLES)
            except Exception as e:  # noqa: BLE001 — fall back, but never silently
                warnings.warn(
                    f"register script load failed ({e!r}); using fallback table list",
                    stacklevel=2,
                )
                break
    return list(FALLBACK_SILVER_TABLES)


def latest_metadata_path(s3, bucket: str, table: str) -> str | None:
    """Latest metadata json for a Hadoop-catalog Iceberg table, relative to the
    external volume base (s3://<bucket>/iceberg/), via its version-hint.text."""
    key = f"iceberg/silver/{table}/metadata/version-hint.text"
    try:
        version = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode().strip()
    except s3.exceptions.NoSuchKey:
        return None
    if not version.isdigit():  # hint content reaches SQL strings — reject garbage early
        raise ValueError(f"corrupt version-hint for {table}: {version!r}")
    return f"silver/{table}/metadata/v{version}.metadata.json"


def iceberg_refresh_statements(table: str, metadata_path: str) -> list[str]:
    """Register-if-new then pin to the latest snapshot (no-op when already
    there) — the same two statements the register script emits. The catalog
    integration itself was created once in P2 under accountadmin."""
    name = f"TRANSIT.SILVER.{table.upper()}"
    return [
        f"create iceberg table if not exists {name} "
        f"external_volume = '{EXTERNAL_VOLUME}' catalog = '{CATALOG_INTEGRATION}' "
        f"metadata_file_path = '{metadata_path}'",
        f"alter iceberg table {name} refresh '{metadata_path}'",
    ]
