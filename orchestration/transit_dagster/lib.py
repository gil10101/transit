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
# Live cities (single source for the P3 fan-out across the Dagster assets)
# ---------------------------------------------------------------------------

# Cities with a live ingestion pipeline. P3 batch 1 added the keyless GTFS-RT
# trio; batch 2 (2026-08-23) adds the keyed dc/sf/zurich. Chicago appends here
# when its CTA GTFS-RT beta key activates.
# The services-box compose (infra/modules/services/main.tf) runs one poller per
# entry — keep the two lists in step.
LIVE_CITIES = ("nyc", "boston", "toronto", "helsinki", "dc", "sf", "zurich")

# Cities whose POLLER is actually deployed. Kept separate from LIVE_CITIES
# because a city can be live for static/weather before its poller ships: zurich
# was in LIVE_CITIES from batch 2 (we needed its national static parsed to build
# the route allow-list) while its poller was deliberately withheld, and the
# raw-feed tripwire asserted on prefixes nothing wrote to, failing every 15 min.
# [rev 2026-08-25] Zurich's allow-list shipped, so its poller deploys and it
# rejoins. Add a city here only once its poller is actually running.
POLLED_CITIES = LIVE_CITIES

# Centroid + tz per city, duplicated from the dim_city seed
# (dbt/transit/seeds/dim_city.csv) on purpose: weather pulls must never wake
# the warehouse just to read a dim. Keep in sync when cities are added.
CITY_WEATHER = {
    "nyc": {"lat": 40.7128, "lon": -74.0060, "tz": "America/New_York"},
    "boston": {"lat": 42.3601, "lon": -71.0589, "tz": "America/New_York"},
    "toronto": {"lat": 43.6532, "lon": -79.3832, "tz": "America/Toronto"},
    "helsinki": {"lat": 60.1699, "lon": 24.9384, "tz": "Europe/Helsinki"},
    "dc": {"lat": 38.9072, "lon": -77.0369, "tz": "America/New_York"},
    "sf": {"lat": 37.7749, "lon": -122.4194, "tz": "America/Los_Angeles"},
    "zurich": {"lat": 47.3769, "lon": 8.5417, "tz": "Europe/Zurich"},
}


def cities_config_dir(config_dir: Path | None = None) -> Path:
    """Locate ingestion/config/cities in a repo checkout or the dagster image.

    Repo checkout: <repo>/ingestion/config/cities next to orchestration/;
    dagster image: /opt/dagster/app/ingestion/config/cities (Dockerfile COPY).
    """
    if config_dir is not None:
        return Path(config_dir)
    here = Path(__file__).resolve()
    for root in (here.parents[1], here.parents[2]):
        candidate = root / "ingestion" / "config" / "cities"
        if candidate.is_dir():
            return candidate
    raise RuntimeError(
        "ingestion/config/cities not found near the repo/image root; "
        "the freshness check and static refresh need the city yamls"
    )


def city_static_sources(city: str, config_dir: Path | None = None):
    """Static GTFS sources for <city> from its yaml, normalized by
    ingestion.city_static (a URL string, or {name: {url, auth}} for agencies
    that split the schedule across zips or gate it behind a key).

    Feed URLs live in the city configs and nowhere else (CLAUDE.md: never invent
    a URL). Returns [] when the city has no static feed — the weekly asset skips
    it rather than failing the other cities' refresh.
    """
    import yaml  # lazy, as above

    from ingestion.city_static import static_sources

    path = cities_config_dir(config_dir) / f"{city}.yaml"
    cfg = yaml.safe_load(path.read_text()) or {}
    return static_sources(cfg.get("static_gtfs"))


def city_feed_endpoints(
    config_dir: Path | None = None, live: Sequence[str] = POLLED_CITIES
) -> dict[str, tuple[str, ...]]:
    """Feed endpoint names per live city, read from the feed_groups keys in
    ingestion/config/cities/*.yaml (the keys are the <endpoint> segment of the
    poller's raw layout, so they drive the freshness prefixes directly).

    Config-driven on purpose: a new city yaml shows up in the freshness
    tripwire without touching checks code. A yaml for a city not yet in
    POLLED_CITIES is skipped (its poller is not deployed). Raises when no
    live-city config is found — the tripwire must fail loudly, never probe
    nothing and pass. Called at materialize time only, so definitions still
    import without the configs present.
    """
    import yaml  # lazy: repo venv + dagster image have it; never ships to EMR

    config_dir = cities_config_dir(config_dir)
    out: dict[str, tuple[str, ...]] = {}
    for path in sorted(Path(config_dir).glob("*.yaml")):
        cfg = yaml.safe_load(path.read_text()) or {}
        city = cfg.get("city", path.stem)
        if city not in live:
            continue
        endpoints = tuple(cfg.get("feed_groups") or ())
        if endpoints:
            out[city] = endpoints
    if not out:
        raise RuntimeError(f"no live-city configs with feed_groups under {config_dir}")
    return out


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
    env: Mapping[str, str],
    city: str = "nyc",
    version_id: str | None = None,
    client_token: str | None = None,
) -> dict:
    """StartJobRun kwargs for the weekly static GTFS parse.

    Drain params plus: entryPoint swapped to gtfs_static_parse.py, [city,
    version_id] as argv, and RAW_BUCKET exported to the driver (the job reads
    the staged zip from raw; the streaming drain never needs that env so
    SPARK_PARAMS lacks it). version_id points at the zip the asset already
    archived — EMR base Python lacks requests, so the download happens on the
    Dagster box, never on EMR.
    """
    request = drain_job_request(env, client_token)
    params = request["jobDriver"]["sparkSubmit"]["sparkSubmitParameters"]
    raw_bucket = require_env(env, "RAW_BUCKET")
    request["name"] = "transit-gtfs-static"
    request["jobDriver"]["sparkSubmit"] = {
        "entryPoint": require_env(env, "STATIC_ENTRY_POINT"),
        "entryPointArguments": [city] + ([version_id] if version_id else []),
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


# --- run-failure alerting -----------------------------------------------------
# [rev 2026-08-25] The pipeline detected breakage and told nobody: the raw-feed tripwire
# failed every 15 min for a day and was found by hand. The dagster wiring lives in
# alerting.py; the message shape lives here so it is unit-testable without dagster.

# SNS caps a message at 256 KB and a dagster stack trace can exceed it. A truncated
# alert that arrives beats a rejected one.
ALERT_MAX_ERROR_CHARS = 4000


def alert_body(run_id: str, job_name: str, error: str | None) -> str:
    """The message a human reads at 3am: job and run id first, then the error."""
    # strip FIRST, then fall back: dagster can hand us a whitespace-only message, and
    # `(error or default).strip()` would render that as an alert with no detail at all
    detail = (error or "").strip() or "no error detail recorded"
    if len(detail) > ALERT_MAX_ERROR_CHARS:
        detail = detail[:ALERT_MAX_ERROR_CHARS] + "\n... (truncated)"
    return f"job: {job_name}\nrun: {run_id}\n\n{detail}"
