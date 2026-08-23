"""Unit tests for the P3 fan-out plumbing in orchestration lib (W3): the
live-city registry, weather centroids, and config-driven feed endpoint
discovery behind the raw-feed freshness tripwire.

Only orchestration.transit_dagster.lib is imported — like test_dagster_lib.py,
these run in the repo venv without dagster installed.
"""

from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from orchestration.transit_dagster.lib import (
    CITY_WEATHER,
    LIVE_CITIES,
    city_feed_endpoints,
)


def test_live_cities_is_p3_batch1():
    assert LIVE_CITIES == ("nyc", "boston", "toronto", "helsinki")


def test_city_weather_covers_every_live_city_with_real_tz():
    assert set(CITY_WEATHER) == set(LIVE_CITIES)
    for spec in CITY_WEATHER.values():
        ZoneInfo(spec["tz"])  # raises on a bad IANA name
        assert -90 <= spec["lat"] <= 90 and -180 <= spec["lon"] <= 180
    # dictionary tzs (do not re-litigate): boston shares NYC's, the others differ
    assert CITY_WEATHER["boston"]["tz"] == "America/New_York"
    assert CITY_WEATHER["toronto"]["tz"] == "America/Toronto"
    assert CITY_WEATHER["helsinki"]["tz"] == "Europe/Helsinki"


def test_repo_configs_discovered_only_live_cities():
    # against the real ingestion/config/cities: nyc always present with its 8
    # feeds; other live cities appear as their yamls land (W1) — never a city
    # outside LIVE_CITIES, never an empty endpoint tuple.
    endpoints = city_feed_endpoints()
    assert len(endpoints["nyc"]) == 8
    assert endpoints["boston"] == ("trip_updates", "vehicle_positions", "alerts")
    assert endpoints["toronto"] == ("trip_updates", "vehicle_positions", "alerts")
    assert endpoints["helsinki"] == ("trip_updates", "alerts")  # HSL has no VP feed
    assert set(endpoints) == set(LIVE_CITIES)  # tripwire covers every live city


def test_endpoints_are_feed_group_keys_and_non_live_skipped(tmp_path: Path):
    (tmp_path / "boston.yaml").write_text(
        "city: boston\nfeed_groups:\n"
        "  trip_updates: https://x/tu\n  vehicle_positions: https://x/vp\n  alerts: https://x/al\n"
    )
    (tmp_path / "zurich.yaml").write_text("city: zurich\nfeed_groups:\n  all: https://x\n")
    out = city_feed_endpoints(config_dir=tmp_path)
    # zurich yaml present but not live -> excluded until batch 2 flips LIVE_CITIES
    assert out == {"boston": ("trip_updates", "vehicle_positions", "alerts")}


def test_no_live_configs_raises_instead_of_probing_nothing(tmp_path: Path):
    with pytest.raises(RuntimeError, match="feed_groups"):
        city_feed_endpoints(config_dir=tmp_path)
