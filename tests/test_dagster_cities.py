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
    POLLED_CITIES,
    city_feed_endpoints,
)


def test_live_cities_is_p3_batch2():
    # batch 1 keyless trio + batch 2 keyed dc/sf/zurich; chicago appends on
    # CTA key activation (order: existing + dc, sf, zurich — locked)
    assert LIVE_CITIES == ("nyc", "boston", "toronto", "helsinki", "dc", "sf", "zurich")


def test_city_weather_covers_every_live_city_with_real_tz():
    assert set(CITY_WEATHER) == set(LIVE_CITIES)
    for spec in CITY_WEATHER.values():
        ZoneInfo(spec["tz"])  # raises on a bad IANA name
        assert -90 <= spec["lat"] <= 90 and -180 <= spec["lon"] <= 180
    # dictionary tzs (do not re-litigate): boston + dc share NYC's, the rest differ
    assert CITY_WEATHER["boston"]["tz"] == "America/New_York"
    assert CITY_WEATHER["toronto"]["tz"] == "America/Toronto"
    assert CITY_WEATHER["helsinki"]["tz"] == "Europe/Helsinki"
    assert CITY_WEATHER["dc"]["tz"] == "America/New_York"
    assert CITY_WEATHER["sf"]["tz"] == "America/Los_Angeles"
    assert CITY_WEATHER["zurich"]["tz"] == "Europe/Zurich"


def test_repo_configs_discovered_only_live_cities():
    # against the real ingestion/config/cities: endpoint names are the
    # feed_groups keys (they drive the freshness tripwire's raw prefixes) —
    # never a city outside LIVE_CITIES, never an empty endpoint tuple.
    endpoints = city_feed_endpoints()
    assert len(endpoints["nyc"]) == 8
    assert endpoints["boston"] == ("trip_updates", "vehicle_positions", "alerts")
    assert endpoints["toronto"] == ("trip_updates", "vehicle_positions", "alerts")
    assert endpoints["helsinki"] == ("trip_updates", "alerts")  # HSL has no VP feed
    # P3 batch 2 (27 endpoints total): dc = {rail,bus} x {TU,VP,alerts};
    # sf = 511 regional aggregation, 3 feeds (at poll_seconds 200 — rate cap);
    # zurich = TU + service alerts only (no VP product on Swiss OTD LA API).
    assert endpoints["dc"] == (
        "rail_trip_updates",
        "rail_vehicle_positions",
        "rail_alerts",
        "bus_trip_updates",
        "bus_vehicle_positions",
        "bus_alerts",
    )
    assert endpoints["sf"] == ("trip_updates", "vehicle_positions", "alerts")
    # zurich's yaml declares (trip_updates, alerts) but its poller is withheld
    # until the route allow-list ships, so the tripwire must not probe it
    assert "zurich" not in endpoints
    assert sum(len(v) for v in endpoints.values()) == 25
    assert set(endpoints) == set(POLLED_CITIES)  # tripwire covers every polled city


def test_endpoints_are_feed_group_keys_and_non_live_skipped(tmp_path: Path):
    (tmp_path / "boston.yaml").write_text(
        "city: boston\nfeed_groups:\n"
        "  trip_updates: https://x/tu\n  vehicle_positions: https://x/vp\n  alerts: https://x/al\n"
    )
    (tmp_path / "chicago.yaml").write_text("city: chicago\nfeed_groups:\n  all: https://x\n")
    out = city_feed_endpoints(config_dir=tmp_path)
    # chicago yaml present but not live -> excluded until its CTA beta key
    # activates and batch 2b flips LIVE_CITIES
    assert out == {"boston": ("trip_updates", "vehicle_positions", "alerts")}


def test_no_live_configs_raises_instead_of_probing_nothing(tmp_path: Path):
    with pytest.raises(RuntimeError, match="feed_groups"):
        city_feed_endpoints(config_dir=tmp_path)
