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


def test_live_cities_is_p4():
    # batch 1 keyless trio + batch 2 keyed dc/sf/zurich + P4 tokyo; chicago
    # appends on CTA key activation (order: existing + tokyo — locked)
    assert LIVE_CITIES == ("nyc", "boston", "toronto", "helsinki", "dc", "sf", "zurich", "tokyo")
    # [rev 2026-09-03] tokyo's poller is deployed and archiving all three ODPT
    # endpoints, so it joins POLLED_CITIES and the raw-feed tripwire now asserts
    # on it. The split itself stays load-bearing: a city is live for
    # static/weather before its poller ships, and asserting on raw prefixes
    # nothing writes to failed the tripwire every 15 minutes when zurich was
    # added early (b708622). The next city repeats that gap.
    assert set(LIVE_CITIES) - set(POLLED_CITIES) == set()


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
    assert CITY_WEATHER["tokyo"]["tz"] == "Asia/Tokyo"


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
    assert endpoints["zurich"] == ("trip_updates", "alerts")
    # [rev 2026-09-03] tokyo is ODPT JSON, not GTFS-RT, so its endpoint names are
    # its own: train-grain snapshots, line-status text on a slower cadence, and
    # Toei's bus VehiclePositions. The poller requests BOTH TokyoMetro and Toei
    # for trains — Metro publishes no odpt:Train today, so it contributes nothing
    # and starts flowing the moment they do, without a config change.
    assert endpoints["tokyo"] == (
        "trains",
        "train_information",
        "toeibus_vehicle_positions",
    )
    assert sum(len(v) for v in endpoints.values()) == 30
    # POLLED_CITIES, never LIVE_CITIES: a city can be live for static/weather
    # before its poller ships, and the tripwire must not assert on prefixes
    # nothing writes to (zurich did exactly that until its allow-list landed)
    assert set(endpoints) == set(POLLED_CITIES)


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
