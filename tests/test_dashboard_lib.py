"""Unit tests for the dashboard's pure helpers (no Snowflake, no Streamlit runtime)."""

import sys
from pathlib import Path

import pytest

# lib imports streamlit, which lives in the optional `dashboard` dependency group
pytest.importorskip("streamlit", reason="dashboard group not installed")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dashboard" / "app"))

from lib import CITIES, city_name, delay_color  # noqa: E402


def test_delay_color_is_rgba_everywhere():
    for d in (-10_000, -120, -1, 0, 1, 150, 300, 301, 600, 10_000):
        c = delay_color(d)
        assert len(c) == 4
        assert all(isinstance(v, int) and 0 <= v <= 255 for v in c), (d, c)


def test_delay_color_orders_severity():
    # more delay must never look LESS red than less delay
    reds = [delay_color(d)[0] for d in (0, 100, 200, 300)]
    assert reds == sorted(reds)


def test_delay_color_early_is_blue_late_is_red():
    early = delay_color(-120)
    late = delay_color(600)
    assert early[2] > early[0]  # blue dominates
    assert late[0] > late[2]  # red dominates


def test_city_registry_complete():
    # every live city must carry the fields every page relies on
    for key, meta in CITIES.items():
        assert set(meta) >= {"name", "color", "center", "zoom"}, key
        lat, lon = meta["center"]
        assert -90 <= lat <= 90 and -180 <= lon <= 180


def test_city_name_falls_back_to_key():
    assert city_name("nyc") == "New York"
    assert city_name("atlantis") == "atlantis"
