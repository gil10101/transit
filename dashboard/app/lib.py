"""Shared plumbing for the Streamlit dashboard.

Read path only: every page queries gold (and, for the live map, silver) and writes
nothing. Connection is key-pair auth, same mechanism as dbt/sfq.

Role: defaults to TRANSIT_PIPELINE so a laptop run works today. A public deploy
must NOT ship that role — create a read-only one first (needs ACCOUNTADMIN, one
time):

    create role TRANSIT_READER;
    grant usage on database TRANSIT to role TRANSIT_READER;
    grant usage on all schemas in database TRANSIT to role TRANSIT_READER;
    grant select on all tables in schema TRANSIT.GOLD to role TRANSIT_READER;
    grant select on future tables in schema TRANSIT.GOLD to role TRANSIT_READER;
    grant select on all tables in schema TRANSIT.SILVER to role TRANSIT_READER;
    grant select on future tables in schema TRANSIT.SILVER to role TRANSIT_READER;
    grant usage on warehouse TRANSFORM_XS to role TRANSIT_READER;
    grant role TRANSIT_READER to user DASHBOARD_SVC;

then set SNOWFLAKE_ROLE=TRANSIT_READER (and a DASHBOARD_SVC key) in the app's env.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import snowflake.connector
import streamlit as st
from cryptography.hazmat.primitives import serialization

# one entry per city that has ever reached gold; color is the city's fixed hue
# across every page so a reader can track a city between charts
CITIES: dict[str, dict] = {
    "nyc": {"name": "New York", "color": [66, 133, 244], "center": (40.75, -73.98), "zoom": 10},
    "boston": {"name": "Boston", "color": [219, 68, 55], "center": (42.36, -71.06), "zoom": 11},
    "dc": {"name": "Washington DC", "color": [244, 180, 0], "center": (38.90, -77.03), "zoom": 11},
    "sf": {"name": "SF Bay Area", "color": [15, 157, 88], "center": (37.78, -122.28), "zoom": 9},
    "toronto": {"name": "Toronto", "color": [171, 71, 188], "center": (43.70, -79.40), "zoom": 10},
    "helsinki": {"name": "Helsinki", "color": [0, 172, 193], "center": (60.20, 24.94), "zoom": 10},
    "zurich": {"name": "Zurich", "color": [255, 112, 67], "center": (47.38, 8.54), "zoom": 11},
}


def city_name(key: str) -> str:
    return CITIES.get(key, {}).get("name", key)


@st.cache_resource
def _connection() -> snowflake.connector.SnowflakeConnection:
    key_path = Path(
        os.environ.get(
            "SNOWFLAKE_PRIVATE_KEY_PATH",
            Path.home() / ".snowflake/keys/transit_terraform_key.p8",
        )
    )
    pk = serialization.load_pem_private_key(key_path.read_bytes(), password=None)
    pkb = pk.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=os.environ.get("SNOWFLAKE_ACCOUNT", "wqteqyy-ib47757"),
        user=os.environ.get("SNOWFLAKE_USER", "TERRAFORM_SVC"),
        private_key=pkb,
        role=os.environ.get("SNOWFLAKE_ROLE", "TRANSIT_PIPELINE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "TRANSFORM_XS"),
        database="TRANSIT",
        schema="GOLD",
    )


@st.cache_data(ttl=600, show_spinner="querying warehouse…")
def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Run one read-only query, cached 10 minutes (keeps the XS warehouse asleep
    between visits instead of paying per page interaction).

    Widget values are user-controlled (query params, websocket) — they go through
    %(name)s bind parameters, never into the SQL string."""
    cur = _connection().cursor()
    try:
        cur.execute(sql, params or {})
        df = cur.fetch_pandas_all()
    finally:
        cur.close()
    df.columns = [c.lower() for c in df.columns]
    return df


def _mix(a: tuple, b: tuple, t: float, alpha: int) -> list[int]:
    return [int(a[i] + (b[i] - a[i]) * t) for i in range(3)] + [alpha]


# gillu.me / skynyc light data palette: good #059669, warn #d97700, bad #cc0000,
# early toward the data blue #2563eb. Same semantic hues as the portfolio site.
_GOOD, _WARN, _BAD, _EARLY = (5, 150, 105), (217, 119, 0), (204, 0, 0), (37, 99, 235)


def delay_color(delay_sec: float) -> list[int]:
    """Shared color scale for delay maps: good at 0, warn at +150s, bad at
    +300s and beyond; early (negative) shades toward blue. Clamped so every city
    page reads on the same scale — the whole point of the comparison."""
    d = max(-120.0, min(600.0, float(delay_sec)))
    if d <= 0:  # early: good at 0 -> blue at -120s, continuous with the late ramp
        return _mix(_GOOD, _EARLY, min(1.0, -d / 120.0), 160)
    if d <= 150:
        return _mix(_GOOD, _WARN, d / 150.0, 170)
    if d <= 300:
        return _mix(_WARN, _BAD, (d - 150) / 150.0, 170)
    return [204, 0, 0, 200]


def empty_state(msg: str) -> None:
    st.info(msg)


def brand() -> None:
    """gillu.me design system, applied on top of .streamlit/config.toml colors:
    Geist Sans everywhere, Geist Mono for numbers, 36px/500 page titles,
    uppercase tracked section headings, hairline sidebar. Call once per page,
    right after st.set_page_config."""
    st.markdown(
        """
<style>
@import url('https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600&family=Geist+Mono:wght@400;500&display=swap');
html, body, [data-testid="stAppViewContainer"] * {
  font-family: 'Geist', system-ui, sans-serif;
}
/* the global font override must not eat Streamlit's icon ligatures */
[data-testid="stIconMaterial"], .material-symbols-rounded {
  font-family: 'Material Symbols Rounded' !important;
}
h1 { font-size: 36px !important; font-weight: 500 !important; letter-spacing: -0.02em; }
h2, h3 {
  font-size: 14px !important; font-weight: 600 !important;
  letter-spacing: 0.14em; text-transform: uppercase; color: #666666 !important;
}
[data-testid="stCaptionContainer"], .stCaption { color: #666666; }
[data-testid="stMetricValue"] {
  font-family: 'Geist Mono', ui-monospace, monospace !important;
  font-variant-numeric: tabular-nums; font-weight: 500;
}
[data-testid="stMetricLabel"] { color: #666666; }
[data-testid="stSidebar"] {
  background: #f5f5f5; border-right: 1px solid #e0e0e0;
}
[data-testid="stSidebarNav"] span { text-transform: lowercase; font-size: 14px; }
[data-testid="stHeader"] { background: rgba(255,255,255,0.9); }
</style>
""",
        unsafe_allow_html=True,
    )
