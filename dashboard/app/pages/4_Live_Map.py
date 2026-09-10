"""Page 4 — last-snapshot map. Latest vehicle position per vehicle from silver,
labeled with its actual lag rather than pretending to be realtime. The Snowflake
SILVER view only advances when the twice-daily chain re-pins Iceberg metadata, so a
15-minute freshness window is guaranteed-empty by design — the window here is 13h
and the per-city lag is shown instead. Helsinki and Zurich publish no
vehicle-position product on the feeds we poll (HSL core has no VP; Swiss LA API
is trip-updates only), so they are honestly absent."""

import pandas as pd
import pydeck as pdk
import streamlit as st
from lib import CITIES, brand, city_name, empty_state, q

st.set_page_config(page_title="Live map", layout="wide")
brand()
st.title("Vehicles on the road — last snapshot")
st.caption(
    "Latest fix per vehicle from the most recent drained window. Silver reaches "
    "Snowflake when the twice-daily chain re-pins Iceberg metadata, so up to ~12h of "
    "lag is design, not outage — each city is labeled with its actual lag."
)

vp = q("""
    with latest as (
        select city as city_key, vehicle_id, lat, lon, fetched_at,
               max(fetched_at) over (partition by city) as city_latest
        from TRANSIT.SILVER.vehicle_positions
        where fetched_at > dateadd(hour, -13, current_timestamp())
          and lat is not null and lon is not null
        qualify row_number() over (partition by city, vehicle_id order by fetched_at desc) = 1
    )
    select city_key, vehicle_id, lat, lon, fetched_at
    from latest
    where fetched_at > dateadd(minute, -10, city_latest)
""")

alerts = q("""
    select city_key, sum(alerts_active) as active_alerts
    from fct_alerts_daily
    where service_date >= current_date - 1
    group by 1
""")

if vp.empty:
    empty_state(
        "No vehicle positions in the last 6 hours — a chain or drain missed; check the ops page."
    )
    st.stop()

now = pd.Timestamp.now(tz="UTC")
lag = (
    vp.assign(fetched_at=pd.to_datetime(vp.fetched_at, utc=True))
    .groupby("city_key")
    .agg(vehicles=("vehicle_id", "nunique"), latest=("fetched_at", "max"))
    .reset_index()
    .sort_values("city_key")
)
lag["lag_min"] = ((now - lag.latest).dt.total_seconds() / 60).round().astype(int)

cols = st.columns(len(lag) if len(lag) else 1)
for col, row in zip(cols, lag.itertuples(), strict=False):
    col.metric(
        f"{city_name(row.city_key)} vehicles",
        f"{int(row.vehicles):,}",
        f"{row.lag_min} min lag",
        delta_color="off",
    )

vp["fill"] = vp.city_key.map(lambda c: CITIES.get(c, {}).get("color", [200, 200, 200]) + [160])
city = st.selectbox("City", sorted(vp.city_key.unique()), format_func=city_name)
sub = vp[vp.city_key == city]
meta = CITIES[city]
st.pydeck_chart(
    pdk.Deck(
        map_style=None,
        initial_view_state=pdk.ViewState(
            latitude=meta["center"][0], longitude=meta["center"][1], zoom=meta["zoom"]
        ),
        layers=[
            pdk.Layer(
                "ScatterplotLayer",
                sub,
                get_position=["lon", "lat"],
                get_fill_color="fill",
                get_radius=45,
                pickable=True,
            )
        ],
        tooltip={"text": "{vehicle_id}"},
    ),
    height=520,
)

with st.sidebar:
    st.subheader("Alerts touching the last 2 days")
    if alerts.empty:
        st.caption("none recorded")
    else:
        alerts["city"] = alerts.city_key.map(city_name)
        st.dataframe(alerts[["city", "active_alerts"]], hide_index=True)
    st.caption(
        "Helsinki and Zurich publish no vehicle-position product on the feeds we "
        "poll, so they appear on every other page but not this one."
    )
