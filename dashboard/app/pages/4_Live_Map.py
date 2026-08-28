"""Page 4 — live-ish map. Latest vehicle position per vehicle from silver,
labeled with its actual lag rather than pretending to be realtime. Helsinki and
Zurich publish no vehicle-position product on the feeds we poll (HSL core has no
VP; Swiss LA API is trip-updates only), so they are honestly absent here."""

import pydeck as pdk
import streamlit as st
from lib import brand, CITIES, city_name, empty_state, q

st.set_page_config(page_title="Live map", page_icon="📍", layout="wide")
brand()
st.title("Vehicles on the road (last snapshot)")

vp = q("""
    select city as city_key, vehicle_id, lat, lon, fetched_at
    from TRANSIT.SILVER.vehicle_positions
    where fetched_at > dateadd(minute, -15, current_timestamp())
      and lat is not null and lon is not null
    qualify row_number() over (partition by city, vehicle_id order by fetched_at desc) = 1
""")

alerts = q("""
    select city_key, sum(alerts_active) as active_alerts
    from fct_alerts_daily
    where service_date >= current_date - 1
    group by 1
""")

if vp.empty:
    empty_state("No vehicle positions in the last 15 minutes — check the ops page.")
    st.stop()

lag = q("""
    select city as city_key,
           round(datediff(second, max(fetched_at), current_timestamp()) / 60.0, 1) as lag_min,
           count(distinct vehicle_id) as vehicles
    from TRANSIT.SILVER.vehicle_positions
    where fetched_at > dateadd(minute, -15, current_timestamp())
    group by 1 order by 1
""")

cols = st.columns(len(lag) if len(lag) else 1)
for col, row in zip(cols, lag.itertuples(), strict=False):
    col.metric(
        city_name(row.city_key),
        f"{int(row.vehicles):,} vehicles",
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
