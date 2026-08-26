"""Page 2 — delay hexmap. Stop-level mean delay aggregated to H3 r8 hexagons
(computed at build time on dim_stop, so this is a plain GROUP BY), rendered as
small multiples on ONE shared color scale — the whole point is that Zurich pale
and someone else glowing red are readable at a glance as the same units."""

import pydeck as pdk
import streamlit as st
from lib import CITIES, delay_color, empty_state, q

st.set_page_config(page_title="Delay map", page_icon="🗺️", layout="wide")
st.title("Where the delay lives")

days = st.sidebar.slider("Closed days to include", 1, 30, 7)
mode = st.sidebar.selectbox("Mode", ["all", "bus", "tram", "metro", "rail", "ferry"])

hexes = q(
    """
    select e.city_key, s.h3_r8,
           avg(e.delay_arr_sec) as mean_delay_sec,
           count(*) as events
    from fct_stop_events e
    join dim_stop s on s.stop_key = e.stop_key
    where e.otp_band is not null
      and s.h3_r8 is not null
      and e.service_date >= dateadd(day, -%(days)s, current_date)
      and (%(mode)s = 'all' or e.mode = %(mode)s)
    group by 1, 2
    having count(*) >= 20
    """,
    {"days": int(days), "mode": mode},
)

st.caption(
    f"Mean arrival delay per hexagon over the last {days} days, scored events only "
    "(no ADDED trips, no skipped stops, no stale echoes — same filters as OTP). "
    "Hexes with under 20 events are dropped. One color scale for every city: "
    "blue = early, green = on time, red = 5+ minutes late."
)

if hexes.empty:
    empty_state("No hex data — either no closed days in range or dim_stop has no H3 (duckdb dev).")
else:
    hexes["fill"] = hexes.mean_delay_sec.map(delay_color)
    cols = st.columns(2)
    for i, (key, meta) in enumerate(CITIES.items()):
        sub = hexes[hexes.city_key == key]
        if sub.empty:
            continue
        with cols[i % 2]:
            st.subheader(meta["name"])
            st.caption(
                f"{len(sub)} hexes · {int(sub.events.sum()):,} events · "
                f"city mean {sub.mean_delay_sec.mul(sub.events).sum() / sub.events.sum():+.0f}s"
            )
            st.pydeck_chart(
                pdk.Deck(
                    map_style=None,
                    initial_view_state=pdk.ViewState(
                        latitude=meta["center"][0],
                        longitude=meta["center"][1],
                        zoom=meta["zoom"] - 1,
                    ),
                    layers=[
                        pdk.Layer(
                            "H3HexagonLayer",
                            sub,
                            get_hexagon="h3_r8",
                            get_fill_color="fill",
                            pickable=True,
                            extruded=False,
                        )
                    ],
                    tooltip={"text": "{mean_delay_sec}s mean · {events} events"},
                ),
                height=380,
            )
