"""Page 3 — route explorer. Pick a city and route; see its path (drawn through
its ordered observed stops — shapes.txt is not ingested, and the stop chain is an
honest approximation), delay-by-hour heatmap, headway distribution, worst stops.

"Which city is most reliable at 8am" is two clicks on this page: city, then the
hour column of the heatmap."""

import pandas as pd
import pydeck as pdk
import streamlit as st
from lib import CITIES, city_name, delay_color, empty_state, q

st.set_page_config(page_title="Route explorer", page_icon="🚌", layout="wide")
st.title("Route explorer")

city = st.sidebar.selectbox("City", list(CITIES.keys()), format_func=city_name)
if city not in CITIES:  # widget values are user-controlled; belt and braces
    st.stop()

routes = q(
    """
    select r.route_id,
           max(coalesce(dr.route_short_name, r.route_id)) as label,
           max(dr.route_long_name) as long_name,
           max(r.mode) as mode,
           sum(r.banded_events) as banded_events,
           round(100 * sum(r.otp_pct * r.banded_events)
               / nullif(sum(case when r.otp_pct is not null then r.banded_events end), 0), 1)
               as otp_pct
    from fct_route_reliability_daily r
    left join dim_route dr on dr.route_key = r.route_key
    where r.city_key = %(city)s
    group by 1
    having sum(r.banded_events) > 0
    order by banded_events desc
    """,
    {"city": city},
)

if routes.empty:
    empty_state("No scored route-days for this city yet.")
    st.stop()

routes["pick"] = routes.label + " — " + routes.long_name.fillna("").str.slice(0, 40)
route_id = st.sidebar.selectbox(
    "Route", routes.route_id, format_func=lambda r: routes.set_index("route_id").pick.get(r, r)
)
if route_id not in set(routes.route_id):  # reject a manipulated widget value
    st.stop()
sel = routes[routes.route_id == route_id].iloc[0]
st.subheader(f"{city_name(city)} · {sel['pick']} ({sel['mode']}) — OTP {sel.otp_pct}%")

col_map, col_heat = st.columns([1, 1])

with col_map:
    stops = q(
        """
        select e.stop_id, max(s.stop_name) as stop_name,
               max(s.lat) as lat, max(s.lon) as lon,
               avg(e.delay_arr_sec) as mean_delay_sec,
               count(e.otp_band) as scored_events,
               max(e.stop_sequence) as seq
        from fct_stop_events e
        left join dim_stop s on s.stop_key = e.stop_key
        where e.city_key = %(city)s and e.route_id = %(route_id)s
          and e.otp_band is not null
        group by 1
        having max(s.lat) is not null
        order by seq
        """,
        {"city": city, "route_id": route_id},
    )
    if stops.empty:
        empty_state("No geolocated stops (city may not share a stop namespace with its static).")
    else:
        stops["fill"] = stops.mean_delay_sec.map(delay_color)
        path = [[[r.lon, r.lat] for r in stops.itertuples()]]
        st.pydeck_chart(
            pdk.Deck(
                map_style=None,
                initial_view_state=pdk.ViewState(
                    latitude=stops.lat.mean(), longitude=stops.lon.mean(), zoom=11
                ),
                layers=[
                    pdk.Layer(
                        "PathLayer",
                        pd.DataFrame({"path": path}),
                        get_path="path",
                        get_width=30,
                        get_color=[120, 120, 140],
                    ),
                    pdk.Layer(
                        "ScatterplotLayer",
                        stops,
                        get_position=["lon", "lat"],
                        get_fill_color="fill",
                        get_radius=60,
                        pickable=True,
                    ),
                ],
                tooltip={"text": "{stop_name}\n{mean_delay_sec}s mean over {scored_events} events"},
            ),
            height=420,
        )
        st.caption(
            "Path drawn through ordered stops (shapes.txt not ingested); "
            "dots colored by mean delay."
        )

with col_heat:
    heat = q(
        """
        select local_hour, local_dow, round(avg(delay_arr_sec)) as mean_delay_sec
        from fct_stop_events
        where city_key = %(city)s and route_id = %(route_id)s and otp_band is not null
        group by 1, 2
        """,
        {"city": city, "route_id": route_id},
    )
    if heat.empty:
        empty_state("No banded events for a heatmap yet.")
    else:
        pivot = heat.pivot(index="local_hour", columns="local_dow", values="mean_delay_sec")
        st.caption("Mean delay (s) by local hour × day of week")
        st.dataframe(
            pivot.style.background_gradient(cmap="RdYlGn_r", vmin=-60, vmax=300).format(
                "{:+.0f}", na_rep=""
            ),
            use_container_width=True,
            height=560,
        )

st.subheader("Headway distribution and worst stops")
col_h, col_w = st.columns([1, 1])
with col_h:
    gaps = q(
        """
        select round(gap_ratio, 1) as gap_ratio, count(*) as n
        from fct_headways
        where city_key = %(city)s and route_id = %(route_id)s
          and gap_ratio is not null and gap_ratio between 0 and 4
        group by 1 order by 1
        """,
        {"city": city, "route_id": route_id},
    )
    if gaps.empty:
        empty_state("No rated gaps (needs a schedule-matched headway).")
    else:
        st.caption("Actual gap ÷ scheduled headway (1.0 = as promised; <0.5 bunched; >2 big gap)")
        st.bar_chart(gaps.set_index("gap_ratio").n)
with col_w:
    worst = q(
        """
        select max(s.stop_name) as stop, count(e.otp_band) as scored,
               round(avg(e.delay_arr_sec)) as mean_delay_sec,
               round(100 * count(case when e.otp_band in ('late','very_late') then 1 end)
                   / nullif(count(e.otp_band), 0), 1) as late_pct
        from fct_stop_events e
        left join dim_stop s on s.stop_key = e.stop_key
        where e.city_key = %(city)s and e.route_id = %(route_id)s and e.otp_band is not null
        group by e.stop_id
        having count(e.otp_band) >= 20
        order by late_pct desc
        limit 10
        """,
        {"city": city, "route_id": route_id},
    )
    st.caption("Worst stops by late share (≥20 scored events)")
    st.dataframe(worst, hide_index=True, use_container_width=True)
