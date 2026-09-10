"""Page 1 — when transit fails. City-level patterns the other pages don't show:
the day-by-day trend, the hour-of-day shape, weekday vs weekend, mode splits,
and the early-departure rule. Every number is gated the same way the scorecard
gates: closed service days, at or past metrics_from, route-days at >= 50%
completeness — the same `eligible` shape Home uses, so a number here never
disagrees with the scorecard page over what counts.

Tokyo caveat repeated wherever it ranks: its delay is operator-stated and
minute-rounded (docs/01 §C.1), so sub-minute lateness reads as on-time there
by construction."""

import streamlit as st
from lib import CITIES, brand, city_name, q

st.set_page_config(page_title="Patterns", layout="wide")
brand()
st.title("When transit fails")

HEX = {k: "#{:02x}{:02x}{:02x}".format(*v["color"]) for k, v in CITIES.items()}

ELIGIBLE = """
    with eligible as (
        select d.city_key, d.route_id, d.service_date
        from fct_service_delivery_daily d
        where d.service_day_closed
          and d.service_date >= d.metrics_from
          and coalesce(d.completeness_pct, 0) >= 0.50
    )
"""

st.subheader("On-time share by service day")
st.caption(
    "Service-weighted across each city's eligible route-days (closed, judged, "
    ">=50% complete). A dip is real — either service failed or an incident day "
    "took coverage with it; incident days are seeded and excluded from scoring "
    "but shown here, because hiding them would fake a smoother line."
)
trend = q(
    ELIGIBLE
    + """
    select r.city_key, r.service_date,
           round(100 * sum(r.otp_pct * r.banded_events)
               / nullif(sum(case when r.otp_pct is not null then r.banded_events end), 0), 1)
               as otp_pct
    from fct_route_reliability_daily r
    join eligible e
      on e.city_key = r.city_key and e.route_id = r.route_id
     and e.service_date = r.service_date
    group by 1, 2
    order by 2
"""
)
if trend.empty:
    st.info("No judged days yet.")
else:
    wide = trend.pivot(index="service_date", columns="city_key", values="otp_pct")
    color = [HEX[k] for k in wide.columns]
    wide.columns = [city_name(c) for c in wide.columns]
    st.line_chart(wide, color=color)

st.subheader("On-time share by local hour")
st.caption(
    "3am Tokyo compares to 3am New York (dim_city.iana_tz — the locked rule). "
    "Cells with under 200 scored events are blanked rather than shown noisy."
)
hourly = q(
    ELIGIBLE
    + """
    select f.city_key, f.local_hour,
           count(case when f.otp_band = 'on_time' then 1 end) as on_time,
           count(f.otp_band) as banded
    from fct_stop_events f
    join eligible e
      on e.city_key = f.city_key and e.route_id = f.route_id
     and e.service_date = f.service_date
    where f.otp_band is not null
    group by 1, 2
"""
)
if hourly.empty:
    st.info("No banded events yet.")
else:
    hourly["otp"] = (100 * hourly.on_time / hourly.banded).where(hourly.banded >= 200)
    hp = hourly.pivot(index="city_key", columns="local_hour", values="otp")
    hp.index = [city_name(c) for c in hp.index]
    st.dataframe(
        hp.style.background_gradient(cmap="RdYlGn", vmin=50, vmax=100, axis=None).format(
            "{:.0f}", na_rep=""
        ),
        use_container_width=True,
    )

col_dow, col_mode = st.columns(2)

with col_dow:
    st.subheader("Weekday vs weekend")
    dow = q(
        ELIGIBLE
        + """
        select f.city_key,
               case when dayofweekiso(f.service_date) >= 6 then 'weekend'
                    else 'weekday' end as day_type,
               round(100 * count(case when f.otp_band = 'on_time' then 1 end)
                   / nullif(count(f.otp_band), 0), 1) as otp_pct
        from fct_stop_events f
        join eligible e
          on e.city_key = f.city_key and e.route_id = f.route_id
         and e.service_date = f.service_date
        where f.otp_band is not null
        group by 1, 2
    """
    )
    if dow.empty:
        st.info("No banded events yet.")
    else:
        dp = dow.pivot(index="city_key", columns="day_type", values="otp_pct")
        dp["weekend edge"] = (dp.get("weekend") - dp.get("weekday")).round(1)
        dp.index = [city_name(c) for c in dp.index]
        st.dataframe(dp.sort_values("weekend edge", ascending=False), use_container_width=True)

with col_mode:
    st.subheader("By mode")
    st.caption("Modes a city actually publishes realtime for — coverage gaps in docs/06.")
    # mode lives on dim_route, not the fact (fct_stop_events header: model-only
    # columns never reached the prod relation) — same join modes.json uses
    modes = q(
        ELIGIBLE
        + """
        select f.city_key, dr.mode,
               round(100 * count(case when f.otp_band = 'on_time' then 1 end)
                   / nullif(count(f.otp_band), 0), 1) as otp_pct,
               count(f.otp_band) as scored
        from fct_stop_events f
        join dim_route dr on dr.route_key = f.route_key
        join eligible e
          on e.city_key = f.city_key and e.route_id = f.route_id
         and e.service_date = f.service_date
        where f.otp_band is not null and dr.mode is not null
        group by 1, 2
        having count(f.otp_band) >= 1000
    """
    )
    if modes.empty:
        st.info("No banded events yet.")
    else:
        mp = modes.pivot(index="city_key", columns="mode", values="otp_pct")
        mp.index = [city_name(c) for c in mp.index]
        st.dataframe(
            mp.style.background_gradient(cmap="RdYlGn", vmin=50, vmax=100, axis=None).format(
                "{:.0f}", na_rep=""
            ),
            use_container_width=True,
        )

st.subheader("Early departures at timepoints")
st.caption(
    "Locked rule: a bus leaving a scheduled timepoint more than 60s early is a "
    "service failure — the rider it strands is the one who arrived on time. Only "
    "cities whose statics carry timepoints appear; the share is of bus timepoint "
    "departures with a measured departure delay."
)
# timepoint never reached the prod fact relation; it lives on the finalized
# int table at the same grain, so the denominator joins through it while the
# flag itself stays the fact's (single authority for the locked rule)
early = q(
    ELIGIBLE
    + """
    select f.city_key,
           round(100 * count(case when f.early_departure_flag then 1 end)
               / count(*), 1) as early_dep_pct,
           count(*) as measured
    from fct_stop_events f
    join dim_route dr on dr.route_key = f.route_key
    join int_stop_events_finalized i
      on i.city_key = f.city_key and i.service_date = f.service_date
     and i.trip_uid = f.trip_uid and i.stop_sequence = f.stop_sequence
    join eligible e
      on e.city_key = f.city_key and e.route_id = f.route_id
     and e.service_date = f.service_date
    where dr.mode = 'bus' and i.timepoint = 1 and f.delay_dep_sec is not null
    group by 1
    having count(*) >= 1000
    order by early_dep_pct desc
"""
)
if early.empty:
    st.info("No timepoint-bearing bus events yet.")
else:
    early["city"] = early.city_key.map(city_name)
    st.bar_chart(early.set_index("city").early_dep_pct)
