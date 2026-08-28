"""Page 5 — ops. Reliability of the pipeline itself: feed freshness, completeness
by city and day, and when each gold table last actually changed (the deploy trap
detector — a green chain with a stale gold is the failure mode this project has
actually had)."""

import streamlit as st
from lib import brand, city_name, q

st.set_page_config(page_title="Ops", layout="wide")
brand()
st.title("Pipeline ops")

st.subheader("Feed freshness (silver, minutes since last prediction)")
fresh = q("""
    select city as city_key,
           round(datediff(second, max(fetched_at), current_timestamp()) / 60.0, 1) as pred_lag_min,
           count(*) as rows_last_hour
    from TRANSIT.SILVER.stop_time_predictions
    where fetched_at > dateadd(hour, -4, current_timestamp())
    group by 1 order by 1
""")
fresh["city"] = fresh.city_key.map(city_name)
st.caption(
    "This measures the SNOWFLAKE VIEW of silver, which advances when the 2-hourly "
    "chain re-pins the Iceberg metadata — so up to ~2h of lag here is design, not "
    "outage (physical drains land hourly; the raw-side tripwire alarms at 40 min "
    "independently). Investigate at 150+ min: that means a chain or drain actually "
    "missed. Chasing 80-minute 'staleness' here cost a 25-minute ghost hunt on "
    "2026-08-26 — read this caption before repeating it."
)
st.dataframe(
    fresh[["city", "pred_lag_min", "rows_last_hour"]].style.map(
        lambda v: "background-color: #8b1e1e" if isinstance(v, float) and v > 150 else "",
        subset=["pred_lag_min"],
    ),
    hide_index=True,
    use_container_width=True,
)

st.subheader("Completeness by city and closed day (service-weighted)")
comp = q("""
    select city_key, service_date,
           round(100 * sum(completeness_pct * trips_scheduled)
               / nullif(sum(trips_scheduled), 0), 1) as completeness_pct
    from fct_service_delivery_daily
    where service_day_closed and service_date >= metrics_from
    group by 1, 2
""")
if comp.empty:
    st.caption("no closed judged days yet")
else:
    pivot = comp.pivot(index="city_key", columns="service_date", values="completeness_pct")
    pivot.index = [city_name(c) for c in pivot.index]
    st.dataframe(
        pivot.style.background_gradient(cmap="RdYlGn", vmin=50, vmax=100).format(
            "{:.1f}", na_rep=""
        ),
        use_container_width=True,
    )

st.subheader("Gold table staleness")
st.caption(
    "last_altered per table. A committed dbt change only reaches these through the "
    "baked dagster image — if the chain reports SUCCESS but these timestamps stop "
    "moving, the image is stale (it has happened; docs/08 F-series)."
)
stale = q("""
    select table_name, row_count,
           round(datediff(minute, last_altered, current_timestamp()) / 60.0, 1)
               as hours_since_altered
    from TRANSIT.information_schema.tables
    where table_schema = 'GOLD' and table_type = 'BASE TABLE'
    order by hours_since_altered desc
""")
st.dataframe(stale, hide_index=True, use_container_width=True, height=560)
