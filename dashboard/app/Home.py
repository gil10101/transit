"""Page 1 — the scorecard. The headline number, shown with the same honesty the
model enforces: fct_city_scorecard refuses to score a city below 20 closed judged
days across its whole judged window (window grain since 2026-09-10 — the month
grain could never hold 20 days between metrics_from and the Sep 17 close), so
until a city crosses that bar this page shows PROGRESS toward a score, clearly
labeled provisional, rather than dressing thin history up as a ranking."""

import streamlit as st
from lib import CITIES, brand, city_name, q

st.set_page_config(page_title="Transit Pulse", layout="wide")
brand()

st.title("Transit Pulse — city reliability scorecard")
st.caption(
    "Which cities run the most reliable public transit — measured, not asserted. "
    "8 cities · 5 countries · live agency feeds scored against each agency's own "
    "schedule. Tokyo's delay is operator-stated and rounded to the minute — its "
    "on-time rate is not measured the same way as the other seven (docs/06)."
)

scores = q("""
    select city_key, window_start, window_end, score_0_100, s_wait, s_otp,
           s_cancel, s_bunch, frequent_service_share, completeness_pct,
           judged_days, excluded_route_days, methodology_version
    from fct_city_scorecard
    order by score_0_100 desc
""")

progress = q("""
    select d.city_key,
           count(distinct case when d.service_day_closed
                                and d.service_date >= d.metrics_from
                               then d.service_date end) as judged_days
    from fct_service_delivery_daily d
    group by 1
    order by 2 desc, 1
""")

MIN_DAYS = 20  # mirrors var('scorecard_min_judged_days'); the model is the authority

census = q("""
    select count(*) as stop_events,
           count(distinct service_date) as service_days,
           count(distinct city_key) as cities,
           max(service_date) as latest_day
    from fct_stop_events
    where otp_band is not null
""").iloc[0]
trips = q("""
    select sum(trips_observed) as observed, sum(trips_scheduled) as scheduled
    from fct_service_delivery_daily
""").iloc[0]

st.subheader("Warehouse")
w = st.columns(5)
w[0].metric("Trips observed", f"{int(trips.observed):,}")
w[1].metric("Stop events scored", f"{census.stop_events / 1e6:.1f}M")
w[2].metric("Cities in gold", f"{int(census.cities)}")
w[3].metric("Service days", f"{int(census.service_days)}")
w[4].metric("Latest service day", f"{census.latest_day:%b %d}")

if scores.empty:
    st.subheader("Progress toward a scoreable month")
    st.caption(
        f"A score requires {MIN_DAYS} closed judged days in a month — the scorecard "
        "refuses to rank cities on less. Until then: progress, and provisional "
        "metrics that are not scores."
    )
    cols = st.columns(len(CITIES))
    for col, (key, meta) in zip(cols, CITIES.items(), strict=True):
        row = progress[progress.city_key == key]
        days = int(row.judged_days.iloc[0]) if not row.empty else 0
        col.metric(meta["name"], f"{days} / {MIN_DAYS}")

    st.subheader("Provisional metrics (service-weighted, closed judged days only)")
    st.caption(
        "Same inputs the score will use, shown raw. Small day counts — treat as a "
        "smoke test of the pipeline, not a comparison of cities."
    )
    prov = q("""
        with eligible as (
            select d.city_key, d.route_id, d.service_date,
                   d.trips_scheduled, d.trips_observed, d.trips_cancelled,
                   d.completeness_pct
            from fct_service_delivery_daily d
            where d.service_day_closed
              and d.service_date >= d.metrics_from
              and coalesce(d.completeness_pct, 0) >= 0.50
        )
        select
            e.city_key,
            count(distinct e.service_date) as judged_days,
            sum(e.trips_observed) as observed_trips,
            round(100 * sum(r.otp_pct * r.banded_events)
                / nullif(sum(case when r.otp_pct is not null then r.banded_events end), 0), 1)
                as otp_pct,
            round(sum(r.ewt_sec * r.ewt_gap_count)
                / nullif(sum(case when r.ewt_sec is not null then r.ewt_gap_count end), 0))
                as ewt_sec,
            round(100 * sum(r.bunching_pct * r.rated_gaps)
                / nullif(sum(case when r.bunching_pct is not null then r.rated_gaps end), 0), 1)
                as bunching_pct,
            round(100 * cast(sum(e.trips_cancelled) as double)
                / nullif(sum(e.trips_scheduled), 0), 2) as cancel_pct
        from eligible e
        left join fct_route_reliability_daily r
          on r.city_key = e.city_key and r.route_id = e.route_id
         and r.service_date = e.service_date
        group by 1
        order by otp_pct desc
    """)
    prov["city"] = prov.city_key.map(city_name)
    st.dataframe(
        prov[
            [
                "city",
                "judged_days",
                "observed_trips",
                "otp_pct",
                "ewt_sec",
                "bunching_pct",
                "cancel_pct",
            ]
        ],
        hide_index=True,
        use_container_width=True,
    )

else:
    m = scores.sort_values("score_0_100", ascending=False)
    m["city"] = m.city_key.map(city_name)
    lo, hi = m.window_start.min(), m.window_end.max()

    st.subheader(
        f"Composite score, {lo:%b %d} - {hi:%b %d} "
        f"(methodology v{m.methodology_version.iloc[0]})"
    )
    st.caption(
        "Each city is scored over its own judged window - window_start differs per "
        "city because metrics_from does. judged_days on every row is the evidence count."
    )
    st.bar_chart(m.set_index("city").score_0_100)

    st.subheader("Component breakdown")
    st.caption(
        "Sub-scores are 0-100, higher is better; a blank sub-score took no part in "
        "that city's composite (its input was never measured, and its weight was "
        "renormalised away rather than faked). frequent_service_share is why two "
        "cities are not directly comparable — it is the share of service judged on "
        "wait-time rather than timetable."
    )
    st.dataframe(
        m[
            [
                "city",
                "score_0_100",
                "s_wait",
                "s_otp",
                "s_cancel",
                "s_bunch",
                "frequent_service_share",
                "completeness_pct",
                "judged_days",
                "excluded_route_days",
            ]
        ],
        hide_index=True,
        use_container_width=True,
    )
