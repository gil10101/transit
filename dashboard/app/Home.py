"""Page 1 — the scorecard. The headline number, shown with the same honesty the
model enforces: fct_city_scorecard_monthly refuses to score a city below 20 closed
judged days, so until a city crosses that bar this page shows PROGRESS toward a
score, clearly labeled provisional, rather than dressing 4 days up as a ranking."""

import streamlit as st
from lib import CITIES, city_name, empty_state, q

st.set_page_config(page_title="Transit Pulse", page_icon="🚇", layout="wide")

st.title("Transit Pulse — city reliability scorecard")

scores = q("""
    select city_key, month, score_0_100, s_wait, s_otp, s_cancel, s_bunch,
           frequent_service_share, completeness_pct, judged_days,
           excluded_route_days, methodology_version
    from fct_city_scorecard_monthly
    order by month desc, score_0_100 desc
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

if scores.empty:
    empty_state(
        f"No city is scored yet — a score requires {MIN_DAYS} closed judged service "
        "days in a month and the warehouse is still accruing history. That is the "
        "scorecard working, not failing: 4 days dressed up as a 0-100 would be "
        "confident nonsense. Below is progress toward the bar, and provisional "
        "metrics that are NOT scores."
    )

    st.subheader("Progress toward a scoreable month")
    cols = st.columns(len(CITIES))
    for col, (key, meta) in zip(cols, CITIES.items(), strict=True):
        row = progress[progress.city_key == key]
        days = int(row.judged_days.iloc[0]) if not row.empty else 0
        with col:
            st.metric(meta["name"], f"{days} / {MIN_DAYS} days")
            st.progress(min(1.0, days / MIN_DAYS))

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
    months = sorted(scores.month.unique(), reverse=True)
    month = st.selectbox("Month", months)
    m = scores[scores.month == month].sort_values("score_0_100", ascending=False)
    m["city"] = m.city_key.map(city_name)

    st.subheader(f"Composite score, {month} (methodology v{m.methodology_version.iloc[0]})")
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
