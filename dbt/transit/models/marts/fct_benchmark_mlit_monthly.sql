-- Tokyo-only validation fact (docs/02): MLIT delay-certificate benchmark vs our
-- measured delays, grain (odpt_railway_urn, month). The seed is filled manually
-- each quarter from the MLIT publication (docs/01 §C.5) and is a skeleton until
-- then — an empty seed yields an empty fact, which is the honest state. The
-- measured side comes from tokyo's odpt_stated stop events, so the two columns
-- compare the operator's own certificates against what we observed live.

with measured as (
    select
        m.railway_urn as odpt_railway_urn,
        {{ dbt.date_trunc('month', 'e.service_date') }} as month_start,
        count(distinct e.service_date) as observed_days,
        count(e.delay_arr_sec) as scored_events,
        {{ dbt.safe_cast('median(e.delay_arr_sec)', 'double') }} / 60.0
            as our_measured_p50_delay_min
    from {{ ref('fct_stop_events') }} e
    join (
        select distinct route_id, railway_urn from {{ ref('int_odpt_stop_map') }}
    ) m on m.route_id = e.route_id
    where e.city_key = 'tokyo'
    group by 1, 2
)

select
    b.odpt_railway_urn || '|' || b.month as benchmark_key,
    b.odpt_railway_urn,
    b.month,
    b.line_name,
    b.delay_certificate_days,
    b.avg_delay_minutes as mlit_avg_delay_minutes,
    b.source_url,
    m.observed_days,
    m.scored_events,
    m.our_measured_p50_delay_min
from {{ ref('mlit_tokyo_benchmark') }} b
left join measured m
  on m.odpt_railway_urn = b.odpt_railway_urn
 and {{ dbt.safe_cast('m.month_start', 'varchar') }} like b.month || '%'
