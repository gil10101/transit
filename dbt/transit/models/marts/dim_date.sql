-- Calendar spine, 2024-01-01 .. 2027-12-31: covers the 2-year weather backfill
-- window behind us and every published GTFS calendar ahead of us. Regenerated
-- deterministically each build — no state, nothing to drift.
--
-- iso_dow is 1=Monday..7=Sunday on BOTH dialects (duckdb isodow / Snowflake
-- dayofweekiso); plain dayofweek is week-start-configurable on Snowflake, which is
-- exactly the sort of silent environment dependency this warehouse does not keep.

{{ config(materialized='table') }}

with spine as (

    {% if target.type == 'snowflake' %}
    select dateadd(day, row_number() over (order by seq4()) - 1, to_date('2024-01-01')) as date_day
    from table(generator(rowcount => 1461))
    {% else %}
    select cast(gs.d as date) as date_day
    from generate_series(timestamp '2024-01-01', timestamp '2027-12-31', interval 1 day) gs(d)
    {% endif %}

)

select
    date_day,
    extract(year from date_day)  as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    extract(day from date_day)   as day_of_month,
    {% if target.type == 'snowflake' %}
    dayofweekiso(date_day) as iso_dow,
    {% else %}
    isodow(date_day) as iso_dow,
    {% endif %}
    {% if target.type == 'snowflake' %}
    dayofweekiso(date_day) >= 6 as is_weekend
    {% else %}
    isodow(date_day) >= 6 as is_weekend
    {% endif %}
from spine
