-- Scheduled service frequency per (city, route, direction, daypart, service_date):
-- gaps between consecutive scheduled arrivals at each stop, then the median gap
-- across the slice. is_frequent (median <= freq_headway_threshold_sec) routes a
-- slice to EWT scoring; scheduled slices are scored by OTP instead (docs/02 §int_service_frequency).
-- A gap belongs to the daypart of the later arrival, in city-local time.

with sched as (

    select
        s.city_key,
        s.service_date,
        s.route_id,
        coalesce(s.direction_id, -1) as direction_id,  -- match the -1 fold used in facts
        s.stop_id,
        s.sched_arr_ts_utc,
        c.iana_tz
    from {{ ref('int_scheduled_stop_times') }} s
    join {{ ref('dim_city') }} c on c.city_key = s.city_key
    where s.sched_arr_ts_utc is not null

),

gaps as (

    select
        city_key,
        service_date,
        route_id,
        direction_id,
        {{ daypart("extract(hour from " ~ to_local('sched_arr_ts_utc', 'iana_tz') ~ ")") }} as daypart,
        {{ seconds_between(
            "lag(sched_arr_ts_utc) over (
                partition by city_key, route_id, direction_id, stop_id, service_date
                order by sched_arr_ts_utc)",
            "sched_arr_ts_utc") }} as gap_sec
    from sched

)

select
    md5(concat_ws('|', city_key, route_id, direction_id, daypart, service_date)) as frequency_key,
    city_key,
    route_id,
    direction_id,
    daypart,
    service_date,
    cast(percentile_cont(0.5) within group (order by gap_sec) as integer) as median_sched_headway_sec,
    count(*) as n_sched_gaps,
    cast(percentile_cont(0.5) within group (order by gap_sec)
         <= {{ var('freq_headway_threshold_sec') }} as boolean) as is_frequent
from gaps
where gap_sec is not null and gap_sec > 0
group by city_key, route_id, direction_id, daypart, service_date
