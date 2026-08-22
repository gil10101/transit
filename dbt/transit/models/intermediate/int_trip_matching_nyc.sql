-- NYC RT trip_id -> static trip_id (docs/01-data-dictionary.md §B quirk ②).
-- RT trip_ids are exactly the origin-time token of static trip_ids
-- (e.g. RT 070950_A..S58R vs static ASP26GEN-..._070950_A..S58R), so the primary
-- match is an exact token equijoin on the active service date. Fallback: same
-- (route, direction-from-suffix) with the nearest origin time within 3 minutes.
-- Origin time comes from the trip_id prefix unconditionally (prefix/100 = minutes
-- after local midnight); direction never comes from direction_id (quirk ⑥).

with rt_trips as (
    select distinct
        city_key,
        service_date,
        trip_id,
        trip_uid,
        route_id,
        cast(substring(trip_id, 1, 6) as integer) as origin_centimin,
        {{ re_extract('trip_id', '\.\.?([NS])', 1) }} as direction_letter
    from {{ ref('stg_gtfsrt__trip_updates') }}
    where city_key = 'nyc' and trip_id is not null
),

static_trips as (
    select
        t.city_key,
        d.service_date,
        t.trip_id as static_trip_id,
        t.route_id,
        t.direction_id,
        t.origin_time_token,
        cast(substring(t.origin_time_token, 1, 6) as integer) as origin_centimin,
        {{ re_extract('t.trip_id', '\.\.?([NS])', 1) }} as direction_letter
    from {{ ref('stg_gtfs__trips') }} t
    join {{ ref('int_service_dates') }} d
      on d.city_key = t.city_key and d.service_id = t.service_id
    where t.city_key = 'nyc'
),

exact as (
    select
        r.city_key, r.service_date, r.trip_id, r.trip_uid, r.route_id,
        s.static_trip_id, s.direction_id,
        1.0 as match_confidence
    from rt_trips r
    join static_trips s
      on s.city_key = r.city_key
     and s.service_date = r.service_date
     and s.origin_time_token = r.trip_id
),

fallback as (
    select
        r.city_key, r.service_date, r.trip_id, r.trip_uid, r.route_id,
        s.static_trip_id, s.direction_id,
        -- same origin minute but different track/path token (e.g. RT ..S07X003 vs
        -- static ..S07R) is a confident match; drifted origin time less so
        case when s.origin_centimin = r.origin_centimin then 0.9 else 0.7 end
            as match_confidence,
        row_number() over (
            partition by r.city_key, r.service_date, r.trip_id
            order by abs(s.origin_centimin - r.origin_centimin)
        ) as rn
    from rt_trips r
    join static_trips s
      on s.city_key = r.city_key
     and s.service_date = r.service_date
     and s.route_id = r.route_id
     and s.direction_letter = r.direction_letter
     and abs(s.origin_centimin - r.origin_centimin) <= 300  -- 3 minutes, in centiminutes
    where r.trip_id not in (select trip_id from exact)
)

select city_key, service_date, trip_id, trip_uid, route_id,
       static_trip_id, direction_id, match_confidence
from exact
union all
select city_key, service_date, trip_id, trip_uid, route_id,
       static_trip_id, direction_id, match_confidence
from fallback
where rn = 1
