-- Tokyo rail stop events from train-grain ODPT snapshots (docs/01 §C, docs/03).
-- Grain matches int_stop_events_finalized: (city_key, service_date, trip_uid,
-- stop_sequence). The union into that model happens THERE; this model owns the
-- odpt mechanics:
--
--   * schedule = odpt:TrainTimetable exploded to stop grain in silver
--     (stop_sequence = trainTimetableObject ordinal; seconds already
--     midnight-reconstructed by spark_jobs/odpt_static_parse.py)
--   * RT->schedule match: (railway_urn, train_number, calendar) — exact ids,
--     confidence 1.0. Calendar resolves from the service date's ISO dow with a
--     per-timetable priority (Sat -> Saturday else SaturdayHoliday; Sun ->
--     Holiday else SaturdayHoliday). KNOWN GAP: Japanese national holidays on
--     weekdays run holiday schedules but classify as Weekday here — a jp
--     holiday seed upgrades this when the MLIT benchmark lands (docs/01 §C.4).
--   * passage detection is TRANSITION-BASED (docs/01 §C.1: toStation-null is
--     unreliable): a stop counts as reached at the FIRST snapshot whose
--     fromStation sits at-or-past the stop along the trip's direction
--     (odpt:Railway stationOrder index, sign from ascending/descending
--     direction). Stops with no at-or-past snapshot yield no event — a train
--     still short of a stop has nothing to finalize.
--   * delay_arr_sec = that snapshot's operator-stated odpt:delay
--     (finalization_method 'odpt_stated'), held to the same plausibility bound
--     as every other city; the Arakawa tram states no delay, so its events are
--     service volume with null delay — exactly the honest state.
--   * snapshots past their dct:valid guarantee window never finalize a stop.
--
-- Oedo quirk: Tochomae appears twice in stationOrder (the line is a 6-shape,
-- not a loop); station positions aggregate to the FIRST index per direction,
-- so second-visit passage finalizes one stop early at worst — bounded by the
-- 30s poll cadence. Zero rows until the tokyo poller deploys; every join
-- tolerates that (docs/03 convention).

with trains as (
    select *
    from {{ ref('stg_odpt__trains') }}
    where operator = 'Toei'
      and (valid_until_ts_utc is null or fetched_at <= valid_until_ts_utc)
),

railway_dirs as (
    select distinct railway_urn, ascending_direction, descending_direction
    from {{ ref('stg_odpt__railway') }}
),

-- one position per (railway, station): min index (Oedo/Tochomae note above)
station_pos as (
    select railway_urn, station_urn, min(station_index) as station_index
    from {{ ref('stg_odpt__railway') }}
    where station_urn is not null
    group by 1, 2
),

-- distinct observed trips with their direction sign
trips as (
    select
        t.city_key,
        t.service_date,
        t.trip_uid,
        t.trip_id,
        t.train_number,
        t.railway_urn,
        t.rail_direction_urn,
        case
            when t.rail_direction_urn = d.ascending_direction then 1
            when t.rail_direction_urn = d.descending_direction then -1
        end as dir_sign,
        {% if target.type == 'snowflake' %}
        dayofweekiso(t.service_date) as iso_dow,
        {% else %}
        isodow(t.service_date) as iso_dow,
        {% endif %}
        min(t.fetched_at) as first_seen_utc,
        max(t.fetched_at) as last_seen_utc,
        count(*) as snapshot_count
    from trains t
    join railway_dirs d on d.railway_urn = t.railway_urn
    group by all
),

-- calendar candidates by day type, priority-ranked per trip
calendar_pick as (
    select
        tr.*,
        tt.timetable_urn,
        row_number() over (
            partition by tr.city_key, tr.service_date, tr.trip_uid
            order by case split_part(tt.calendar_urn, ':', 2)
                when 'Weekday' then 1
                when 'Saturday' then case when tr.iso_dow = 6 then 1 else 3 end
                when 'Holiday' then case when tr.iso_dow = 7 then 1 else 3 end
                when 'SaturdayHoliday' then 2
                else 9 end,
                tt.timetable_urn  -- deterministic tie-break for .1/.2 suffixes
        ) as cal_rank
    from trips tr
    join (
        select distinct timetable_urn, railway_urn, train_number, calendar_urn
        from {{ ref('stg_odpt__train_timetable') }}
    ) tt
      on tt.railway_urn = tr.railway_urn
     and tt.train_number = tr.train_number
     and (
        (tr.iso_dow between 1 and 5 and split_part(tt.calendar_urn, ':', 2) = 'Weekday')
        or (tr.iso_dow = 6
            and split_part(tt.calendar_urn, ':', 2) in ('Saturday', 'SaturdayHoliday'))
        or (tr.iso_dow = 7
            and split_part(tt.calendar_urn, ':', 2) in ('Holiday', 'SaturdayHoliday'))
     )
),

matched as (
    select * from calendar_pick where cal_rank = 1
),

sched as (
    select
        m.city_key,
        m.service_date,
        m.trip_uid,
        m.trip_id,
        m.railway_urn,
        m.dir_sign,
        m.timetable_urn,
        m.first_seen_utc,
        m.last_seen_utc,
        m.snapshot_count,
        s.stop_sequence,
        s.station_urn,
        s.arrival_seconds,
        s.departure_seconds,
        sp.station_index * m.dir_sign as stop_pos
    from matched m
    join {{ ref('stg_odpt__train_timetable') }} s on s.timetable_urn = m.timetable_urn
    left join station_pos sp
      on sp.railway_urn = m.railway_urn and sp.station_urn = s.station_urn
),

-- first snapshot at-or-past each scheduled stop, along the trip direction
reached as (
    select
        sc.*,
        min(t.fetched_at) as reached_at_utc,
        min_by(t.delay_sec, t.fetched_at) as stated_delay_sec
    from sched sc
    join trains t
      on t.city_key = sc.city_key
     and t.service_date = sc.service_date
     and t.trip_uid = sc.trip_uid
    join station_pos fp
      on fp.railway_urn = sc.railway_urn and fp.station_urn = t.from_station_urn
    where sc.stop_pos is not null
      and fp.station_index * sc.dir_sign >= sc.stop_pos
    group by all
),

version as (
    select max(gtfs_version_id) as gtfs_version_id
    from {{ ref('stg_gtfs__stops') }}
    where city_key = 'tokyo'
),

bounded as (
    select
        r.*,
        -- same plausibility rule as every other city: an impossible stated
        -- value carries no information and is treated as absent (CLAUDE.md)
        case when abs(r.stated_delay_sec) <= {{ var('max_plausible_delay_sec') }}
             then r.stated_delay_sec end as bounded_delay,
        {{ scheduled_ts_utc('r.service_date', 'r.arrival_seconds', "'Asia/Tokyo'") }}
            as sched_arr_ts_utc,
        {{ scheduled_ts_utc('r.service_date', 'r.departure_seconds', "'Asia/Tokyo'") }}
            as sched_dep_ts_utc
    from reached r
)

select
    b.city_key,
    b.service_date,
    b.trip_uid,
    b.stop_sequence,
    b.trip_id as trip_id_raw,
    b.timetable_urn as static_trip_id,
    map.route_id,
    -- ascending = 0, descending = 1 (stable within the city; the GTFS
    -- direction_id convention for Toei is unverified — consistent grouping is
    -- what headways/OTP need, not the operator's own labeling)
    case b.dir_sign when 1 then 0 when -1 then 1 end as direction_id,
    map.stop_id,
    cast(null as varchar) as vehicle_id,
    1.0 as match_confidence,
    v.gtfs_version_id,
    cast(null as integer) as timepoint,
    b.sched_arr_ts_utc,
    b.sched_dep_ts_utc,
    -- actual = schedule + operator-stated delay (the delay_plus_schedule family;
    -- Tokyo's method label is odpt_stated because the delay is authoritative)
    {{ dbt.dateadd('second', 'b.bounded_delay', 'b.sched_arr_ts_utc') }} as actual_arr_ts_utc,
    {{ dbt.dateadd('second', 'b.bounded_delay', 'b.sched_dep_ts_utc') }} as actual_dep_ts_utc,
    b.bounded_delay as delay_arr_sec,
    b.bounded_delay as delay_dep_sec,
    'SCHEDULED' as schedule_relationship,
    false as cancelled_flag,
    false as skipped_flag,
    'odpt_stated' as finalization_method,
    b.snapshot_count as prediction_count,
    b.first_seen_utc,
    b.last_seen_utc,
    'odpt_json' as source_format
from bounded b
join {{ ref('int_odpt_stop_map') }} map on map.station_urn = b.station_urn
cross join version v
