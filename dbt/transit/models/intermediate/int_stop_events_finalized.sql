-- THE model: prediction snapshots -> one finalized stop event per
-- (city_key, service_date, trip_uid, stop_sequence). Two finalization methods:
--   * last_prediction — the last observed prediction before the vehicle plausibly
--     passed the stop stands in for the actual (NYC VP status is unreliable,
--     dictionary quirk ③, so passage detection is not used).
--   * delay_plus_schedule [rev 2026-08-27] — for feeds that publish delay-only
--     StopTimeEvents (Zurich: 97% of rows carry arr_delay_sec and no timestamp),
--     the actual is schedule + stated delay. Tokyo's odpt_stated is this family.
--
-- Mechanics:
--   * ignore predictions fetched > prediction_staleness_min after their own
--     predicted event time (stale revisions of a passed stop)
--   * finalize only events at least finalize_horizon_min behind the freshest
--     fetched_at in the data (still-active stops keep accumulating revisions);
--     data-driven watermark keeps the model replayable
--   * schedule context comes via int_trip_matching (all cities, unioned) ->
--     int_gtfs_scheduled_stop_times; the matcher joins on trip_uid because HSL
--     trip_ids are empty. stop_sequence falls back to the static one (NYC quirk ⑤,
--     and HSL omits stop_sequence on nearly all STUs); nearest schedule row wins
--     if a trip serves a stop twice
--   * delay_arr_sec = COALESCE(feed delay, actual - scheduled)  (canonical rule;
--     a city that states arrival.delay flows through the first branch automatically)
--   * direction_id: NYC from the matcher else the ..N/..S trip_id suffix (quirk ⑥ —
--     never the feed's direction_id); every other city from the feed's direction_id
--     else the matched static trip's

-- [rev 2026-08-25] on_schema_change matches fct_stop_events. dbt's default is
-- 'ignore', which silently computes a new column in the SELECT and never adds it to
-- the existing relation — exactly how stale_observation_flag once failed its own
-- test. Three sibling incrementals carried the guard; this one did not.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    unique_key=['city_key', 'service_date', 'trip_uid', 'stop_sequence']
) }}

with preds as (
    select
        p.*,
        m.static_trip_id,
        m.route_id as matched_route_id,
        m.direction_id as matched_direction_id,
        m.match_confidence,
        coalesce(p.arr_pred_ts_utc, p.dep_pred_ts_utc) as event_pred_ts
    from {{ ref('stg_gtfsrt__trip_updates') }} p
    left join {{ ref('int_trip_matching') }} m
      on m.city_key = p.city_key
     and m.service_date = p.service_date
     and m.trip_uid = p.trip_uid
    -- [rev 2026-08-27] A prediction is EITHER an absolute time OR a delay —
    -- GTFS-RT allows delay-only StopTimeEvents and Zurich's national feed uses
    -- them almost exclusively (97% of its 4.1M rows/day carry arr_delay_sec and
    -- no timestamp). The old filter demanded a timestamp and silently threw the
    -- whole city away: 38k RT trips/day survived silver, matched the static at
    -- 99.5%, and then 1.1k reached gold. Caught by completeness_above_error_50pct
    -- on Zurich's FIRST judged day (410 route-days at 0.7%). Delay-only rows are
    -- admitted here and get their timestamps synthesized from schedule + delay
    -- after the static join below; rows with neither time nor delay stay out.
    where (p.arr_pred_ts_utc is not null or p.dep_pred_ts_utc is not null
           or p.arr_delay_sec is not null or p.dep_delay_sec is not null)
    {% if is_incremental() %}
      and p.service_date >= current_date - cast(ceil({{ var('lookback_hours') }} / 24.0) as int)
    {% endif %}
),

fresh as (
    select * from preds
    -- delay-only rows have no event_pred_ts yet; their staleness is judged after
    -- synthesis, on the same rule, in `ranked`
    where event_pred_ts is null
       or fetched_at <= {{ dbt.dateadd('minute', var('prediction_staleness_min'), 'event_pred_ts') }}
),

watermark as (
    select max(fetched_at) as max_fetched from fresh
),

sched_ranked as (
    select
        f.city_key, f.service_date, f.trip_uid, f.stop_id, f.fetched_at,
        s.stop_sequence as static_stop_sequence,
        s.sched_arr_ts_utc, s.sched_dep_ts_utc, s.timepoint, s.gtfs_version_id,
        row_number() over (
            partition by f.city_key, f.service_date, f.trip_uid, f.stop_id, f.fetched_at
            -- delay-only rows anchor on fetch time; only loop routes visiting a
            -- stop twice in one trip even reach the tiebreak
            order by abs({{ seconds_between("coalesce(f.event_pred_ts, f.fetched_at)", 's.sched_arr_ts_utc') }})
        ) as sched_rn
    from fresh f
    join {{ ref('int_gtfs_scheduled_stop_times') }} s
      on s.city_key = f.city_key
     and s.service_date = f.service_date
     and s.trip_id = f.static_trip_id
     and s.stop_id = f.stop_id
),

enriched as (
    select
        f.*,
        sr.static_stop_sequence,
        sr.sched_arr_ts_utc,
        sr.sched_dep_ts_utc,
        sr.timepoint,
        sr.gtfs_version_id,
        coalesce(f.stop_sequence, sr.static_stop_sequence) as stop_sequence_eff,
        -- Synthesized actuals: schedule + stated delay stands in where the feed
        -- publishes no absolute time (the canonical COALESCE rule extended to the
        -- timestamps themselves). NULL when there is neither a time nor a
        -- schedule row to add the delay to — no schedule, no synthesized event.
        coalesce(
            f.arr_pred_ts_utc,
            {{ dbt.dateadd('second', 'f.arr_delay_sec', 'sr.sched_arr_ts_utc') }}
        ) as arr_eff_ts_utc,
        coalesce(
            f.dep_pred_ts_utc,
            {{ dbt.dateadd('second', 'f.dep_delay_sec', 'sr.sched_dep_ts_utc') }}
        ) as dep_eff_ts_utc
    from fresh f
    left join sched_ranked sr
      on sr.city_key = f.city_key and sr.service_date = f.service_date
     and sr.trip_uid = f.trip_uid and sr.stop_id = f.stop_id
     and sr.fetched_at = f.fetched_at and sr.sched_rn = 1
),

ranked as (
    select
        e.*,
        coalesce(e.event_pred_ts, e.arr_eff_ts_utc, e.dep_eff_ts_utc) as event_eff_ts,
        row_number() over (
            partition by e.city_key, e.service_date, e.trip_uid, e.stop_sequence_eff
            order by e.fetched_at desc, coalesce(e.event_pred_ts, e.arr_eff_ts_utc, e.dep_eff_ts_utc) desc
        ) as rn,
        count(*) over (
            partition by e.city_key, e.service_date, e.trip_uid, e.stop_sequence_eff
        ) as prediction_count,
        min(e.fetched_at) over (
            partition by e.city_key, e.service_date, e.trip_uid, e.stop_sequence_eff
        ) as first_seen_utc,
        max(e.fetched_at) over (
            partition by e.city_key, e.service_date, e.trip_uid, e.stop_sequence_eff
        ) as last_seen_utc
    from enriched e
    cross join watermark w
    -- A stop visit needs a stop. HSL emits a few updates carrying stop_sequence
    -- but no stop_id (892 of 7.2M silver rows, 849 of them with a sequence, so
    -- they clear the sequence filter above): they cannot be placed at a stop,
    -- joined to the schedule, or paired into a headway, and they arrived in
    -- fct_headways as 225 null stop_ids breaking its not-null contract. Same
    -- class of rule as the sequence filter — no identity, no event.
    where e.stop_sequence_eff is not null
      and e.stop_id is not null
      -- no timestamp even after synthesis = no event (delay-only row that never
      -- matched a schedule); and the staleness rule applies to synthesized rows
      -- exactly as it did to raw-timestamp rows in `fresh`
      and coalesce(e.event_pred_ts, e.arr_eff_ts_utc, e.dep_eff_ts_utc) is not null
      and e.fetched_at <= {{ dbt.dateadd('minute', var('prediction_staleness_min'),
                                         'coalesce(e.event_pred_ts, e.arr_eff_ts_utc, e.dep_eff_ts_utc)') }}
      and coalesce(e.event_pred_ts, e.arr_eff_ts_utc, e.dep_eff_ts_utc)
          <= w.max_fetched - interval '{{ var("finalize_horizon_min") }} minutes'
)

select
    city_key,
    service_date,
    trip_uid,
    stop_sequence_eff as stop_sequence,
    trip_id as trip_id_raw,
    static_trip_id,
    -- matched trips inherit the static route when the feed omits route_id
    -- (SF ADDED trips, 2026-08-27) — same authority rule as the matcher itself
    coalesce(route_id, matched_route_id) as route_id,
    case
        when city_key = 'nyc' then coalesce(
            matched_direction_id,
            case {{ re_extract('trip_id', '\.\.?([NS])', 1) }} when 'N' then 0 when 'S' then 1 end
        )
        else coalesce(direction_id, matched_direction_id)
    end as direction_id,
    stop_id,
    vehicle_id,
    match_confidence,
    gtfs_version_id,
    timepoint,
    sched_arr_ts_utc,
    sched_dep_ts_utc,
    arr_eff_ts_utc as actual_arr_ts_utc,
    dep_eff_ts_utc as actual_dep_ts_utc,
    -- Canonical rule, with the feed's own number held to a sanity bound.
    --
    -- [rev 2026-08-31] COALESCE prefers the agency's stated delay, which is
    -- right: the operator knows its own service. But it was preferring it
    -- UNCONDITIONALLY, so a corrupt field beat a correct computation sitting
    -- right beside it. 511 publishes arrival.delay values like -16,245,480 on
    -- SFMTA trips whose schedule and actual are two minutes apart; TTC and MTA
    -- do it too (sf 4,297, toronto 11,733, nyc 1,780 events over 2026-08-25..31,
    -- while zurich, helsinki, boston and dc emit none). At 0.08% of scored
    -- events they never moved a median and no test saw them, but they wrecked
    -- every mean: SF's mean arrival delay read -35,559 SECONDS against a median
    -- of +63.
    --
    -- A stop event cannot be a day early or a day late; such a value carries no
    -- information about delay, so it is treated as ABSENT and the COALESCE falls
    -- through to actual - scheduled, which we already have and which is right.
    -- The bound is deliberately loose (a day, not an hour): the goal is to
    -- reject the impossible, not to second-guess a genuinely terrible day.
    --
    -- [rev 2026-08-31, second pass] The bound applies to the RESULT, not just to
    -- the feed's input. Bounding only the stated delay left the computed branch
    -- free to be just as impossible: 14,695 events survived with |delay| > 1 day
    -- (toronto 12,422, nyc 2,213, sf 60) where BOTH timestamps are ordinary but
    -- the actual lands a full day after the matched schedule row — an overnight
    -- trip matched to the wrong calendar day, not a bus that is 24 hours late.
    -- When neither branch yields a plausible number the delay is UNKNOWN, and
    -- the honest value for unknown is NULL, not a fabricated one. The event
    -- still counts as service volume; it just stops polluting every delay and
    -- OTP aggregate (docs/06 divides by count(delay_arr_sec), never count(*)).
    {% set arr_delay = "coalesce(case when abs(arr_delay_sec) <= " ~ var('max_plausible_delay_sec')
        ~ " then arr_delay_sec end, " ~ seconds_between('sched_arr_ts_utc',
            'coalesce(arr_pred_ts_utc, dep_pred_ts_utc)') ~ ")" %}
    {% set dep_delay = "coalesce(case when abs(dep_delay_sec) <= " ~ var('max_plausible_delay_sec')
        ~ " then dep_delay_sec end, " ~ seconds_between('sched_dep_ts_utc', 'dep_pred_ts_utc') ~ ")" %}
    cast(case when abs({{ arr_delay }}) <= {{ var('max_plausible_delay_sec') }}
              then {{ arr_delay }} end as integer) as delay_arr_sec,
    cast(case when abs({{ dep_delay }}) <= {{ var('max_plausible_delay_sec') }}
              then {{ dep_delay }} end as integer) as delay_dep_sec,
    schedule_relationship,
    (schedule_relationship = 'CANCELED') as cancelled_flag,
    (stu_schedule_relationship = 'SKIPPED') as skipped_flag,
    -- which rule produced the actual: a real published timestamp, or schedule
    -- plus the feed's stated delay (Zurich; Tokyo's odpt_stated joins this family)
    case when arr_pred_ts_utc is not null or dep_pred_ts_utc is not null
         then 'last_prediction' else 'delay_plus_schedule' end as finalization_method,
    prediction_count,
    first_seen_utc,
    last_seen_utc,
    source_format
from ranked
where rn = 1

-- Tokyo rail rides its own path: train-grain ODPT snapshots have no
-- stop_time_updates, so the stop explosion + odpt_stated finalization live in
-- int_odpt_stop_events (docs/03 priority 0 — the only Tokyo rail source, so no
-- collision with the GTFS-RT branch above is possible; ToeiBus VP-only data
-- never reaches stg_gtfsrt__trip_updates either).
union all
select
    city_key, service_date, trip_uid, stop_sequence, trip_id_raw, static_trip_id,
    route_id, direction_id, stop_id, vehicle_id, match_confidence,
    gtfs_version_id, timepoint, sched_arr_ts_utc, sched_dep_ts_utc,
    actual_arr_ts_utc, actual_dep_ts_utc, delay_arr_sec, delay_dep_sec,
    schedule_relationship, cancelled_flag, skipped_flag, finalization_method,
    prediction_count, first_seen_utc, last_seen_utc, source_format
from {{ ref('int_odpt_stop_events') }}
