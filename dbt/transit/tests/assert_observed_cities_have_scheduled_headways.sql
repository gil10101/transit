-- CONTRACT TEST for the defect found 2026-09-12: Tokyo produced 362,473 real
-- observed headway gaps and not one scheduled headway to compare them against,
-- because fct_headways drew its scheduled side from the GTFS path alone and
-- Tokyo's schedule is odpt:TrainTimetable. Nothing went red. gap_ratio,
-- bunched_flag and big_gap_flag all divide by sched_headway_sec, so they simply
-- nulled out together, and the city rendered a dash for bunching and excess
-- wait next to a fully populated OTP column.
--
-- The shape of the bug is the point: a JOIN THAT MATCHES NOTHING produces NULL,
-- and NULL is indistinguishable from "honestly unmeasurable" unless something
-- asserts the difference. This test is that assertion. A city whose feed cannot
-- be matched to its schedule is exempt by design — dim_city.schedule_matchable
-- is how that is declared, and fct_headways nulls sched_headway_sec for those
-- cities deliberately — so the test fails only when a city claims to be
-- matchable, produces rated gaps, and still has no scheduled headway anywhere.
--
-- Windowed to the incremental lookback so it judges what the chain just built
-- rather than the whole history.

select
    h.city_key,
    count(*)                        as rated_gaps,
    count(h.sched_headway_sec)      as gaps_with_scheduled_headway
from {{ ref('fct_headways') }} h
join {{ ref('dim_city') }} c
  on c.city_key = h.city_key
where coalesce(c.schedule_matchable, true)
  and h.service_date >= current_date - cast(ceil({{ var('lookback_hours') }} / 24.0) as int)
group by h.city_key
having count(h.sched_headway_sec) = 0
