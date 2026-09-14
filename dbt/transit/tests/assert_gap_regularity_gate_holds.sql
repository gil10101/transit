-- A city that declares gap_regularity_measurable = false must publish NO
-- gap-derived regularity anywhere: no gap_ratio, no bunched/big-gap flag, and
-- no EWT. Zero is not an acceptable substitute for absent here, which is the
-- whole point of the flag.
--
-- Why the flag exists (Tokyo, 2026-09-12). Bunching and EWT compare an observed
-- sequence of arrivals against the timetable. That is only a measurement where
-- the observed side is independent of the scheduled side. Toei publishes a
-- stated delay instead of an arrival time, so an arrival is reconstructed as
-- schedule + stated delay; 91% of those stated delays are exactly zero, which
-- made 90.4% of Tokyo's consecutive-train gaps identical to the scheduled gap,
-- against 0.2-4.1% in every other city. Computed naively Tokyo scored 0.7%
-- bunching and a 2s median EWT — best in the fleet, on a metric that was very
-- nearly its own timetable handed back. The pipeline's standing rule is that a
-- metric with no evidence is NULL and never zero, so these are nulled at the
-- fact layer and this test keeps them nulled.
--
-- The complement is deliberately NOT asserted: a measurable city is free to have
-- null gap_ratio on individual rows (a first arrival of the day has no
-- predecessor, and an unmatched stop has no scheduled gap).

select 'fct_headways' as relation, h.city_key, count(*) as leaked_rows
from {{ ref('fct_headways') }} h
join {{ ref('dim_city') }} c
  on c.city_key = h.city_key
where not coalesce(c.gap_regularity_measurable, true)
  and (h.gap_ratio is not null or h.bunched_flag is not null or h.big_gap_flag is not null)
group by 1, 2

union all

select 'fct_route_reliability_daily' as relation, r.city_key, count(*) as leaked_rows
from {{ ref('fct_route_reliability_daily') }} r
join {{ ref('dim_city') }} c
  on c.city_key = r.city_key
where not coalesce(c.gap_regularity_measurable, true)
  and (r.ewt_sec is not null or r.bunching_pct is not null or r.big_gap_pct is not null)
group by 1, 2
