-- No scored stop event may carry a delay beyond max_plausible_delay_sec.
--
-- [rev 2026-08-31] Feeds publish corrupt delay fields, and the canonical
-- COALESCE used to prefer them unconditionally over the computation sitting
-- beside them: 511 stated -16,245,480s on SFMTA trips whose schedule and actual
-- were two minutes apart. 17,810 such events (sf 4,297 / toronto 11,733 /
-- nyc 1,780) survived in gold across 2026-08-25..31 without moving a single
-- median or failing a single test, while making SF's mean arrival delay read
-- -35,559 seconds against a median of +63. Means are quoted in docs/06 and on
-- the dashboard; a metric that only breaks in the mean is still broken.
--
-- The finalizer now treats an out-of-range stated delay as absent. This asserts
-- the outcome rather than the mechanism, so it also catches a future feed
-- inventing a new way to be wrong.

-- Scoped to the REPAIRABLE window, for the same reason
-- assert_gold_reflects_silver_coverage is: a test that is permanently red on
-- history no run can rewrite teaches everyone to ignore it. Shipping this
-- unscoped on 2026-08-31 failed two chains on 17,587 pre-existing rows that no
-- chain could have fixed — the lesson the coverage test had already learned,
-- applied one file too late. Older rows are repaired by a wide-lookback rebuild,
-- not by a tripwire nagging about them every two hours.
select
    city_key,
    service_date,
    trip_uid,
    stop_sequence,
    delay_arr_sec,
    delay_dep_sec
from {{ ref('fct_stop_events') }}
where (abs(delay_arr_sec) > {{ var('max_plausible_delay_sec') }}
    or abs(delay_dep_sec) > {{ var('max_plausible_delay_sec') }})
  and service_date >= {{ dbt.dateadd('day',
        "-cast(ceil(" ~ var('lookback_hours') ~ " / 24.0) as int)", 'current_date') }}
