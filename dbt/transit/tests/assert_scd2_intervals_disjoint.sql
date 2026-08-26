-- Contract: SCD2 validity intervals must never overlap within a natural key.
-- An overlap makes the point-in-time join in scd2_join() match two dim rows for
-- one fact row, and a delete+insert incremental silently doubles those facts —
-- the same double-count shape the scorecard grain bug had, one layer down.
-- valid_to is exclusive, so intervals may touch (a.valid_to = b.valid_from) but
-- never cross.

with all_intervals as (

    select 'dim_route' as dim, city_key, route_id as natural_id, valid_from, valid_to
    from {{ ref('dim_route') }}
    union all
    select 'dim_stop', city_key, stop_id, valid_from, valid_to
    from {{ ref('dim_stop') }}

)

select
    a.dim,
    a.city_key,
    a.natural_id,
    a.valid_from as a_from,
    a.valid_to   as a_to,
    b.valid_from as b_from,
    b.valid_to   as b_to
from all_intervals a
join all_intervals b
  on b.dim = a.dim
 and b.city_key = a.city_key
 and b.natural_id = a.natural_id
 and b.valid_from > a.valid_from  -- each pair once; equal valid_from is impossible (surrogate key is md5 of it)
where a.valid_to is null
   or b.valid_from < a.valid_to
