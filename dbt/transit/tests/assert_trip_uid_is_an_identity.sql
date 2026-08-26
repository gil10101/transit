-- A trip_uid must IDENTIFY one trip. If it does not, every join through it silently
-- merges unrelated vehicles, and nothing else in the warehouse notices.
--
-- [rev 2026-08-26] This test exists because the same defect shipped twice, and both
-- times it was found by hand rather than by anything failing.
--
-- Round 1: Spark's concat_ws SKIPS nulls and returns '' — never NULL — when every input
-- is null, so the COALESCE that was meant to fall back never fired and every record
-- with no trip descriptor hashed to sha256('<city>|<date>|'). Live impact: 1,374,084
-- toronto rows across 2,189 vehicles on one uid, plus sf and dc.
--
-- Round 2: wrapping the inner concat in nullif made the identity NULL, and then the
-- OUTER concat_ws skipped it too, producing sha256('<city>|<date>'). Same collapse, new
-- constant. 82,774 toronto rows still shared one uid NINETEEN HOURS after the fix
-- shipped and was reported as done.
--
-- A shape test could not have caught either round: the column was populated, unique-ish
-- in aggregate, and non-null. Only a test of what the value MEANS catches it. That is
-- the difference this test is here to make.
--
-- The rule: a record with no trip identity at all (no trip_id, and no route_id to build
-- a composite from) must carry a NULL trip_uid. GTFS-RT makes VehiclePosition.trip
-- optional — deadheading and unassigned vehicles are normal — so the absence is fine.
-- Inventing an identity for it is not.
{{ config(severity='error') }}

with degenerate as (

    -- vehicle positions are the exposed surface: trip updates have never had a
    -- descriptor-less row in any city, and if that ever changes this catches it too
    select
        'vehicle_positions' as source,
        city_key,
        service_date,
        trip_uid,
        count(*) as rows_sharing_uid,
        count(distinct vehicle_id) as vehicles_sharing_uid
    from {{ ref('stg_gtfsrt__vehicle_positions') }}
    where trip_uid is not null
      and trip_id is null
      and route_id is null
    group by 1, 2, 3, 4

    union all

    select
        'trip_updates' as source,
        city_key,
        service_date,
        trip_uid,
        count(*) as rows_sharing_uid,
        count(distinct vehicle_id) as vehicles_sharing_uid
    from {{ ref('stg_gtfsrt__trip_updates') }}
    where trip_uid is not null
      and trip_id is null
      and route_id is null
    group by 1, 2, 3, 4

)

select *
from degenerate
-- one vehicle with a fabricated id is a curiosity; several sharing one is a collapse,
-- and it is always several when the hash is a constant
where vehicles_sharing_uid > 1
