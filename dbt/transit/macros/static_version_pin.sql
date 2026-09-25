{% macro static_version_pin(city_col, retired_from_col, version_col='gtfs_version_id') %}
{#- The static version a model pins to. Normally the newest, which is what the delay
    path wants: the matchers and scheduled stop times only ever work inside the 72h
    window, where newest is correct by definition.

    [rev 2026-09-25] For a RETIRED city, newest means newest staged before its
    retired_from. Once a poller stops there is no 72h window any more, so "newest"
    stops being correct by definition and becomes merely whatever the Sunday refresh
    downloaded last. The 2026-09-20 refresh proved it: five days after seven cities
    retired, a timetable none of them was ever observed against became the one their
    history was matched through. Events were safe (int_stop_events_finalized is
    incremental and frozen), but int_trip_matching is rebuilt each chain, so 142k
    historical trips stopped resolving, more than half of Boston's matched
    cancellations fell out of the rate and its score moved 81.1 -> 81.3 with nothing
    new observed, and Helsinki's and DC's cancels vanished from it entirely. With the
    cap every figure came back exactly: int_trip_matching 2,091,844 rows, Boston
    7,409 matched cancels and 81.1, Helsinki 168, DC 11. Capping the pin at
    retirement makes a retired city's timetable stop where its feed stopped — the
    same idea as retired_from in the completeness tests and the scorecard ledger.
    Version ids are <city>-<UTC staging date>-<sha8>, so the date compares as text.

    var('static_version_override') = {city: gtfs_version_id} swaps in an OLDER version
    for a one-off repair run (docs/08, 2026-09-11). A rotation whose calendar starts at
    its publication date leaves the days before it unschedulable against the newest
    static — TTC's 2026-09-06 refresh did that to toronto 09-03..05 — and recomputing
    those days correctly needs the static that was live on them. Scheduled chains never
    set it. -#}
{%- set newest -%}
max(case when {{ retired_from_col }} is null
          or split_part({{ version_col }}, '-', 2) < replace(cast({{ retired_from_col }} as varchar), '-', '')
     then {{ version_col }} end)
{%- endset -%}
{%- set overrides = var('static_version_override', {}) -%}
{%- if overrides -%}
case {{ city_col }}
{%- for city, version in overrides.items() %}
    when '{{ city }}' then '{{ version }}'
{%- endfor %}
    else {{ newest }} end
{%- else -%}
{{ newest }}
{%- endif -%}
{% endmacro %}
