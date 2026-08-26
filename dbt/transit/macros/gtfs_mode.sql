{#-
  route_type -> mode, one place. Base GTFS types 0-4 plus Google extended route
  types (HSL uses them: 701/702/704 bus, 109 suburban rail, 900 tram — 92% of its
  routes). Zurich's Swiss national static is extended-ONLY (no 0-7 anywhere,
  fixture-verified 2026-08-23): 1xx rail (RT-matched routes 102-116), 700 bus,
  900 tram, 1000 water -> ferry — all covered; the static's aerial 13xx /
  funicular 14xx / taxi 15xx land on 'other' (marginal for city scoring; revisit
  only if a scored route surfaces there).
-#}
{% macro gtfs_mode(route_type) %}
    case
        when {{ route_type }} = 0 then 'tram'
        when {{ route_type }} = 1 then 'metro'
        when {{ route_type }} = 2 then 'rail'
        when {{ route_type }} = 3 then 'bus'
        when {{ route_type }} = 4 then 'ferry'
        when {{ route_type }} between 100 and 199 then 'rail'
        when {{ route_type }} between 200 and 299 then 'bus'
        when {{ route_type }} between 400 and 499 then 'metro'
        when {{ route_type }} between 700 and 799 then 'bus'
        when {{ route_type }} between 900 and 999 then 'tram'
        when {{ route_type }} between 1000 and 1299 then 'ferry'
        else 'other'
    end
{% endmacro %}
