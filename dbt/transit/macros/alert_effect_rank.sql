-- GTFS-RT alert effect -> severity rank (1 = worst). Drives worst_effect picks in
-- fct_alerts_daily via min_by(effect, rank). Unknown/unmapped values sink to the
-- bottom so a real effect always outranks UNKNOWN_EFFECT.

{% macro alert_effect_rank(effect_col) %}
    case {{ effect_col }}
        when 'NO_SERVICE' then 1
        when 'REDUCED_SERVICE' then 2
        when 'SIGNIFICANT_DELAYS' then 3
        when 'DETOUR' then 4
        when 'MODIFIED_SERVICE' then 5
        when 'ADDITIONAL_SERVICE' then 6
        when 'STOP_MOVED' then 7
        when 'ACCESSIBILITY_ISSUE' then 8
        when 'OTHER_EFFECT' then 9
        when 'NO_EFFECT' then 10
        when 'UNKNOWN_EFFECT' then 11
        else 12
    end
{% endmacro %}
