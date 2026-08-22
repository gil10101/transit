-- OTP banding per docs/03-dbt-spec.md §8 vars: early < -60s, on_time <= +299s,
-- late <= +899s, very_late beyond. Null delay stays null (never counted on-time).

{% macro otp_band(delay_col) %}
    case
        when {{ delay_col }} is null then null
        when {{ delay_col }} < {{ var("otp_bands")["early_max"] }} then 'early'
        when {{ delay_col }} <= {{ var("otp_bands")["on_time_max"] }} then 'on_time'
        when {{ delay_col }} <= {{ var("otp_bands")["late_max"] }} then 'late'
        else 'very_late'
    end
{% endmacro %}
