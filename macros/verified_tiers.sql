{% macro verified_tiers() %}
    SELECT
        'TIER_2' AS tier
    UNION ALL
    SELECT
        'TIER_3' AS tier
{% endmacro %}