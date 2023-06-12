{% macro generate_cross_sell_new_user_offer_sql(primary_currency) %}

    {# Dictionary that maps country to days_joined_since to qualify for activation #}
    {% set country_mapping = {
        'NGN': -60,
        'KES': -60,
        'UGX': -60,
        'ZAR': -60,
        'GHS': -60,
        'RWF': -60,
        'TZS': -60,
        'USD': -60,
        'GBP': -60,
        'ZMW': -60
    } %}

    {% set query %}
    WITH country_segment AS (
        SELECT users.id as user_id
        FROM {{ var("core_public")}}."USERS" AS users
        LEFT JOIN "CHIPPER".{{ var("compliance_public") }}."USER_TIERS" AS user_tiers
            ON users.id = user_tiers.user_id
        WHERE primary_currency = '{{ primary_currency }}'
            AND  CAST(user_tiers.updated_at AS DATE) BETWEEN
                DATEADD('day', {{country_mapping[primary_currency]}}, CURRENT_DATE()) AND CURRENT_DATE()
            AND user_tiers.tier in ({{ verified_tiers() }})
    ),
    first_new_user_offer_status AS (
        SELECT country_segment.user_id
        FROM country_segment
        JOIN {{ var("core_public") }}."FEED_BANNERS" AS feed_banners
            ON feed_banners.user_id = country_segment.user_id
        WHERE uat_transfer_id IS NULL
            AND feed_banners.tag = 'FIRST_NEW_USER_OFFER_{{ primary_currency }}'
            AND country_segment.user_id NOT IN (
                SELECT user_id FROM {{ var("core_public") }}."USER_SEGMENTS"
                WHERE segment = 'FIRST_NEW_USER_OFFER_{{ primary_currency }}'
            )
    )
    SELECT DISTINCT user_id
    FROM first_new_user_offer_status
    {%- endset %}

    {{ return(query) }}

{% endmacro %}
