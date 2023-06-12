{% macro generate_reactivation_sql(primary_currency, transfer_type) %}

{# Dictionary that maps country to days_joined_since to qualify for activation #}
    {% set country_mapping= {'NGN': -60, 
                            'KES' : -60,
                            'UGX' : -60, 
                            'ZAR' : -60,
                            'GHS' : -60,
                            'RWF' : -60,
                            'TZS' : -60,
                            'USD' : -60,
                            'GBP' : -60,
                            'ZMW' : -60}
    %}

    {% set query %}

    with country_segment as (
    SELECT  expanded_users.user_id 
    FROM       {{ ref('expanded_users')}} as expanded_users
    LEFT JOIN  "CHIPPER".{{var("core_public")}}."USER_SEGMENTS" as base_users
        ON   expanded_users.user_id = base_users.user_id
    WHERE  primary_currency = '{{ primary_currency }}'
    AND  segment = 'BASE_AUDIENCE_FOR_ACTIVATION_CAMPAIGNS'
    AND  cast(created_at as date) < dateadd('day', {{country_mapping[primary_currency]}}, current_date()) 
    ), 

    product_line_for_reactivation as (
    SELECT country_segment.user_id, expanded_ledgers.transfer_type
    FROM  country_segment 
    LEFT JOIN  {{ ref('expanded_ledgers')}} as expanded_ledgers
    ON   expanded_ledgers.main_party_user_id = country_segment.user_id
    AND  expanded_ledgers.transfer_type = '{{transfer_type}}'
    AND  is_original_transfer_reversed = FALSE
    AND  cast(hlo_created_at as date) >= dateadd('day', {{country_mapping[primary_currency]}}, current_date()))

    SELECT  distinct user_id 
    FROM  product_line_for_reactivation
    WHERE transfer_type is null

    {%- endset %}


{{ return(query) }}

{% endmacro %}
