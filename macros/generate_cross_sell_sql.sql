{% macro generate_cross_sell_sql(primary_currency, transfer_type) %}

{# LPV_GROUPS to use for targetting cross-sell #}
    {% set targeted_lpv_group= ( 'Slight Outliers', 'Outlier Base', 'Top User')
    %}

    {% set query %}

    WITH lpv_segment as (
        SELECT  expanded_users.user_id 
        FROM       {{ ref('expanded_users')}} as expanded_users
        LEFT JOIN  "CHIPPER".{{var("core_public")}}."USER_SEGMENTS" as base_users
        ON   expanded_users.user_id = base_users.user_id
        LEFT JOIN {{ ref('user_lpv_groups')}} as user_lpv_groups
            ON  expanded_users.user_id = user_lpv_groups.user_id

        WHERE  primary_currency = '{{ primary_currency }}'
        AND  segment = 'BASE_AUDIENCE_FOR_ACTIVATION_CAMPAIGNS'
        AND  lpv_group in {{targeted_lpv_group}}
    ), 

    product_line_for_cross_sell as (
        SELECT 
            lpv_segment.user_id, 
            expanded_ledgers.transfer_type
        FROM 
            lpv_segment
        LEFT JOIN   {{ ref('expanded_ledgers')}} as expanded_ledgers
               ON   expanded_ledgers.main_party_user_id = lpv_segment.user_id
               AND  expanded_ledgers.transfer_type = '{{transfer_type}}'
               AND  is_original_transfer_reversed = FALSE
    )
    

    SELECT  distinct user_id 
    FROM  product_line_for_cross_sell
    WHERE  transfer_type is null

    {%- endset %}


{{ return(query) }}

{% endmacro %}
