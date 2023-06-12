{{
    config(
        materialized='ephemeral'
    )
}}


SELECT DISTINCT monetized_user_id 
FROM {{ ref("revenues") }} 
