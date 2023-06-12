{{ config(materialized='ephemeral') }}

with chargeback_details as (

SELECT 
    id,
    transfer_id,
    status,
    updated_status,
    amount,
    amount_in_usd,
    chargeback_created_at
FROM "CHIPPER"."DBT_TRANSFORMATIONS"."EXPANDED_CHARGEBACKS" as chargebacks
qualify ROW_NUMBER() OVER (PARTITION BY TRANSFER_ID ORDER BY TRANSFER_ID, CHARGEBACK_CREATED_AT DESC) = '1'

) 

select * from chargeback_details


