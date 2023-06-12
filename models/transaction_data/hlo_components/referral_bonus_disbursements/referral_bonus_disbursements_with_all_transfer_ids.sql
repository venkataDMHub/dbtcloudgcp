{{ config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns', 
        schema='intermediate'
    )
}}

select
    disbursements.id::text as hlo_id,
    'DISBURSEMENTS' as hlo_table,
    disbursements.journal_id as hlo_journal_id,
    disbursements.status as hlo_status,
    disbursements.created_at as hlo_created_at,
    disbursements.updated_at as hlo_updated_at,
    referrals.transfer_id as transfer_id,
    false as is_original_transfer_reversed,
    false as is_transfer_reversal
from {{ ref('referrals_with_all_transfer_ids') }} as referrals
inner join "CHIPPER".{{ var("core_public") }}."DISBURSEMENTS" as disbursements
    on referrals.transfer_id = disbursements.transfer_id
where 
    referrals.transfer_id is not null
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
    {% endif %}
