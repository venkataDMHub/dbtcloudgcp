{{  config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

with all_transfer_ids as (
    select
        id::text as hlo_id,
        'DEPOSITS' as hlo_table,
        journal_id as hlo_journal_id,
        status as hlo_status,
        created_at as hlo_created_at,
        updated_at as hlo_updated_at,
        transfer_id as transfer_id,
        reverse_transfer_id as reverse_transfer_id,
        case when reverse_transfer_id is null then FALSE
            else TRUE
        end as is_original_transfer_reversed
    from
        "CHIPPER".{{ var("core_public") }}."DEPOSITS"
    where transfer_id is not null
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and hlo_updated_at >= (select max(hlo_updated_at) from {{ this }})
    {% endif %}


),

transfer_ids as (
    select
        hlo_id,
        hlo_table,
        hlo_journal_id,
        hlo_status,
        hlo_created_at,
        hlo_updated_at,
        transfer_id,
        is_original_transfer_reversed,
        FALSE as is_transfer_reversal
    from
        all_transfer_ids
),

reverse_transfer_ids as (
    select
        hlo_id,
        hlo_table,
        transfers.journal_id as hlo_journal_id,
        hlo_status,
        hlo_created_at,
        hlo_updated_at,
        reverse_transfer_id as transfer_id,
        is_original_transfer_reversed,
        TRUE as is_transfer_reversal
    from
        all_transfer_ids
        join
        "CHIPPER".{{ var("core_public") }}."TRANSFERS" on
            all_transfer_ids.reverse_transfer_id = transfers.id
    where
        is_original_transfer_reversed = TRUE
)

select *
from
    transfer_ids
union
select *
from
    reverse_transfer_ids
