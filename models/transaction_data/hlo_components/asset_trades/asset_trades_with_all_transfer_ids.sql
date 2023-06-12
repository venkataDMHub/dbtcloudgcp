{{  config(
        materialized='incremental',
        unique_key='transfer_id',
        on_schema_change='append_new_columns', 
        schema='intermediate') }}

with all_transfer_ids as (
    select
        id::text as hlo_id,
        'ASSET_TRADES' as hlo_table,
        position as hlo_position,
        journal_id as hlo_journal_id,
        status as hlo_status,
        created_at as hlo_created_at,
        updated_at as hlo_updated_at,
        transfer_id as transfer_id,
        reverse_transfer_id as reverse_transfer_id,
        case
            when reverse_transfer_id is null then FALSE
            else TRUE
        end as is_original_transfer_reversed,
        null as external_provider,
        null as external_provider_transaction_id,
        asset,
        position,
        user_id as outgoing_user_id,
        user_id as incoming_user_id
    from {{ref('asset_trades')}}  
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
        hlo_position,
        hlo_journal_id,
        hlo_status,
        hlo_created_at,
        hlo_updated_at,
        transfer_id,
        is_original_transfer_reversed,
        FALSE as is_transfer_reversal,
        external_provider,
        external_provider_transaction_id,
        object_construct(
            '_internalTransactionDetails',object_construct(
                'asset', coalesce(asset, try_parse_json('NULL')),
                'position', coalesce(position,try_parse_json('NULL'))
            ),
            'externalProviderTransactionDetails',try_parse_json('NULL')
        ) as transaction_details,
        concat(
        transaction_details:"_internalTransactionDetails":"asset",
        ' ',
        transaction_details:"_internalTransactionDetails":"position"
      ) as shortened_transaction_details,
        outgoing_user_id,
        incoming_user_id
    from all_transfer_ids
),

reverse_transfer_ids as (
    select
        hlo_id,
        hlo_table,
        hlo_position,
        hlo_journal_id,
        hlo_status,
        hlo_created_at,
        hlo_updated_at,
        reverse_transfer_id as transfer_id,
        is_original_transfer_reversed,
        TRUE as is_transfer_reversal,
        external_provider,
        external_provider_transaction_id,
        object_construct(
            '_internalTransactionDetails',object_construct(
                'asset', coalesce(asset, try_parse_json('NULL')),
                'position', coalesce(position,try_parse_json('NULL'))
            ),
            'externalProviderTransactionDetails',try_parse_json('NULL')
        ) as transaction_details,
        concat(
        transaction_details:"_internalTransactionDetails":"asset",
        ' ',
        transaction_details:"_internalTransactionDetails":"position"
      ) as shortened_transaction_details,
        outgoing_user_id,
        incoming_user_id
    from all_transfer_ids
    where is_original_transfer_reversed = TRUE
)

select *
from transfer_ids
union
select *
from reverse_transfer_ids
