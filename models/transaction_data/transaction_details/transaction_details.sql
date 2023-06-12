{{
    config(
        materialized="incremental",
        unique_key="transfer_id",
        on_schema_change="append_new_columns",
    )
}}

with
    all_transaction_details as (
        select
            transfer_id,
            hlo_table,
            external_provider,
            external_provider_transaction_id,
            transaction_details,
            shortened_transaction_details,
            hlo_created_at,
            hlo_updated_at,
            hlo_status,
            outgoing_user_id,
            incoming_user_id
        from {{ ref("airtime_purchases_with_all_transfer_ids") }}
        union
        select
            transfer_id,
            hlo_table,
            external_provider,
            external_provider_transaction_id,
            transaction_details,
            shortened_transaction_details,
            hlo_created_at,
            hlo_updated_at,
            hlo_status,
            outgoing_user_id,
            incoming_user_id

        from {{ ref("asset_trades_with_all_transfer_ids") }}
        union
        select
            transfer_id,
            hlo_table,
            external_provider,
            external_provider_transaction_id,
            transaction_details,
            shortened_transaction_details,
            hlo_created_at,
            hlo_updated_at,
            hlo_status,
            outgoing_user_id,
            incoming_user_id
        from {{ ref("bill_payments_with_all_transfer_ids") }}
        union
        select
            transfer_id,
            hlo_table,
            external_provider,
            external_provider_transaction_id,
            transaction_details,
            shortened_transaction_details,
            hlo_created_at,
            hlo_updated_at,
            hlo_status,
            outgoing_user_id,
            incoming_user_id
        from {{ ref("checkouts_with_all_transfer_ids") }}
        union
        select
            transfer_id,
            hlo_table,
            external_provider,
            external_provider_transaction_id,
            transaction_details,
            shortened_transaction_details,
            hlo_created_at,
            hlo_updated_at,
            hlo_status,
            outgoing_user_id,
            incoming_user_id
        from {{ ref("crypto_deposits_with_all_transfer_ids") }}
        union
        select
            transfer_id,
            hlo_table,
            external_provider,
            external_provider_transaction_id,
            transaction_details,
            shortened_transaction_details,
            hlo_created_at,
            hlo_updated_at,
            hlo_status,
            outgoing_user_id,
            incoming_user_id
        from {{ ref("crypto_withdrawals_with_all_transfer_ids") }}
        union
        select
            transfer_id,
            hlo_table,
            external_provider,
            external_provider_transaction_id,
            transaction_details,
            shortened_transaction_details,
            hlo_created_at,
            hlo_updated_at,
            hlo_status,
            outgoing_user_id,
            incoming_user_id
        from {{ ref("data_purchases_with_all_transfer_ids") }}
        union
        select
            transfer_id,
            hlo_table,
            external_provider,
            external_provider_transaction_id,
            transaction_details,
            shortened_transaction_details,
            hlo_created_at,
            hlo_updated_at,
            hlo_status,
            outgoing_user_id,
            incoming_user_id
        from {{ ref("deposits_transaction_details") }}
        union
        select
            transfer_id,
            hlo_table,
            external_provider,
            external_provider_transaction_id,
            transaction_details,
            shortened_transaction_details,
            hlo_created_at,
            hlo_updated_at,
            hlo_status,
            outgoing_user_id,
            incoming_user_id
        from {{ ref("issued_card_transactions_with_all_transfer_ids") }}
        union
        select
            transfer_id,
            hlo_table,
            external_provider,
            external_provider_transaction_id,
            transaction_details,
            shortened_transaction_details,
            hlo_created_at,
            hlo_updated_at,
            hlo_status,
            outgoing_user_id,
            incoming_user_id
        from {{ ref("orders_with_all_transfer_ids") }}
        union
        select
            transfer_id,
            hlo_table,
            external_provider,
            external_provider_transaction_id,
            transaction_details,
            shortened_transaction_details,
            hlo_created_at,
            hlo_updated_at,
            hlo_status,
            outgoing_user_id,
            incoming_user_id
        from {{ ref("payment_invitations_with_all_transfer_ids") }}
        union
        (
            select
                transfer_id,
                hlo_table,
                external_provider,
                external_provider_transaction_id,
                transaction_details,
                shortened_transaction_details,
                hlo_created_at,
                hlo_updated_at,
                hlo_status,
                outgoing_user_id,
                incoming_user_id
            from {{ ref("payments_transaction_details") }}
            where
                transfer_id not in (
                    select transfer_id
                    from {{ ref("withdrawals_with_all_transfer_ids") }}
                )
        )
        union
        select
            transfer_id,
            hlo_table,
            external_provider,
            external_provider_transaction_id,
            transaction_details,
            shortened_transaction_details,
            hlo_created_at,
            hlo_updated_at,
            hlo_status,
            outgoing_user_id,
            incoming_user_id
        from {{ ref("requests_with_all_transfer_ids") }}
        union
        select
            transfer_id,
            hlo_table,
            external_provider,
            external_provider_transaction_id,
            transaction_details,
            shortened_transaction_details,
            hlo_created_at,
            hlo_updated_at,
            hlo_status,
            outgoing_user_id,
            incoming_user_id
        from {{ ref("stock_trades_with_all_transfer_ids") }}
        union
        select
            transfer_id,
            hlo_table,
            external_provider,
            external_provider_transaction_id,
            transaction_details,
            shortened_transaction_details,
            hlo_created_at,
            hlo_updated_at,
            hlo_status,
            outgoing_user_id,
            incoming_user_id
        from {{ ref("withdrawals_with_all_transfer_ids") }}
    )
select
    all_transaction_details.*,
    outgoing_user_transfer_metadata_details,
    incoming_user_transfer_metadata_details,
    case
        when expanded_transfers.transfer_id is null then false else true
    end as is_on_ledger
from all_transaction_details
left join
    {{ ref("transfer_metadata") }} as transfer_metadata
    on all_transaction_details.transfer_id = transfer_metadata.transfer_id
left join
    {{ ref("expanded_transfers") }}
    on all_transaction_details.transfer_id = expanded_transfers.transfer_id
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where
    all_transaction_details.hlo_updated_at
    >= (select max(hlo_updated_at) from {{ this }})
{% endif %}
