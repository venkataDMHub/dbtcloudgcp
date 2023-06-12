{{
  config(
    cluster_by=['left(outbound_user_id, 4)','left(inbound_user_id, 4)'] 
  )
}}

WITH user_transaction_ip_addresses AS (
    SELECT
        transaction_details.transfer_id,
        COALESCE(transaction_details:"_internalTransactionDetails":"paymentMethodDetails":"isExternal",false) AS is_external,
        outgoing_user_transfer_metadata_details:"transferMetadataDetails"[0]:"ipAddress"::text AS outgoing_user_ip_address,           
        incoming_user_transfer_metadata_details:"transferMetadataDetails"[0]:"ipAddress"::text AS incoming_user_ip_address,
        outgoing_user_transfer_metadata_details:"transferMetadataDetails"[0]:"deviceId"::text AS sender_chipper_device_id,
        incoming_user_transfer_metadata_details:"transferMetadataDetails"[0]:"deviceId"::text AS receiver_chipper_device_id,
        transaction_details
    FROM {{ ref('transaction_details') }}
),
sender_ip AS (
    SELECT 
        LEFT(ip_address, POSITION('/' IN ip_address)-1) AS sender_ip_address_a,
        get(response, 'country_code') AS sender_country_code,
        get(response, 'city') AS sender_city,
        get(response, 'region') AS sender_region,
        get(response, 'connection_type') AS sender_connection_type
    FROM
        {{ var("core_public") }}.ip_address_lookups
),
receiver_ip AS (
    SELECT 
        LEFT(ip_address, POSITION('/' IN ip_address)-1) AS receiver_ip_address_a,
        get(response, 'country_code') AS receiver_country_code,
        get(response, 'city') AS receiver_city,
        get(response, 'region') AS receiver_region,
        get(response, 'connection_type') AS receiver_connection_type
    FROM
        {{ var("core_public") }}.ip_address_lookups
)
SELECT 
    expanded_transfers.transfer_id,
    expanded_transfers.journal_id,
    expanded_transfers.hlo_table,
    expanded_transfers.journal_type,
    expanded_transfers.transfer_type,
    expanded_transfers.hlo_id,
    expanded_transfers.origin_currency,
    expanded_transfers.origin_amount,
    expanded_transfers.origin_rate,
    expanded_transfers.destination_currency,
    expanded_transfers.destination_amount,
    expanded_transfers.destination_rate,
    expanded_transfers.hlo_status,
    expanded_transfers.transfer_status,
    expanded_transfers.transfer_created_at,
    expanded_transfers.hlo_created_at,
    expanded_transfers.transfer_updated_at,
    expanded_transfers.hlo_updated_at,
    expanded_transfers.origin_amount_in_usd,
    expanded_transfers.destination_amount_in_usd,
    expanded_transfers.outgoing_user_id AS outbound_user_id,
    eu_sender.display_first_name AS sender_user_display_first_name,
    eu_sender.display_last_name AS sender_user_display_last_name,
    eu_sender.legal_first_name AS sender_user_legal_first_name,
    eu_sender.legal_last_name AS sender_user_legal_last_name,
    eu_sender.tag AS sender_user_tag,
    eu_sender.primary_currency AS sender_user_primary_currency,
    eu_sender.acquisition_source AS sender_user_acquisition_source,
    eu_sender.created_at AS sender_user_created_at,
    eu_sender.kyc_tier AS sender_user_kyc_tier,
    sender_ip.sender_ip_address_a AS sender_user_ip_address,
    user_transaction_ip_addresses.sender_chipper_device_id,
    expanded_transfers.incoming_user_id AS inbound_user_id,
    eu_receiver.display_first_name AS receiver_user_display_first_name,
    eu_receiver.display_last_name AS receiver_user_display_last_name,
    eu_receiver.legal_first_name AS receiver_user_legal_first_name,
    eu_receiver.legal_last_name AS receiver_user_legal_last_name,
    eu_receiver.tag AS receiver_user_tag,
    eu_receiver.primary_currency AS receiver_user_primary_currency,
    eu_receiver.acquisition_source AS receiver_user_acquisition_source,
    eu_receiver.kyc_tier AS receiver_user_kyc_tier,
    eu_receiver.created_at AS receiver_user_created_at,
    receiver_ip.receiver_ip_address_a AS receiver_user_ip_address,
    user_transaction_ip_addresses.receiver_chipper_device_id,
    transaction_details.shortened_transaction_details,
    user_transaction_ip_addresses.transaction_details,
    sender_ip.sender_country_code AS sender_user_country_code,
    sender_ip.sender_city AS sender_user_city,
    sender_ip.sender_region AS sender_user_region,
    sender_ip.sender_connection_type AS sender_user_connection_type,
    receiver_ip.receiver_country_code AS receiver_user_country_code,
    receiver_ip.receiver_city AS receiver_user_city,
    receiver_ip.receiver_region AS receiver_user_region,
    receiver_ip.receiver_connection_type AS receiver_user_connection_type,
    user_transaction_ip_addresses.is_external,
    expanded_transfers.corridor,
    expanded_transfers.is_original_transfer_reversed,
    expanded_transfers.is_transfer_reversal
FROM {{ ref('expanded_transfers') }}
LEFT JOIN {{ ref('expanded_users') }} AS eu_sender ON expanded_transfers.outgoing_user_id = eu_sender.user_id
LEFT JOIN {{ ref('expanded_users') }} AS eu_receiver ON expanded_transfers.incoming_user_id = eu_receiver.user_id
LEFT JOIN {{ ref('transaction_details') }} ON expanded_transfers.transfer_id = transaction_details.transfer_id 
LEFT JOIN user_transaction_ip_addresses ON expanded_transfers.transfer_id = user_transaction_ip_addresses.transfer_id
LEFT JOIN sender_ip ON user_transaction_ip_addresses.outgoing_user_ip_address = sender_ip.sender_ip_address_a
LEFT JOIN receiver_ip ON user_transaction_ip_addresses.incoming_user_ip_address = receiver_ip.receiver_ip_address_a
