with
    banking_data as (
        select
            user_id,
            banks.id,
            bank_accounts.id as bank_account_id,
            bank_accounts.account_number,
            bank_accounts.routing_number,
            bank_accounts.account_type
        from "CHIPPER".{{ var("core_public") }}."LINKED_ACCOUNTS"
        join
            "CHIPPER".{{ var("core_public") }}."BANK_ACCOUNTS"
            on linked_accounts.id = bank_accounts.linked_account_id
        join
            "CHIPPER".{{ var("core_public") }}."BANKS"
            on banks.id = bank_accounts.bank_id
        where is_linked = true

    ),
    risk_scoring as (
        select user_id, primary_currency, risk_bucket, crr_score
        from chipper.utils.user_crr
        where updated_at is not null
        qualify row_number() over (partition by user_id order by updated_at desc) = 1
    ),
    amplitude_event as (

        select
            amplitude_event.user_creation_time,
            amplitude_event.ip_address,
            map_user_amplitude.user_id
        from {{ ref("map_user_amplitude") }} as map_user_amplitude
        left join
            "CHIPPER"."AMPLITUDE"."EVENTS_204512" as amplitude_event
            on amplitude_event.amplitude_id = map_user_amplitude.amplitude_id
        where amplitude_event.event_type = 'Onboarding - Ended'


    ),
    kyc_documents as (


        select *
        from "CHIPPER".{{ var("compliance_public") }}."KYC_DOCUMENTS"
        where status = 'ACCEPTED'

    ),
    main as (

        select
            expanded_users.user_id as user_id,
            expanded_users.created_at as date_created_at,
            expanded_users.primary_currency as primary_currency,
            amplitude_event.user_creation_time,
            amplitude_event.ip_address as first_ip_address,
            expanded_users.phone_number as phone_number,
            expanded_users.email_address as email,
            kyc_documents.doc_expires_on as doc_expiration_date,
            kyc_documents.doc_number as doc_number,
            kyc_documents.doc_type as doc_type,
            kyc_documents.issuing_country as issuing_country,
            kyc_documents.doc_issued_on as doc_issued_on,
            kyc_documents.doc_url as doc_url,
            kyc_documents.ownership_proof_url as ownership_proof_url,
            expanded_users.kyc_tier as user_tier,
            concat(
                expanded_users.legal_first_name, ' ', expanded_users.legal_last_name
            ) as legal_name,
            case
                when expanded_users.primary_currency = 'USD'
                then trim(onfido_v.document_report_result:properties:issuingstate, '"')
            end as issuing_state,
            to_timestamp_tz(user_demographic_features.dob) as date_of_birth,
            concat_ws(
                ',',
                user_demographic_features.country_first,
                user_demographic_features.city_first,
                user_demographic_features.street_first
            ) as location_at_registration_time,
            banking_data.bank_account_id,
            banking_data.routing_number,
            banking_data.account_number,
            banking_data.account_type,
            businesses.id as business_id,
            businesses.dba,
            businesses.tin,
            businesses.phone as business_phone_number,
            businesses.email as business_email,
            businesses.website as business_website,
            crypto_addresses.id as crypto_addresses_id,
            crypto_addresses.address as crypto_addresses,
            devices.provider_response:"id" as device_id,
            banking_data.id as institution_id,
            case
                when expanded_users.primary_currency = 'USD' then 'Voyse Technologies'
            end as institution_name,
            case
                when expanded_users.primary_currency = 'USD'
                then 'IRS'
                else 'NOT_APPLICABLE'
            end as primary_federal_regulator,
            risk_scoring.risk_bucket,
            risk_scoring.crr_score
        from {{ ref("expanded_users") }} as expanded_users
        left join
            {{ ref("user_demographic_features") }} as user_demographic_features
            on expanded_users.user_id = user_demographic_features.user_id
        left join amplitude_event on expanded_users.user_id = amplitude_event.user_id
        left join kyc_documents on expanded_users.user_id = kyc_documents.owner_id
        left join
            "CHIPPER".{{ var("compliance_public") }}."ONFIDO_VERIFICATIONS" as onfido_v
            on onfido_v.user_id = expanded_users.user_id
        left join
            "CHIPPER".{{ var("compliance_public") }}."BUSINESS_INFO" as businesses
            on businesses.primary_account_owner_id = expanded_users.user_id
        left join
            "CHIPPER".{{ var("core_public") }}."CRYPTO_ADDRESSES" as crypto_addresses
            on crypto_addresses.user_id = expanded_users.user_id
        left join
            "CHIPPER".{{ var("core_public") }}."DEVICE_FINGERPRINTS" as devices
            on devices.user_id = expanded_users.user_id
        left join banking_data on banking_data.user_id = expanded_users.user_id
        left join risk_scoring on risk_scoring.user_id = expanded_users.user_id
    )

select *
from main
