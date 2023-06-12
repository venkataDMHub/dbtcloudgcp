{{ config(materialized='table',
          schema='looker') }}

/* When there is only one doc/country the document name is double (Jinja constraint): NGN, GHS, RWF */
{% set PRIMARY_DOCUMENTS = {'NGN':('BVN','BVN'), 
                           'GHS': ('GHANA_CARD', 'GHANA_CARD'), 
                           'ZAR': ('NATIONAL_ID','DRIVERS_LICENSE', 'PASSPORT', 'ZA_GREEN_BOOK'),
                           'UGX': ('NATIONAL_ID', 'PASSPORT'),
                           'USD': ('NATIONAL_ID','PASSPORT','PASSPORT_CARD', 'DRIVERS_LICENSE', 'SERVICE_ID_CARD', 'VISA'),
                           'GBP': ('PASSPORT', 'DRIVERS_LICENSE', 'RESIDENCE_PERMIT', 'SERVICE_ID_CARD'),
                           'RWF': ('NATIONAL_ID','NATIONAL_ID'),
                           'TZS': ('NATIONAL_ID', 'PASSPORT'),
                           'KES': ('NATIONAL_ID', 'PASSPORT', 'ALIEN_CARD')
                             }   
%} 	

with user_base as (

    select
        user_id,
        created_at as account_created_at,
        primary_currency,
        acquisition_source,
        is_deleted,
        is_valid_user,
        is_blocked_by_flag,
        kyc_tier as current_kyc_tier
    from {{ref( 'expanded_users') }}
    where is_admin = FALSE
        and is_internal = FALSE

),

selfie_submitted as (

    select distinct
        user_id,
        created_at as selfie_first_submitted_at,
        provider as provider_for_first_submitted_selfie
    from chipper.{{ var("compliance_public") }}.liveness_checks
    qualify created_at = min(created_at) over (partition by user_id)

),

selfie_accepted as (

    select distinct
        user_id,
        updated_at as selfie_first_accepted_at,
        provider as provider_for_first_accepted_selfie
    from chipper.{{ var("compliance_public") }}.liveness_checks
    where status = 'ACCEPTED'
    qualify updated_at = min(updated_at) over (partition by user_id)

),

document_submitted as (

    select distinct
        owner_id as user_id,
        submitted_at as first_submitted_doc_at,
        doc_type as first_submitted_document_type,
        doc_number as first_submitted_document_number
    from chipper.{{ var("compliance_public") }}.kyc_documents
    qualify submitted_at = min(submitted_at) over (partition by user_id)

),

document_accepted as (

    select distinct 
    kyc_documents.owner_id as user_id, 
    document_status_changes.timestamp as first_primary_document_accepted_at, 
    kyc_documents.doc_type as first_accepted_primary_document_type,
    kyc_documents.doc_number as first_accepted_primary_document_number
from  chipper.{{ var("compliance_public") }}.kyc_documents
left join  chipper.{{ var("compliance_public") }}.document_status_changes  
    on kyc_documents.id = document_status_changes.document_id
    and document_status_changes.new_status = 'ACCEPTED' 
join user_base users
    on kyc_documents.owner_id = users.user_id 
where kyc_documents.status = 'ACCEPTED' 
        
  and 
Case 
{% for key, value in PRIMARY_DOCUMENTS.items() %}     
    when primary_currency = '{{key}}' and kyc_documents.doc_type in {{value}}
        then true
{% endfor %}
else false end = TRUE
qualify document_status_changes.timestamp = min(document_status_changes.timestamp) 
        over (partition by owner_id)

),

account_flag_check as (

    select
        user_id,
        min(date_flagged) as first_date_of_current_account_blocking_flags,
        listagg(distinct flag, ', ')
        within group (order by flag asc) as list_of_current_flags
    from chipper.{{ var("compliance_public") }}.account_flags
    where unflagged_by is null
        and flag in ('USER_LOCKED', 'USER_OFFBOARDED', 'BLOCKED_PEP', 'POTENTIAL_SANCTIONS_MATCH', 'CONFIRMED_SANCTIONS_MATCH')
    group by 1


),

verification_complete as (

    select
        t.user_id,
        c.timestamp as first_verified_at,
        c.previous_tier as tier_before_verification
    from chipper.{{ var("compliance_public") }}.user_tiers as t
    inner join chipper.{{ var("compliance_public") }}.user_tier_changes as c
        on t.user_id = c.user_id
    where t.tier in ({{ verified_tiers() }})

),

final as (

    select
        base.user_id,
        base.account_created_at,
        base.primary_currency,
        base.acquisition_source,
        base.is_deleted,
        base.is_valid_user,
        base.is_blocked_by_flag,
        base.current_kyc_tier,

        iff(selfie_submitted.user_id is not null, TRUE, FALSE) as has_submitted_selfie,
        selfie_submitted.selfie_first_submitted_at,
        selfie_submitted.provider_for_first_submitted_selfie,

        iff(selfie_accepted.user_id is not null, TRUE, FALSE) as has_selfie_accepted,
        selfie_accepted.selfie_first_accepted_at,
        selfie_accepted.provider_for_first_accepted_selfie,
        
        iff(document_submitted.user_id is not null, TRUE, FALSE) as has_submitted_documentation,
        document_submitted.first_submitted_doc_at,
        document_submitted.first_submitted_document_type,
        document_submitted.first_submitted_document_number,

        iff(document_accepted.user_id is not null, TRUE, FALSE) as has_documentation_accepted,
        document_accepted.first_primary_document_accepted_at,
        document_accepted.first_accepted_primary_document_type,
        document_accepted.first_accepted_primary_document_number,
        
        iff(account_flag_check.user_id is not null, TRUE, FALSE) as has_account_blocking_flag_check,
        account_flag_check.first_date_of_current_account_blocking_flags,
        account_flag_check.list_of_current_flags,

        iff(verification_complete.user_id is not null, TRUE, FALSE) as has_been_verified,
        verification_complete.first_verified_at,
        verification_complete.tier_before_verification,
        
        case
            when has_been_verified = TRUE then '7. verified'
            when has_account_blocking_flag_check = TRUE then '6. account blocked'
            when has_documentation_accepted = TRUE then '5. documentation_accepted'
            when has_submitted_documentation = TRUE then '4. documentation_submitted'
            when has_selfie_accepted = TRUE then '3. selfie_accepted'
            when has_submitted_selfie = TRUE then '2. selfie_submitted'
            else '1. account_created'
        end as last_step_in_funnel,
        case
            when has_been_verified = TRUE then first_verified_at
            when has_account_blocking_flag_check = TRUE then first_date_of_current_account_blocking_flags
            when has_documentation_accepted = TRUE then first_primary_document_accepted_at
            when has_submitted_documentation = TRUE then first_submitted_doc_at
            when has_selfie_accepted = TRUE then selfie_first_accepted_at
            when has_submitted_selfie = TRUE then selfie_first_submitted_at
            else account_created_at
        end as last_step_in_funnel_at,
        datediff(min, account_created_at, first_verified_at) as minutes_account_created_to_verified,
        datediff(min, account_created_at, first_submitted_doc_at) as minutes_account_created_to_doc_submitted,
        datediff(min, first_submitted_doc_at, first_primary_document_accepted_at ) as minutes_doc_submitted_to_accepted, 
        datediff(min, account_created_at, selfie_first_submitted_at) as minutes_account_created_to_selfie_submitted, 
        datediff(min, selfie_first_submitted_at, selfie_first_accepted_at) as minutes_selfie_submitted_to_accepted 
    from user_base as base
    left join selfie_submitted
        on base.user_id = selfie_submitted.user_id
    left join selfie_accepted
              on base.user_id = selfie_accepted.user_id
    left join document_submitted
              on base.user_id = document_submitted.user_id
    left join document_accepted
              on base.user_id = document_accepted.user_id
    left join account_flag_check
        on base.user_id = account_flag_check.user_id
    left join verification_complete
              on base.user_id = verification_complete.user_id

)

select *
from final
