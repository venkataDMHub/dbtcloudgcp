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

with unaccepted_documents as (

    select
        kyc_documents.submitted_at,
        u.primary_currency,
        u.user_id,
        u.acquisition_source,
        kyc_documents.doc_type,
        kyc_documents.status as current_status,
        kyc_documents.issuing_country,
        document_status_changes.document_id,
        document_status_changes.new_status,
        document_status_changes.reason,
        document_status_changes.timestamp as document_info_updated_at,
        case
            when rank() over (
                partition by document_id order by document_info_updated_at desc) = 1
                then true
            else false
        end as is_latest_document_info
    from chipper.{{ var("compliance_public") }}.kyc_documents
    left join chipper.{{ var("compliance_public") }}.document_status_changes
        on kyc_documents.id = document_status_changes.document_id
            and status != 'ACCEPTED'
 inner join dbt_transformations.expanded_users as u
        on kyc_documents.owner_id = u.user_id
    where
        case
                {% for key, value in PRIMARY_DOCUMENTS.items() %}     
            when primary_currency = '{{ key }}' and kyc_documents.doc_type in {{ value }}
                then true
                        {% endfor %}
            else false end = true

),

top_rejected_reasons_last_6_months as ( -- This is to serve as a filter since so many reasons 

    select distinct
        reason,
        count(distinct document_id) as total_documents_per_reason
    from unaccepted_documents
    where new_status = 'REJECTED'
          and submitted_at >= dateadd(month, -6, current_date)
    group by 1

),

final as (

    select
        unaccepted_documents.*,
        dense_rank() over (order by total_documents_per_reason desc) as doc_reason_rank
    from unaccepted_documents
    left join top_rejected_reasons_last_6_months as top
        on unaccepted_documents.reason = top.reason
            and unaccepted_documents.new_status = 'REJECTED'
    order by doc_reason_rank asc
)

select *
from final
