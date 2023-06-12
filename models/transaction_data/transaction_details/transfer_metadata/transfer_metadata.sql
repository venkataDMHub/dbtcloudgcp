{{ config(materialized='ephemeral') }}

with transfer_metadata_details as (
    select 
        distinct transfer_metadata.transfer_id,

        case when origin_expanded_transfers.outgoing_user_id = transfer_metadata.user_id then 
            object_construct(
                'transferMetadataDetails', array_agg(
                    object_construct(
                        'deviceId', transfer_metadata.device_id,
                        'ipAddress', transfer_metadata.ip_address,
                        'countryCode', ip_address_lookups.response:country_code::text,
                        'city', ip_address_lookups.response:city::text,
                        'region', ip_address_lookups.response:region::text,
                        'connectionType', ip_address_lookups.response:connection_type::text
                    )
                ) within group (order by transfer_metadata.transfer_id, origin_expanded_transfers.outgoing_user_id)
            ) 
            else try_parse_json('NULL')
        end as outgoing_user_transfer_metadata_details,

        case when destination_expanded_transfers.incoming_user_id = transfer_metadata.user_id then 
            object_construct(
                'transferMetadataDetails', array_agg(
                    object_construct(
                        'deviceId', transfer_metadata.device_id,
                        'ipAddress', transfer_metadata.ip_address,
                        'countryCode', ip_address_lookups.response:country_code::text,
                        'city', ip_address_lookups.response:city::text,
                        'region', ip_address_lookups.response:region::text,
                        'connectionType', ip_address_lookups.response:connection_type::text
                    )
                ) within group (order by transfer_metadata.transfer_id, destination_expanded_transfers.incoming_user_id)
            ) 
            else try_parse_json('NULL')
        end as incoming_user_transfer_metadata_details
    from CHIPPER.{{ var("core_public") }}."TRANSFER_METADATA"

    left join {{ref('expanded_transfers')}} as origin_expanded_transfers 
        on (
            origin_expanded_transfers.transfer_id = transfer_metadata.transfer_id
            and origin_expanded_transfers.outgoing_user_id = transfer_metadata.user_id
        ) 

    left join {{ref('expanded_transfers')}} as destination_expanded_transfers
        on (
            destination_expanded_transfers.transfer_id = transfer_metadata.transfer_id
            and destination_expanded_transfers.incoming_user_id = transfer_metadata.user_id
        )

    left join CHIPPER.{{ var("core_public") }}."IP_ADDRESS_LOOKUPS" 
        on transfer_metadata.ip_address = ip_address_lookups.response:host::varchar
    where 
        transfer_metadata.transfer_id in (select transfer_id from dbt_transformations.expanded_transfers)
        and transfer_metadata.user_id is not null
    group by 
        transfer_metadata.transfer_id,
        transfer_metadata.user_id,
        origin_expanded_transfers.outgoing_user_id,
        destination_expanded_transfers.incoming_user_id
    order by
        transfer_metadata.transfer_id desc
)
    
select 
    * 
from transfer_metadata_details
where 
    outgoing_user_transfer_metadata_details is not null
    or incoming_user_transfer_metadata_details is not null
