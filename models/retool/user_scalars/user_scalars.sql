SELECT
    user_details.*,
    chipper_device_count.num_of_chipper_devices,
    ip_address_count.num_of_ip_addresses,
    device_count.num_of_devices,
    payment_method_count.num_of_payment_methods,
    user_contact_counts.num_of_email_identifiers,
    user_contact_counts.num_of_phone_identifiers,
    user_referrals_made_count.num_of_referrals
FROM {{ref('user_details')}}
LEFT JOIN {{ref('chipper_device_count')}} ON user_details.user_id = chipper_device_count.user_id 
LEFT JOIN {{ref('ip_address_count')}} ON user_details.user_id = ip_address_count.user_id
LEFT JOIN {{ref('device_count')}} ON user_details.user_id = device_count.user_id
LEFT JOIN {{ref('payment_method_count')}} ON user_details.user_id = payment_method_count.user_id
LEFT JOIN {{ref('user_contact_counts')}} ON user_details.user_id = user_contact_counts.user_id
LEFT JOIN {{ref('user_referrals_made_count')}} ON user_details.user_id = user_referrals_made_count.user_id
