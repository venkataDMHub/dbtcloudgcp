SELECT 
    distinct user_id,
    True AS has_similar_selfie
FROM 
    (
        SELECT 
            data:PartnerParams:user_id AS user_id,
            value:user_id as detected_similar_selfie_user_id
        FROM 
            {{ var('compliance_public') }}.smile_id_callback,
            lateral flatten( input => DATA:Antifraud:SuspectUsers)
        WHERE 
            user_id != detected_similar_selfie_user_id
            -- 5211 as the SmileID code for similar selfie found
            and 
                (
                    data:ResultCode = '5210'
                    or data:ResultCode = '5211'
                )
            and type = 'ANTI_FRAUD'
    )
