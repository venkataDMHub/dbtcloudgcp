
with uvs_timing_combined as (
    SELECT 
        user_uvs_timing_features.user_id,
        user_app_version_at_onboarding.os,
        user_app_version_at_onboarding.app_version,
        user_uvs_timing_features.onboarding_time_sec,
        user_uvs_timing_features.time_creation_to_first_kyc_submission_sec,
        user_uvs_timing_features.time_creation_to_verified_sec
    FROM  {{ ref('user_uvs_timing_features') }} 
        LEFT JOIN {{ ref('user_app_version_at_onboarding') }} on user_uvs_timing_features.user_id = user_app_version_at_onboarding.user_id
    WHERE os is not null and app_version is not null
)

SELECT
    DISTINCT os,
    app_version,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY onboarding_time_sec asc) OVER (PARTITION BY os, app_version) AS onboarding_time_sec_25_percentile,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY onboarding_time_sec asc) OVER (PARTITION BY os, app_version) AS onboarding_time_sec_50_percentile,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY onboarding_time_sec asc) OVER (PARTITION BY os, app_version) AS onboarding_time_sec_75_percentile,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY time_creation_to_first_kyc_submission_sec asc) OVER (PARTITION BY os, app_version) AS time_creation_to_first_kyc_submission_25_percentile,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY time_creation_to_first_kyc_submission_sec asc) OVER (PARTITION BY os, app_version) AS time_creation_to_first_kyc_submission_50_percentile,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY time_creation_to_first_kyc_submission_sec asc) OVER (PARTITION BY os, app_version) AS time_creation_to_first_kyc_submission_75_percentile,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY time_creation_to_verified_sec asc) OVER (PARTITION BY os, app_version) AS time_creation_to_verified_sec_25_percentile,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY time_creation_to_verified_sec asc) OVER (PARTITION BY os, app_version) AS time_creation_to_verified_sec_50_percentile,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY time_creation_to_verified_sec asc) OVER (PARTITION BY os, app_version) AS time_creation_to_verified_sec_75_percentile
FROM uvs_timing_combined
WHERE os is not null and app_version is not null
    and onboarding_time_sec > 0
    and time_creation_to_first_kyc_submission_sec > 0
    and time_creation_to_verified_sec > 0
    and app_version like '%.%'
