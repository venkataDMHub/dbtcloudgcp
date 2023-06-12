{{ config(materialized='table',
          schema='looker') }}


WITH users_who_opened_app AS (
    SELECT
        feed_banners.tag,
        feed_banners.user_id,
        count(*) AS event_count
    FROM chipper.{{ var("core_public") }}.feed_banners AS feed_banners
    LEFT JOIN amplitude.EVENTS_204512 AS amplitude_data
        ON amplitude_data.user_id = feed_banners.user_id
    WHERE
        amplitude_data.client_event_time BETWEEN feed_banners.updated_at AND feed_banners.end_timestamp
    GROUP BY feed_banners.tag, feed_banners.user_id
)

SELECT
    feed_banners.id,
    feed_banners.deep_link_url,
    feed_banners.start_timestamp,
    feed_banners.title,
    feed_banners.updated_at,
    feed_banners.end_timestamp,
    feed_banners.user_id,
    feed_banners.subtitle,
    feed_banners.uat_transfer_id,
    feed_banners.rank,
    feed_banners.currency,
    feed_banners.tag,
    feed_banners.icon_link,
    feed_banners.uat_reward_amount,
    feed_banners.link_text,
    feed_banners.uat_criteria_fulfilled,
    expanded_ledgers.hlo_updated_at AS uat_updated_at,
    user_demographic_features.legal_first_name,
    user_demographic_features.legal_last_name,
    user_demographic_features.dob,
    user_demographic_features.gender,
    user_demographic_features.primary_currency,
    user_demographic_features.acquisition_source,
    user_demographic_features.acquisition_date,
    user_demographic_features.kyc_tier,
    user_demographic_features.is_internal,
    user_demographic_features.is_deleted,
    user_demographic_features.is_admin,
    user_demographic_features.is_valid_user,
    user_demographic_features.is_blocked_by_flag,
    user_demographic_features.all_active_flags,
    user_demographic_features.phone_number,
    user_demographic_features.email_address,
    user_demographic_features.total_lpv_usd,
    user_demographic_features.percent_lpv_from_rewards,
    user_demographic_features.lpv_group,
    user_demographic_features.latest_engagement_bucket,
    user_demographic_features.latest_engagement_score,
    CASE
        WHEN users_who_opened_app.event_count > 0 THEN 'true' ELSE 'false'
    END AS opened_app_during_campaign
FROM chipper.{{ var("core_public") }}.feed_banners AS feed_banners

LEFT JOIN {{ ref('expanded_ledgers') }} AS expanded_ledgers
    ON expanded_ledgers.transfer_id = feed_banners.uat_transfer_id

LEFT JOIN users_who_opened_app
    ON users_who_opened_app.user_id = feed_banners.user_id
        AND users_who_opened_app.tag = feed_banners.tag

LEFT JOIN {{ ref('user_demographic_features') }} AS user_demographic_features
    ON user_demographic_features.user_id = feed_banners.user_id

INNER JOIN chipper.utils.rados_campaigns_v2
    on rados_campaigns_v2.tag = feed_banners.tag

WHERE type = 'ACTIVATED'
