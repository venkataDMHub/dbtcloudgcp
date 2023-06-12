{{ config(materialized='ephemeral') }}

    with ranked_branch_installs as (
        select
            user_id,
            branch_installs.advertising_id,
            branch_installs.campaign,
            TRIM(branch_installs.body:"last_attributed_touch_data":"~ad_set_id",'"') AS ad_set_id,
            TRIM(branch_installs.body:"last_attributed_touch_data":"~ad_set_name",'"') AS ad_set_name,
            branch_installs.created_at as branch_install_created_at,
            branch_installs.id as branch_install_id,
            coalesce(
                branch_installs.advertising_partner_name, 'Direct Install'
            ) as acquisition_source,
            row_number() over (
                partition by user_id order by branch_install_created_at asc
            ) as row_num

        from {{ref('first_seen_user_device_ids')}} as user_device_ids
        join chipper.{{var("core_public")}}.branch_installs as branch_installs
            on user_device_ids.advertising_id = branch_installs.advertising_id
    )

    select
        user_id,
        acquisition_source,
        campaign,
        NVL(ad_set_id, campaign) AS ad_set_id,
        NVL(ad_set_name, campaign) AS ad_set_name,
        advertising_id,
        branch_install_created_at,
        branch_install_id

    from ranked_branch_installs
    where row_num = 1
    