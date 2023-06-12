SELECT 
    user_lpv_groups.user_id, created_at, lpv_group
FROM 
    dbt_transformations.user_lpv_groups
LEFT JOIN 
    dbt_transformations.expanded_users 
    ON user_lpv_groups.user_id = expanded_users.user_id
WHERE
    lpv_group in ('Middle Base', 'High Base', 'Outlier Base')
    AND 
        (
        expanded_users.created_at between '2021-12-04' and '2021-12-06'
        OR
        expanded_users.created_at between '2022-01-22' and '2022-01-24'
        OR
        expanded_users.created_at between '2022-03-24' and '2022-03-26'
        OR
        expanded_users.created_at between '2022-05-15' and '2022-05-17'
        )
