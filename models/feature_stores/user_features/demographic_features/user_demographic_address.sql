{{
    config(
        materialized='ephemeral'
    )
}}

with addresses_rank as (
    select
        *,
        row_number() over (
            partition by user_id order by created_at asc
        ) as row_num_first,
        row_number() over (
            partition by user_id order by created_at desc
        ) as row_num_latest
    from chipper.{{ var("compliance_public") }}.addresses
),

addresses_first as (
    select
        user_id,
        country,
        city,
        street,
        postal_code,
        lat,
        long,
        region
    from addresses_rank
    where row_num_first = 1
),

addresses_latest as (
    select
        user_id,
        country,
        city,
        street,
        postal_code,
        lat,
        long,
        region
    from addresses_rank
    where row_num_latest = 1
)

select
    addresses_first.user_id,
    addresses_first.country as country_first,
    addresses_first.city as city_first,
    addresses_first.street as street_first,
    addresses_first.postal_code as postal_code_first,
    addresses_first.lat as lat_first,
    addresses_first.long as long_first,
    addresses_first.region as region_first,
    addresses_latest.country as country_latest,
    addresses_latest.city as city_latest,
    addresses_latest.street as street_latest,
    addresses_latest.postal_code as postal_code_latest,
    addresses_latest.lat as lat_latest,
    addresses_latest.long as long_latest,
    addresses_latest.region as region_latest
from
    addresses_first
inner join
    addresses_latest on addresses_first.user_id = addresses_latest.user_id
