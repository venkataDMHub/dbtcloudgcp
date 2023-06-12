{{ config(materialized='table', schema='looker') }}

Select *
From {{ ref('monthly_retention_by_activity') }}
Union
Select *
From {{ ref('weekly_retention_by_activity') }}
Union
Select *
From {{ ref('quarterly_retention_by_activity') }}
