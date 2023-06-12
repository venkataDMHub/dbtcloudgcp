{{ config(materialized='table', schema='looker') }}

Select *
From {{ ref('monthly_retention') }}
Union
Select *
From {{ ref('weekly_retention') }}
Union
Select *
From {{ ref('quarterly_retention') }}
