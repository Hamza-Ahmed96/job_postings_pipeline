/*
dim_role - distinct roles with surrogate key
*/
-- produces a table
{{ config(
    materialized='table'
) }}

WITH distinct_roles AS (
    SELECT DISTINCT
        role
    FROM {{ref ('stg_raw__job_postings')}}
    WHERE role IS NOT NULL
)
SELECT
    'R' || ROW_NUMBER() OVER(ORDER BY role) AS role_id,
    role
FROM distinct_roles


