{{
    config(materialized='table')
}}


WITH distinct_locations AS (
    SELECT DISTINCT
        city,
        region
    FROM {{ref('stg_raw__job_postings')}}
)
SELECT
    'L' || ROW_NUMBER() OVER(ORDER BY city, region) AS location_id,
    *
FROM distinct_locations
