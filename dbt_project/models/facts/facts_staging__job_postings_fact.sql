{{config(materialized='table', alias='job_postings_fact')}}

SELECT
    jp.id as job_id,
    roles.role_id,
    loc.location_id,
    dates.date_id,
    jp.title,
    jp.company_name,
    jp.minimum_salary, 
    jp.maximum_salary,
    jp.salary_avg_year,
    jp.description AS job_description,
    jp.load_date AS load_date
FROM {{ref('stg_raw__job_postings')}} AS jp
LEFT JOIN {{ref('dims_staging__dim_location')}} AS loc
ON jp.city = loc.city
LEFT JOIN {{ref('dims_staging__dim_roles')}} AS roles
ON jp.role = roles.role
LEFT JOIN  {{ref('dims_staging__dim_dates')}} AS dates
ON CAST(jp.job_posted_date AS DATE) = dates.DATE_DAY