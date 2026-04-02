{{config(materialized='view', alias='average_salary_by_location')}}


SELECT
    roles.role AS role,
    CASE WHEN loc.region IS NULL THEN 'n/a' ELSE loc.region
    END AS region,
    ROUND(AVG(jp.salary_avg_year)) AS average_salary_by_location
FROM 
    {{ref('facts_staging__job_postings_fact')}} AS jp
JOIN 
    {{ref('dims_staging__dim_location')}} AS loc
ON 
    jp.location_id = loc.location_id
JOIN 
    {{ref('dims_staging__dim_roles')}} AS roles
ON
    jp.role_id = roles.role_id
GROUP BY 
    roles.role,
    loc.region
ORDER BY 
    role, average_salary_by_location DESC