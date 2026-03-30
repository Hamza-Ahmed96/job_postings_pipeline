{{config(materialized='view', alias='average_salary_by_role')}}


SELECT
    roles.role AS roles,
    ROUND(AVG(jp.salary_avg_year)) AS average_salary
FROM 
    {{ref('facts_staging__job_postings_fact')}} AS jp
LEFT JOIN 
    {{ref('dims_staging__dim_roles')}} AS roles
ON 
    jp.role_id = roles.role_id
GROUP BY 
    roles.role
ORDER BY 
    average_salary DESC