
/*
Specifically for stg_job_postings:

  1. Rename columns — id → job_id, job_title_short → role etc. Consistent naming throughout the project
  2. Split location — "Belfast, Northern Ireland" → city + region separately so you can group by either
  3. Extract date parts — week_posted, month_posted from job_posted_date for trend analysis
  4. Calculate salary midpoint — salary_mid = (salary_min + salary_max) / 2 — this becomes the target variable for the ML salary predictor
  5. Flag nulls and edge cases — is_salary_missing, is_fixed_salary — so analysts can filter rather than getting silent nulls affecting aggregations

  Everything after this model builds on top of stg_job_postings — it's the single source of truth for the rest of the dbt project.
*/

with sources as (
    select * from {{source('raw', 'job_postings')}}
),

staged as (
    SELECT
        id, 
        job_title_short as role,
        title,
        company_name,
        job_posted_date,
        trim(split_part(location, ',', 1)) as city,
        trim(split_part(location, ',', 2)) as region,
        salary_min as minimum_salary,
        salary_max as maximum_salary,
        ROUND((salary_min + salary_max) / 2, 0) as salary_avg_year,
        case
            when contract_type IS NULL THEN 'n/a'
            ELSE contract_type
        END AS contract_type,
        description,
        load_date
    FROM sources
)

SELECT * FROM staged