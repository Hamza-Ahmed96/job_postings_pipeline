{{config(materialized='table')}}

WITH distinct_dates AS (
    SELECT DISTINCT
        CAST(job_posted_date AS DATE) AS date_day
    FROM {{ref('stg_raw__job_postings')}}
    WHERE job_posted_date IS NOT NULL
)
SELECT
      TO_CHAR(date_day, 'YYYYMMDD')      AS date_id,
      date_day,                                                                                                                                        
      DAYNAME(date_day)                   AS day_name,
      TO_CHAR(date_day, 'MMMM')          AS month_name,                                                                                                
      MONTH(date_day)                     AS month_number,                                                                                             
      YEAR(date_day)                      AS year,
      WEEKOFYEAR(date_day)                AS week_number
  FROM distinct_dates
  ORDER BY date_day                               

