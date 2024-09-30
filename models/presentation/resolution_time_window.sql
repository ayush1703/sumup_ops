{{ config(materialized='view') }}

SELECT 
time_difference, 
SUM(touchpoint_count) AS num_cases, 
count(*) AS row_count
FROM {{ ref('resolution_window') }}
GROUP BY 1