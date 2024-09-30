{{ config(materialized='view') }}

WITH base AS (
    SELECT
        agent_company,
        created_month,
        SUM(total_handling_time_minutes) AS total_handle_time,
        SUM(total_response_time_minutes) AS total_response_time,
        SUM(total_wait_time_minutes) AS total_wait_time,
        SUM(num_cases) AS total_cases,
        SUM(perfect_cases) AS perfect_cases,
        SUM(high_response_cases) AS high_response_cases,
        SUM(resolved_cases) AS resolved_cases
    FROM {{ ref('ops_performance') }}
    GROUP BY agent_company, created_month
)
SELECT  
    agent_company,
    created_month,
    total_handle_time/total_cases AS avg_handle_time,
    total_response_time/total_cases AS avg_response_time,
    total_wait_time/total_cases AS avg_wait_time,
    resolved_cases/total_cases AS resolved_cases_perc,
    high_response_cases/total_cases AS high_response_cases_perc,
    perfect_cases/total_cases AS perfect_cases_perc,
    '{{ run_started_at.strftime("%Y-%m-%d") }}' AS dw_created_at  --timestamp to check for incremental addition of new row
FROM base

