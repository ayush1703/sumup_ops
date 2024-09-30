{{ config(materialized='view') }}


SELECT
    channel,
    created_month,
    SUM(monthly_cost),
    SUM(case_count),
    SUM(monthly_cost)/SUM(case_count),
    '{{ run_started_at.strftime("%Y-%m-%d") }}' AS dw_created_at  --timestamp to check for incremental addition of new rows
FROM {{ ref('channel_cost') }}
GROUP BY channel, created_month

