{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key=['channel','created_month']
) }}

WITH base AS (
    SELECT
        channel,
        total_handling_time_seconds,
        response_time_seconds,
        CONCAT(EXTRACT(MONTH FROM (created_at::TIMESTAMPTZ)),'-',EXTRACT(YEAR FROM (created_at::TIMESTAMPTZ))) AS created_month,  --explicit casting for datetime column type from string
        touchpoint_id
    FROM {{ ref('contact_raw_data') }}
),
channel_share AS (
    SELECT
        channel,
        created_month,
        SUM(CASE WHEN channel='chat' THEN 1/3 ELSE 1 END) AS num_cases, -- cases per channel and for chat 1/3 cases are counted
        COUNT(distinct touchpoint_id) as case_count
    FROM base
    GROUP BY CHANNEL, created_month
),
total_share AS (
    SELECT
        created_month,
        SUM(CASE WHEN channel='chat' THEN 1/3 ELSE 1 END) AS total_cases,
    FROM base
    GROUP BY created_month
)
SELECT
  c.channel,
  c.created_month,
  case_count,
  CASE WHEN channel='email' THEN (num_cases/total_cases)*500000
       WHEN channel='call' THEN (num_cases/total_cases)*500000
       WHEN channel='chat' THEN (num_cases/total_cases)*500000
    END AS monthly_cost,
 '{{ run_started_at.strftime("%Y-%m-%d") }}' AS dw_created_at  --timestamp to check for incremental addition of new rows
FROM channel_share c
LEFT JOIN total_share AS ts ON c.created_month = ts.created_month

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where dw_created_at::DATE > current_date - interval '7 day'
{% endif %}