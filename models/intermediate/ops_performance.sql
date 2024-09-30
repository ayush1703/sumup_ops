{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key=['channel','created_month','agent_company','country','reason', 'detailed_reason', 'mcc_group']
) }}


WITH base AS (
    SELECT
        country,
        channel,
        agent_company,
        mcc_group,
        reason,
        detailed_reason,
        CONCAT(EXTRACT(MONTH FROM (created_at::TIMESTAMPTZ)),'-',EXTRACT(YEAR FROM (created_at::TIMESTAMPTZ))) AS created_month,  --explicit casting for datetime column type from string
        status,
        total_handling_time_seconds,
        response_time_seconds,
        queue_waiting_time_seconds,
        touchpoint_id
    FROM {{ ref('contact_raw_data') }}
    WHERE agent_company is not null
),
performance_metrics AS(
    SELECT
        country,
        channel,
        agent_company,
        created_month,
        reason,
        detailed_reason,
        mcc_group,
        COUNT(distinct touchpoint_id) AS num_cases,
        SUM(total_handling_time_seconds/60) AS total_handling_time_minutes,
        SUM(response_time_seconds/60) AS total_response_time_minutes,
        SUM(queue_waiting_time_seconds/60) AS total_wait_time_minutes,
        AVG(total_handling_time_seconds/60) AS avg_handling_time_minutes,
        AVG(response_time_seconds/60) AS avg_response_time_minutes,
        AVG(queue_waiting_time_seconds/60) AS avg_wait_time_minutes,
        COUNT(distinct CASE WHEN STATUS='Resolved' AND channel='chat' AND total_handling_time_seconds<900 AND queue_waiting_time_seconds<15 THEN touchpoint_id
                            WHEN STATUS='Resolved' AND channel='call' AND total_handling_time_seconds<720 AND queue_waiting_time_seconds<15 THEN touchpoint_id
                            WHEN STATUS='Resolved' AND channel='email'AND response_time_seconds<3600*12 THEN touchpoint_id
                            ELSE NULL
                        END ) as perfect_cases,
        COUNT(distinct CASE WHEN channel='email' and response_time_seconds > 5*24*3600 THEN touchpoint_id 
                            WHEN channel<>'email' and queue_waiting_time_seconds>60 THEN touchpoint_id 
                            ELSE NULL END) as high_response_cases, 
        COUNT(distinct CASE WHEN STATUS = 'Resolved' THEN touchpoint_id ELSE NULL END) AS resolved_cases,
        '{{ run_started_at.strftime("%Y-%m-%d") }}' AS dw_created_at  --timestamp to check for incremental addition of new rows
    FROM base
    GROUP BY country, channel, agent_company, created_month, reason, detailed_reason, mcc_group
)
SELECT
*
FROM performance_metrics

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where dw_created_at::DATE > current_date - interval '7 day'
{% endif %}

