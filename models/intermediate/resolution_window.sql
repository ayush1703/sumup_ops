{{ config(
    materialized='incremental',
    schema='intermediate'
) }}


WITH touchpoints_with_diff AS ( --CTE to identify the difference of window to be created
    SELECT
        merchant_id,
        reason,
        detailed_reason,
        touchpoint_id,
        created_at,
        status,
        country,
        channel,
        mcc_group,
        agent_company,
        total_handling_time_seconds,
        response_time_seconds,
        LAG(CREATED_AT) OVER (PARTITION BY merchant_id, reason, detailed_reason ORDER BY created_at) AS previous_touchpoint_time,
        -- Calculate the difference in days between consecutive touchpoints
        EXTRACT(EPOCH FROM (created_at::TIMESTAMPTZ - LAG(created_at) OVER (PARTITION BY merchant_id, reason, detailed_reason ORDER BY created_at)::TIMESTAMPTZ)) / 86400 AS diff_days
    FROM {{ ref('contact_raw_data') }}
),
touchpoints_with_window AS ( --CTE to assign window group within 7 days range
    SELECT
        merchant_id,
        reason,
        detailed_reason,
        touchpoint_id,
        created_at,
        status,
        country,
        channel,
        mcc_group,
        agent_company,
        total_handling_time_seconds,
        response_time_seconds,
        -- Assign a new window group each time the gap between touchpoints is greater than 7 days
        SUM(CASE WHEN diff_days IS NULL OR diff_days > 7 THEN 1 ELSE 0 END) OVER (PARTITION BY merchant_id, reason, detailed_reason ORDER BY created_at) AS window_group
    FROM
        touchpoints_with_diff
),
windowed_touchpoints AS (
    -- Aggregate data by the window group to get the first and last touchpoint for each window
    SELECT
        merchant_id,
        reason,
        detailed_reason,
        window_group,
        MIN(created_at) AS first_touchpoint_time,
        MAX(created_at) AS last_touchpoint_time,
        SUM(CASE WHEN STATUS='Resolved' THEN 1 ELSE 0 END) resolved_count,
        SUM(CASE WHEN STATUS='Serviced' THEN 1 ELSE 0 END) serviced_count,
        SUM(total_handling_time_seconds) as total_handle_time,
        COUNT(distinct touchpoint_id) AS touchpoint_count
    FROM
        touchpoints_with_window
    GROUP BY
        merchant_id,
        reason,
        detailed_reason,
        window_group
)
SELECT
    merchant_id,
    reason,
    detailed_reason,
    first_touchpoint_time,
    last_touchpoint_time,
    touchpoint_count,
    total_handle_time,
    resolved_count,
    serviced_count,
    EXTRACT(DAY FROM (last_touchpoint_time::TIMESTAMPTZ - first_touchpoint_time::TIMESTAMPTZ)) AS time_difference,
     '{{ run_started_at.strftime("%Y-%m-%d") }}' AS dw_created_at  --timestamp to check for incremental addition of new row
FROM
    windowed_touchpoints

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where dw_created_at > current_date - interval '7 day'
{% endif %}