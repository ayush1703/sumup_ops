{{ config(
    materialized='incremental',
    schema='intermediate',
) }}

WITH ranked_touchpoints AS (
    SELECT
        MERCHANT_ID,
        REASON,
        DETAILED_REASON,
        TOUCHPOINT_ID,
        CREATED_AT,
        ROW_NUMBER() OVER (PARTITION BY MERCHANT_ID, REASON, DETAILED_REASON ORDER BY CREATED_AT) AS touchpoint_rank_first,
        ROW_NUMBER() OVER (PARTITION BY MERCHANT_ID, REASON, DETAILED_REASON ORDER BY CREATED_AT DESC) AS touchpoint_rank_last
    FROM {{ ref('contact_raw_data') }}
),
first_last_touchpoints AS (
    -- Extract only the first and last touchpoints for each merchant, reason, and detailed reason
    SELECT
        MERCHANT_ID,
        REASON,
        DETAILED_REASON,
        COUNT(distinct touchpoint_id) as touchpoint_count,
        MIN(CASE WHEN touchpoint_rank_first = 1 THEN CREATED_AT END) AS first_touchpoint_time,
        MAX(CASE WHEN touchpoint_rank_last = 1 THEN CREATED_AT END) AS last_touchpoint_time
    FROM 
        ranked_touchpoints
    GROUP BY
        MERCHANT_ID,
        REASON,
        DETAILED_REASON
)
SELECT
    MERCHANT_ID,
    REASON,
    DETAILED_REASON,
    touchpoint_count,
    first_touchpoint_time,
    last_touchpoint_time,
    EXTRACT(DAY FROM (last_touchpoint_time::TIMESTAMPTZ - first_touchpoint_time::TIMESTAMPTZ)) AS time_difference_days,
     '{{ run_started_at.strftime("%Y-%m-%d") }}' AS dw_created_at  --timestamp to check for incremental addition of new row
FROM
    first_last_touchpoints
WHERE
    first_touchpoint_time IS NOT NULL 
    AND last_touchpoint_time IS NOT NULL

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
AND dw_created_at > current_date - interval '7 day'
{% endif %}