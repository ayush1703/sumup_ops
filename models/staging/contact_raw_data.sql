{{ config(materialized='incremental', schema='staging', unique_key=['TOUCHPOINT_ID']) }}


WITH import_csv AS (            --CTE to load the csv data
        SELECT 
            *
        FROM read_csv_auto('../Ops_Analyst_case_study_350.csv')
    )
SELECT
    touchpoint_id,
    reason,
    detailed_reason,
    reason_group,
    country,
    channel,
    status,
    merchant_id,
    mcc_group,
    total_handling_time_seconds,
    response_time_seconds,
    queue_waiting_time_seconds,
    agent_id,
    agent_company,
    created_at,
    '{{ run_started_at.strftime("%Y-%m-%d") }}' AS dw_created_at  --timestamp to check for incremental addition of new rows
FROM import_csv

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where dw_created_at > current_date - interval '7 day'
{% endif %}