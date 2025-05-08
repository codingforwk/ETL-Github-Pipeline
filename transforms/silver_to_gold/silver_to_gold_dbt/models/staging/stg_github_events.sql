{{
    config(
        materialized='incremental',
        schema='staging',
        incremental_strategy = 'merge',
        unique_key = 'event_id',
        partition_by={
            'field':'event_day',
            'data_type':'date'})
}}

SELECT
    id as event_id,
    type as event_type,
    actor_login,
    repo_name,
    created_at_ts,
    CAST(created_at_ts AS DATE) as event_day,
    TIMESTAMP_TRUNC(created_at_ts, 'HOUR') as event_hour
    FROM
    {{ source('dbo', 'github_events') }}
    WHERE
    1=1
    {% if is_incremental() %}
    AND created_at_ts >= TIMESTAMP_SUB(
        TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR),
        INTERVAL 1 HOUR
    )
    {% endif %}
