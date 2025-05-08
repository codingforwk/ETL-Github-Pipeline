{{
    config(
        materialized='incremental',
        schema='staging',
        incremental_strategy = 'delete+insert',
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
    DATEADD(HOUR, DATEDIFF(HOUR, 0, created_at_ts), 0) as event_hour
FROM
    {{ source('dbo', 'github_events') }}
WHERE
    1=1
    {% if is_incremental() %}
    AND created_at_ts >= DATEADD(HOUR, -1, DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()), 0))
    {% endif %}
