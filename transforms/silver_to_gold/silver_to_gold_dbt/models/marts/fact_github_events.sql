{{ config(
    materialized='incremental',
    schema='gold',
    unique_key='event_id',
    incremental_strategy='delete+insert',
    partition_by={'field': 'event_day', 'data_type': 'date'}
) }}

SELECT
    stg.event_id,
    dim_actor.actor_login,
    stg.event_type,
    dim_type.event_category,
    dim_repo.repo_name,
    stg.created_at_ts,
    stg.event_day,
    stg.event_hour
FROM {{ ref('stg_github_events') }} stg
LEFT JOIN {{ ref('dim_actors') }} dim_actor
    ON stg.actor_login = dim_actor.actor_login
LEFT JOIN {{ ref('dim_event_types') }} dim_type
    ON stg.event_type = dim_type.event_type
LEFT JOIN {{ ref('dim_repos') }} dim_repo
    ON stg.repo_name = dim_repo.repo_name
{% if is_incremental() %}
WHERE stg.created_at_ts >= DATEADD(HOUR, -1, DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()), 0))
{% endif %}
