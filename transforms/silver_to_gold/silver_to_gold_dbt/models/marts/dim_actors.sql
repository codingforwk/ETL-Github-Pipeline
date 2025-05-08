{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key='actor_login',
        incremental_strategy='merge'
    )
}}


SELECT DISTINCT
    actor_login
FROM {{ ref('stg_github_events') }}
WHERE actor_login IS NOT NULL
{% if is_incremental() %}
  AND event_day >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
{% endif %}