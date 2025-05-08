{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key='repo_name',
        incremental_strategy='merge'
    )
}}


SELECT DISTINCT
    repo_name
FROM {{ ref('stg_github_events') }}
WHERE repo_name IS NOT NULL
{% if is_incremental() %}
  AND event_day >= DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
{% endif %}
