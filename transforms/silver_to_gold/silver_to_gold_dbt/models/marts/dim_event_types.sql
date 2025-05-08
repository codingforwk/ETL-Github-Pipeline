{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT DISTINCT
    event_type,
    {{ categorize_event_type('event_type') }} as event_category

FROM {{ ref('stg_github_events') }}