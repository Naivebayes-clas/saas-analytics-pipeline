{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ ref('stg_events') }}
)

SELECT
    event_id,
    session_id,
    user_id,
    event_type,
    feature_used,
    plan_tier,
    page_url,
    device_type,
    country,
    event_ts
FROM source   
