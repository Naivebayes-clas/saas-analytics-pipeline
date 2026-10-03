{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_events') }}
    WHERE data:event_type IS NOT NULL
),

renamed AS (
    SELECT
        data:event_id::VARCHAR       AS event_id,
        data:user_id::VARCHAR        AS user_id,
        data:event_type::VARCHAR     AS event_type,
        data:feature_used::VARCHAR   AS feature_used,
        data:plan_tier::VARCHAR      AS plan_tier,
        data:session_id::VARCHAR     AS session_id,
        data:page_url::VARCHAR       AS page_url,
        data:device_type::VARCHAR    AS device_type,
        data:country::VARCHAR        AS country,
        TO_TIMESTAMP_NTZ(data:event_ts::NUMBER, 3) AS event_ts   
    FROM source
)

SELECT * FROM renamed   
