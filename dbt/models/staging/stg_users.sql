{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'bronze_events') }}
    WHERE data:email IS NOT NULL
),

renamed AS (
    SELECT
        data:user_id::VARCHAR        AS user_id,
        data:email::VARCHAR          AS email,
        data:plan_tier::VARCHAR      AS plan_tier,
        data:company_size::VARCHAR   AS company_size,
        TO_TIMESTAMP_NTZ(data:signup_ts::NUMBER, 3) AS signup_ts   
    FROM source
)

SELECT * FROM renamed   
