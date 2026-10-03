{{ config(
    materialized='table',
    tests=[
        {'unique': 'user_id'},
        {'not_null': ['user_id', 'email', 'plan_tier']},
    ]
) }}   

WITH source AS (
    SELECT * FROM {{ ref('stg_users') }}
)

SELECT
    user_id,
    email,
    plan_tier,
    company_size,
    signup_ts
FROM source   
