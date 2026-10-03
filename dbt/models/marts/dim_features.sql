SELECT DISTINCT
    feature_used AS feature_id,
    feature_used AS feature_name
FROM {{ ref('stg_events') }}
WHERE feature_used IS NOT NULL   
