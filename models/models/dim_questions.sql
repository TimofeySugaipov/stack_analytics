{{ config(
      materialized='table'
    , partition_by= {
        "field": "creation_dt"
      , "data_type": "date"
      , "granularity": "month"
    }
    , cluster_by= ['question_id', 'accepted_answer_id', 'primary_tag', 'secondary_tag'])
}}

SELECT pq.question_id
     , pq.accepted_answer_id
     , pq.owner_user_id
     , pq.creation_dt
     , pq.last_activity_dt
     , pq.title
     , pq.body
     , pq.tags
     , pq.n_answers
     , pq.n_comments
     , pq.n_favourites
     , pq.n_votes
     , pq.n_views
     , IF(pq.accepted_answer_id IS NOT NULL, 1, 0) AS bool_has_been_resolved
     , IF(pq.n_answers > 0, 1, 0) AS bool_has_answers
     , pa.creation_dt AS question_resolved_dt
     , DATE_DIFF(pa.creation_dt, pq.creation_dt, DAY) AS days_to_resolution
     , IF(pq.n_views > bey.median_views, 1, 0) AS bool_is_popular_question
     , SPLIT(tags, '|')[SAFE_OFFSET(0)] AS primary_tag
     , SPLIT(tags, '|')[SAFE_OFFSET(1)] AS secondary_tag

FROM {{ ref('logs_posts_questions')}} pq

LEFT JOIN {{ ref('logs_posts_answers') }} pa
    ON pq.accepted_answer_id = pa.answer_id

LEFT JOIN {{ ref('stg_question_metric_benchmarks_yearly')}} bey
    ON DATE_TRUNC(pq.creation_dt, YEAR) = bey.year_start_dt