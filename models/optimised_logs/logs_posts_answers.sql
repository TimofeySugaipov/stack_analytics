{{ config(
      materialized='table'
    , partition_by= {
        "field": "creation_dt"
      , "data_type": "date"
      , "granularity": "month"
    }
    , cluster_by= ['answer_id', 'question_id', 'answer_owner_user_id', 'post_type_id'])
}}

/* 
    This model extracts the answers from the Stack Overflow dataset.
    This model is necessary as it provides a more optimised version of the questions table and will make exploration cheaper and faster.
*/

SELECT id AS answer_id
     , parent_id AS question_id
     , last_editor_user_id AS answer_owner_user_id
     , post_type_id
     , DATE(creation_date) AS creation_dt
     , DATE(last_activity_date) AS last_activity_dt
     , body
     , comment_count AS n_comments
     , score AS n_votes

FROM {{ source('raw_logs', 'posts_answers') }}