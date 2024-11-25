{{ config(
      materialized='table'
    , partition_by= {
        "field": "creation_dt"
      , "data_type": "date"
      , "granularity": "month"
    }
    , cluster_by= ['question_id', 'accepted_answer_id', 'tags', 'owner_user_id'])
}}

/* 
    This model extracts the questions from the Stack Overflow dataset.
    This model is necessary as it provides a more optimised version of the questions table and will make exploration cheaper and faster.
*/

SELECT id AS question_id
     , owner_user_id
     , post_type_id
     , DATE(creation_date) AS creation_dt
     , DATE(last_activity_date) AS last_activity_dt
     , title
     , body
     , tags
     , answer_count AS n_answers
     , comment_count AS n_comments
     , favorite_count AS n_favourites
     , score AS n_votes
     , view_count AS n_views

FROM `bigquery-public-data.stackoverflow.posts_questions`