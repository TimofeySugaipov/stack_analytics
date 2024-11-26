{{ config(
      materialized='table'
    , partition_by= {
        "field": "vote_dt"
      , "data_type": "date"
      , "granularity": "month"
    }
    , cluster_by= ['vote_id', 'post_id', 'vote_type_id'])
}}

/* 
    This model extracts the questions from the Stack Overflow dataset.
    This model is necessary as it provides a more optimised version of the questions table and will make exploration cheaper and faster.
*/

SELECT id AS vote_id
     , DATE(creation_date) AS vote_dt
     , post_id
     , vote_type_id

FROM {{ source('raw_logs', 'votes') }}