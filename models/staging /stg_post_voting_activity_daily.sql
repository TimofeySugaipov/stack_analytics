{{ config(
      materialized='table'
    , partition_by= {
        "field": "vote_dt"
      , "data_type": "date"
      , "granularity": "month"
    }
    , cluster_by= ['post_id'])
}}

SELECT vote_dt
     , post_id
     , COUNT(DISTINCT vote_id) AS n_votes
     , COUNT(DISTINCT IF(vote_type_id = 2, vote_id, NULL)) AS n_upvotes
     , COUNT(DISTINCT IF(vote_type_id = 3, vote_id, NULL)) AS n_downvotes


FROM {{ ref('logs_posts_votes') }}

GROUP BY 1,2