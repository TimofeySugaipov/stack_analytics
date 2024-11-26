{{ config(
      materialized='table'
    , partition_by= {
        "field": "vote_dt"
      , "data_type": "date"
      , "granularity": "month"
    }
    , cluster_by= ['question_id', 'bool_has_answers', 'bool_is_trending', 'primary_tag'])
}}

WITH rolling_sums AS (

  SELECT vote_dt
       , post_id
       , n_votes
       , n_upvotes
       , n_downvotes
       , SUM(n_votes) OVER(PARTITION BY post_id ORDER BY vote_dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_votes
       , SUM(n_votes) OVER(PARTITION BY post_id ORDER BY vote_dt ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) AS prev_rolling_7d_votes
         -- This is used to make sure that the vote uptrend does not highlight a post with a poor community sentiment
       , SUM(COALESCE(n_upvotes, 0) - COALESCE(n_downvotes, 0)) OVER(PARTITION BY post_id ORDER BY vote_dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_up_vs_downvotes

  FROM {{ ref('stg_post_voting_activity_daily') }}

)

SELECT rs.vote_dt
     , rs.post_id AS question_id
     , rs.n_votes
     , rs.n_upvotes
     , rs.n_downvotes
     , rs.rolling_7d_votes
     , rs.prev_rolling_7d_votes
     , rs.rolling_7d_up_vs_downvotes
     , IF(rs.rolling_7d_votes > rs.prev_rolling_7d_votes AND rs.rolling_7d_up_vs_downvotes >= 0, 1, 0) AS bool_is_trending
     , pq.primary_tag
     , pq.secondary_tag
     , pq.bool_is_popular_question
     , pq.bool_has_been_resolved
     , pq.bool_has_answers

FROM rolling_sums rs

-- Filtering using INNER to include only question posts
INNER JOIN {{ ref('dim_questions') }} pq
  ON rs.post_id = pq.question_id