{{ config(
      materialized='table'
    , partition_by= {
        "field": "year_start_dt"
      , "data_type": "date"
      , "granularity": "year"
    })
}}

/*
This model is used to calculate the yearly view benchmarks for questions.
The distribution seems to vary from year to year, so we will use the yearly median as a benchmark to determine if a question is popular or not.
It should be noted that there is still activity on posts after the year they were created, however the majority of the activity occurs in the year the post was created.
Therefore this model is still able to provide a good benchmark for the popularity of a question.
*/

SELECT DATE_TRUNC(creation_dt, YEAR) AS year_start_dt
     , COUNT(DISTINCT question_id) AS n_questions
     , APPROX_QUANTILES(n_views, 100)[SAFE_OFFSET(50)] AS median_views

FROM {{ ref('logs_posts_questions') }}

GROUP BY 1

