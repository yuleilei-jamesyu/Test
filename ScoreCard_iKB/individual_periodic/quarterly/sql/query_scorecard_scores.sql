SELECT
  *
FROM
  scorecard_scores
WHERE created_ts >= ? and created_ts < ? and _del='0'
ORDER BY created_ts desc, article_id asc
