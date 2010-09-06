SELECT
  *, LEFT(DATE_ADD(created_ts, INTERVAL (8 - DAYOFWEEK(created_ts)) DAY), 10) AS week_ending
FROM
  scorecard_scores
WHERE created_ts >= ? and created_ts < ? and _del='0'
ORDER BY created_ts desc, article_id asc
