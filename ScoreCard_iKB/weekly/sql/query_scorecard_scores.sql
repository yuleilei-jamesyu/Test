SELECT * FROM scorecard_scores WHERE created_ts >= ? and created_ts < ? and _del='0' ORDER BY article_id asc, created_ts asc
