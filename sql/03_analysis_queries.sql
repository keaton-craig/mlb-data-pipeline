-- ============================================
-- Query 1: Team Run Differential Leaderboard
-- ============================================

SELECT
  team,
  COUNT(*) AS games_played,
  ROUND(AVG(run_diff),2) AS avg_run_diff
FROM (

  SELECT
    home_team AS team,
    run_diff
  FROM `mlb-data-pipeline-489220.mlb_pipeline.games_clean`

  UNION ALL

  SELECT
    away_team AS team,
    -run_diff AS run_diff
  FROM `mlb-data-pipeline-489220.mlb_pipeline.games_clean`
)
GROUP BY team
HAVING COUNT(*) > 150
ORDER BY avg_run_diff DESC;


-- ============================================
-- Query 2: Home vs Away Win Rate
-- ============================================

SELECT
  winner_location,
  COUNT(*) AS wins,
  SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER()) AS win_rate
FROM (
  SELECT
    CASE
      WHEN home_score > away_score THEN 'home'
      ELSE 'away'
    END AS winner_location
  FROM `mlb-data-pipeline-489220.mlb_pipeline.games_clean`
)
GROUP BY winner_location;