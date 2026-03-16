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