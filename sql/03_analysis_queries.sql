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
-- Query 2: League Home vs Away Win Rate
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


-- ============================================
-- Query 3: Runs Scored by Month
-- ============================================

SELECT
  month,
  COUNT(*) AS games_played,
  ROUND(AVG(total_runs),2) AS avg_total_runs
FROM `mlb-data-pipeline-489220.mlb_pipeline.games_clean`
GROUP BY month
ORDER BY month;


-- ============================================
-- Query 4: Team Home Win Rate
-- ============================================

SELECT
  home_team,
  COUNT(*) AS home_games,
  SUM(CASE WHEN run_diff > 0 THEN 1 ELSE 0 END) AS home_wins,
  ROUND(
    SAFE_DIVIDE(
      SUM(CASE WHEN run_diff > 0 THEN 1 ELSE 0 END),
      COUNT(*)
    ),
    3
  ) AS home_win_rate
FROM `mlb-data-pipeline-489220.mlb_pipeline.games_clean`
GROUP BY home_team
HAVING COUNT(*) > 70
ORDER BY home_win_rate DESC;


-- ============================================
-- Query 5: Team Away Win Rate
-- ============================================

SELECT
  away_team,
  COUNT(*) AS away_games,
  SUM(CASE WHEN run_diff < 0 THEN 1 ELSE 0 END) AS away_wins,
   ROUND(
    SAFE_DIVIDE(
      SUM(CASE WHEN run_diff < 0 THEN 1 ELSE 0 END),
      COUNT(*)
    ),
    3
  ) AS away_win_rate
FROM `mlb-data-pipeline-489220.mlb_pipeline.games_clean`
GROUP BY away_team
HAVING COUNT(*) > 70
ORDER BY away_win_rate DESC;


-- ============================================
-- Query 6: Team Home VS Away Win Rate
-- ============================================

WITH home_stats AS (

SELECT
  home_team AS team,
  COUNT(*) AS home_games,
  SUM(CASE WHEN run_diff > 0 THEN 1 ELSE 0 END) AS home_wins,
  ROUND(
    SAFE_DIVIDE(
      SUM(CASE WHEN run_diff > 0 THEN 1 ELSE 0 END),
      COUNT(*)
    ),3
  ) AS home_win_rate
FROM `mlb-data-pipeline-489220.mlb_pipeline.games_clean`
GROUP BY home_team
HAVING COUNT(*) > 70

),

away_stats AS (

SELECT
  away_team AS team,
  COUNT(*) AS away_games,
  SUM(CASE WHEN run_diff < 0 THEN 1 ELSE 0 END) AS away_wins,
  ROUND(
    SAFE_DIVIDE(
      SUM(CASE WHEN run_diff < 0 THEN 1 ELSE 0 END),
      COUNT(*)
    ),3
  ) AS away_win_rate
FROM `mlb-data-pipeline-489220.mlb_pipeline.games_clean`
GROUP BY away_team
HAVING COUNT(*) > 70

)

SELECT
  h.team,
  h.home_games,
  h.home_win_rate,
  a.away_games,
  a.away_win_rate
FROM home_stats h
JOIN away_stats a
ON h.team = a.team
ORDER BY home_win_rate DESC;