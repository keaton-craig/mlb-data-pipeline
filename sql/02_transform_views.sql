-- ============================================
-- View 1: Team Run Differential Leaderboard
-- ============================================

CREATE OR REPLACE VIEW `mlb-data-pipeline-489220.mlb_pipeline.team_run_diff_leaderboard` AS

SELECT
  team,
  COUNT(*) AS games_played,
  ROUND(AVG(run_diff),2) AS avg_run_diff
FROM (

  SELECT home_team AS team, run_diff
  FROM `mlb-data-pipeline-489220.mlb_pipeline.games_clean`

  UNION ALL

  SELECT away_team AS team, -run_diff AS run_diff
  FROM `mlb-data-pipeline-489220.mlb_pipeline.games_clean`

)
GROUP BY team
HAVING COUNT(*) > 150;


-- ============================================
-- View 2: Team Home vs Away Win Rates
-- ============================================

CREATE OR REPLACE VIEW `mlb-data-pipeline-489220.mlb_pipeline.team_home_away_win_rates` AS

WITH home_stats AS (

SELECT
  home_team AS team,
  COUNT(*) AS home_games,
  ROUND(SAFE_DIVIDE(SUM(CASE WHEN run_diff > 0 THEN 1 ELSE 0 END), COUNT(*)),3) AS home_win_rate
FROM `mlb-data-pipeline-489220.mlb_pipeline.games_clean`
GROUP BY home_team
HAVING COUNT(*) > 70

),

away_stats AS (

SELECT
  away_team AS team,
  COUNT(*) AS away_games,
  ROUND(SAFE_DIVIDE(SUM(CASE WHEN run_diff < 0 THEN 1 ELSE 0 END), COUNT(*)),3) AS away_win_rate
FROM `mlb-data-pipeline-489220.mlb_pipeline.games_clean`
GROUP BY away_team
HAVING COUNT(*) > 70

)

SELECT
  h.team,
  h.home_win_rate,
  a.away_win_rate
FROM home_stats h
JOIN away_stats a
ON h.team = a.team;


-- ============================================
-- View 3: Runs by Month
-- ============================================

CREATE OR REPLACE VIEW `mlb-data-pipeline-489220.mlb_pipeline.runs_by_month` AS

SELECT
  month,
  COUNT(*) AS games_played,
  ROUND(AVG(total_runs),2) AS avg_total_runs
FROM `mlb-data-pipeline-489220.mlb_pipeline.games_clean`
GROUP BY month;