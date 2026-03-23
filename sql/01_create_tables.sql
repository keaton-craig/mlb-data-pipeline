-- ============================================
-- Table: games_clean
-- Description: Cleaned MLB game-level dataset
-- ============================================

CREATE OR REPLACE TABLE `mlb-data-pipeline-489220.mlb_pipeline.games_clean` (

  game_id INT64,
  game_date DATE,
  season INT64,

  home_team STRING,
  away_team STRING,

  home_score INT64,
  away_score INT64,

  run_diff INT64,
  total_runs INT64,

  winner_team STRING,
  is_one_run_game BOOL,

  month INT64

);

-- Note:
-- This table is populated via the Python ETL pipeline (load_bigquery.py).
-- This script documents the intended schema for reproducibility.