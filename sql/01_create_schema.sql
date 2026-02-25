-- DuckDB SQL
-- Standard tables created by Python loader (src/features.py):
--
-- teams(team_id, full_name, abbreviation, city, state, year_founded, ...)
-- games(game_id, season, game_date, team_id, opponent_team_id, is_home, wl,
--       pts, fg_pct, fg3_pct, ft_pct, reb, ast, tov, ...)
--
-- Optional (if you provide the CSVs):
-- players(player_id, full_name, first_name, last_name, is_active)
-- player_games(player_id, game_id, season, game_date, team_id, opponent_team_id, is_home,
--              wl, minutes, pts, reb, ast, tov, plus_minus, fg_pct, fg3_pct, ft_pct, stl, blk, pf)
--
-- This file creates convenience views used by later scripts.

CREATE OR REPLACE VIEW v_games AS
SELECT
  game_id,
  season,
  CAST(game_date AS DATE) AS game_date,
  team_id,
  opponent_team_id,
  CAST(is_home AS BOOLEAN) AS is_home,
  wl,
  CAST(pts AS DOUBLE) AS pts,
  CAST(fg_pct AS DOUBLE) AS fg_pct,
  CAST(fg3_pct AS DOUBLE) AS fg3_pct,
  CAST(ft_pct AS DOUBLE) AS ft_pct,
  CAST(reb AS DOUBLE) AS reb,
  CAST(ast AS DOUBLE) AS ast,
  CAST(tov AS DOUBLE) AS tov
FROM games;

CREATE OR REPLACE VIEW v_teams AS
SELECT
  team_id,
  full_name,
  abbreviation,
  city,
  state,
  year_founded
FROM teams;

-- Optional views (created only if tables exist; if not, these CREATE VIEW
-- statements will fail, so they are executed conditionally from Python.)
