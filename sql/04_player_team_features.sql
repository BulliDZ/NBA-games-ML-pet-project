-- Player-informed team features (NO leakage):
-- 1) v_player_form: rolling last-5 form per player, excluding current game
-- 2) v_team_star_features: for each team-game, aggregate top-3 players by pts_avg_l5
-- 3) v_training_dataset_enriched: join star features for team + opponent

CREATE OR REPLACE VIEW v_player_games AS
SELECT
  player_id,
  game_id,
  season,
  CAST(game_date AS DATE) AS game_date,
  team_id,
  opponent_team_id,
  CAST(is_home AS BOOLEAN) AS is_home,
  wl,
  CAST(minutes AS DOUBLE) AS minutes,
  CAST(pts AS DOUBLE) AS pts,
  CAST(reb AS DOUBLE) AS reb,
  CAST(ast AS DOUBLE) AS ast,
  CAST(tov AS DOUBLE) AS tov,
  CAST(plus_minus AS DOUBLE) AS plus_minus,
  CAST(fg_pct AS DOUBLE) AS fg_pct,
  CAST(fg3_pct AS DOUBLE) AS fg3_pct,
  CAST(ft_pct AS DOUBLE) AS ft_pct,
  CAST(stl AS DOUBLE) AS stl,
  CAST(blk AS DOUBLE) AS blk,
  CAST(pf AS DOUBLE) AS pf
FROM player_games;

CREATE OR REPLACE VIEW v_players AS
SELECT
  player_id,
  full_name,
  first_name,
  last_name,
  is_active
FROM players;

CREATE OR REPLACE VIEW v_player_form AS
SELECT
  *,
  AVG(minutes) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS min_avg_l5,
  AVG(pts)     OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS pts_avg_l5,
  AVG(plus_minus) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS pm_avg_l5
FROM v_player_games;

-- For each team in each game, choose "top 3" players by their *pre-game* scoring form (pts_avg_l5)
CREATE OR REPLACE VIEW v_team_star_features AS
WITH ranked AS (
  SELECT
    game_id,
    team_id,
    opponent_team_id,
    game_date,
    player_id,
    pts_avg_l5,
    min_avg_l5,
    pm_avg_l5,
    ROW_NUMBER() OVER (
      PARTITION BY game_id, team_id
      ORDER BY pts_avg_l5 DESC NULLS LAST
    ) AS rn
  FROM v_player_form
),
stars AS (
  SELECT * FROM ranked WHERE rn <= 3
)
SELECT
  game_id,
  team_id,
  opponent_team_id,
  game_date,
  SUM(pts_avg_l5) AS star_pts_avg_l5_sum,
  AVG(min_avg_l5) AS star_min_avg_l5_avg,
  AVG(pm_avg_l5)  AS star_pm_avg_l5_avg
FROM stars
GROUP BY 1,2,3,4;

-- Enrich base training dataset with player-driven "star form" for team + opponent
CREATE OR REPLACE VIEW v_training_dataset_enriched AS
WITH base AS (
  SELECT * FROM v_training_dataset
),
team_star AS (
  SELECT * FROM v_team_star_features
)
SELECT
  b.*,

  ts.star_pts_avg_l5_sum AS star_pts_avg_l5_sum,
  ts.star_min_avg_l5_avg AS star_min_avg_l5_avg,
  ts.star_pm_avg_l5_avg  AS star_pm_avg_l5_avg,

  ots.star_pts_avg_l5_sum AS opp_star_pts_avg_l5_sum,
  ots.star_min_avg_l5_avg AS opp_star_min_avg_l5_avg,
  ots.star_pm_avg_l5_avg  AS opp_star_pm_avg_l5_avg
FROM base b
LEFT JOIN team_star ts
  ON b.game_id = ts.game_id AND b.team_id = ts.team_id
LEFT JOIN team_star ots
  ON b.game_id = ots.game_id AND b.opponent_team_id = ots.team_id;
