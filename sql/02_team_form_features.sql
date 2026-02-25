-- Rolling team form features (NO leakage):
-- Uses past N games only (N=5 here), excluding current game.
-- Produces one row per team per game with engineered features + label.

CREATE OR REPLACE VIEW v_team_form AS
WITH g AS (
  SELECT
    game_id,
    season,
    game_date,
    team_id,
    opponent_team_id,
    is_home,
    wl,
    pts, fg_pct, fg3_pct, ft_pct, reb, ast, tov
  FROM v_games
)
SELECT
  *,
  CASE WHEN wl = 'W' THEN 1 ELSE 0 END AS y_win,

  date_diff('day',
    LAG(game_date) OVER (PARTITION BY team_id ORDER BY game_date),
    game_date
  ) AS rest_days,

  -- last-5 (excluding current)
  AVG(pts)     OVER (PARTITION BY team_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS pts_avg_l5,
  AVG(reb)     OVER (PARTITION BY team_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS reb_avg_l5,
  AVG(ast)     OVER (PARTITION BY team_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS ast_avg_l5,
  AVG(tov)     OVER (PARTITION BY team_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS tov_avg_l5,
  AVG(fg_pct)  OVER (PARTITION BY team_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS fg_pct_avg_l5,
  AVG(fg3_pct) OVER (PARTITION BY team_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS fg3_pct_avg_l5,
  AVG(ft_pct)  OVER (PARTITION BY team_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS ft_pct_avg_l5,
  AVG(CASE WHEN wl='W' THEN 1 ELSE 0 END)
              OVER (PARTITION BY team_id ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS winrate_l5
FROM g;
