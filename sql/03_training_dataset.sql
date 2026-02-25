-- Join team form with opponent form for the same game.
-- Final dataset is at team-game grain: "Will THIS team win THIS game?"

CREATE OR REPLACE VIEW v_training_dataset AS
WITH tf AS (
  SELECT * FROM v_team_form
),
joined AS (
  SELECT
    a.game_id,
    a.season,
    a.game_date,
    a.team_id,
    a.opponent_team_id,
    a.is_home,
    a.y_win,

    -- team features
    a.rest_days,
    a.pts_avg_l5,
    a.reb_avg_l5,
    a.ast_avg_l5,
    a.tov_avg_l5,
    a.fg_pct_avg_l5,
    a.fg3_pct_avg_l5,
    a.ft_pct_avg_l5,
    a.winrate_l5,

    -- opponent features
    b.rest_days     AS opp_rest_days,
    b.pts_avg_l5    AS opp_pts_avg_l5,
    b.reb_avg_l5    AS opp_reb_avg_l5,
    b.ast_avg_l5    AS opp_ast_avg_l5,
    b.tov_avg_l5    AS opp_tov_avg_l5,
    b.fg_pct_avg_l5 AS opp_fg_pct_avg_l5,
    b.fg3_pct_avg_l5 AS opp_fg3_pct_avg_l5,
    b.ft_pct_avg_l5 AS opp_ft_pct_avg_l5,
    b.winrate_l5    AS opp_winrate_l5
  FROM tf a
  JOIN tf b
    ON a.game_id = b.game_id
   AND a.opponent_team_id = b.team_id
)
SELECT *
FROM joined
WHERE pts_avg_l5 IS NOT NULL
  AND opp_pts_avg_l5 IS NOT NULL
  AND winrate_l5 IS NOT NULL
  AND opp_winrate_l5 IS NOT NULL;
