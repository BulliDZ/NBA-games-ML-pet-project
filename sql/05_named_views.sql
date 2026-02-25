-- Adds human-readable columns to the training dataset (for BI / Streamlit display).
-- Keeps numeric features intact but replaces IDs with team names/abbreviations.

CREATE OR REPLACE VIEW v_training_dataset_named AS
SELECT
  d.game_id,
  d.season,
  d.game_date,
  d.is_home,
  d.y_win,

  t.full_name      AS team_name,
  t.abbreviation   AS team_abbr,
  o.full_name      AS opponent_name,
  o.abbreviation   AS opponent_abbr,

  -- numeric features (team)
  d.rest_days,
  d.pts_avg_l5,
  d.reb_avg_l5,
  d.ast_avg_l5,
  d.tov_avg_l5,
  d.fg_pct_avg_l5,
  d.fg3_pct_avg_l5,
  d.ft_pct_avg_l5,
  d.winrate_l5,

  -- numeric features (opponent)
  d.opp_rest_days,
  d.opp_pts_avg_l5,
  d.opp_reb_avg_l5,
  d.opp_ast_avg_l5,
  d.opp_tov_avg_l5,
  d.opp_fg_pct_avg_l5,
  d.opp_fg3_pct_avg_l5,
  d.opp_ft_pct_avg_l5,
  d.opp_winrate_l5,

  -- keep IDs at the end (optional)
  d.team_id,
  d.opponent_team_id
FROM v_training_dataset d
JOIN v_teams t ON t.team_id = d.team_id
JOIN v_teams o ON o.team_id = d.opponent_team_id;

-- Same as above but for enriched dataset (if player tables exist).
CREATE OR REPLACE VIEW v_training_dataset_enriched_named AS
SELECT
  d.game_id,
  d.season,
  d.game_date,
  d.is_home,
  d.y_win,

  t.full_name      AS team_name,
  t.abbreviation   AS team_abbr,
  o.full_name      AS opponent_name,
  o.abbreviation   AS opponent_abbr,

  d.rest_days,
  d.pts_avg_l5,
  d.reb_avg_l5,
  d.ast_avg_l5,
  d.tov_avg_l5,
  d.fg_pct_avg_l5,
  d.fg3_pct_avg_l5,
  d.ft_pct_avg_l5,
  d.winrate_l5,

  d.star_pts_avg_l5_sum,
  d.star_min_avg_l5_avg,
  d.star_pm_avg_l5_avg,

  d.opp_rest_days,
  d.opp_pts_avg_l5,
  d.opp_reb_avg_l5,
  d.opp_ast_avg_l5,
  d.opp_tov_avg_l5,
  d.opp_fg_pct_avg_l5,
  d.opp_fg3_pct_avg_l5,
  d.opp_ft_pct_avg_l5,
  d.opp_winrate_l5,

  d.opp_star_pts_avg_l5_sum,
  d.opp_star_min_avg_l5_avg,
  d.opp_star_pm_avg_l5_avg,

  d.team_id,
  d.opponent_team_id
FROM v_training_dataset_enriched d
JOIN v_teams t ON t.team_id = d.team_id
JOIN v_teams o ON o.team_id = d.opponent_team_id;
