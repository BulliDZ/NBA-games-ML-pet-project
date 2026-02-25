-- A few “portfolio-style” analytics queries

-- 1) Season standings
SELECT
  season,
  t.full_name AS team_name,
  SUM(CASE WHEN wl='W' THEN 1 ELSE 0 END) AS wins,
  SUM(CASE WHEN wl='L' THEN 1 ELSE 0 END) AS losses,
  ROUND(1.0 * SUM(CASE WHEN wl='W' THEN 1 ELSE 0 END) / COUNT(*), 3) AS win_pct
FROM v_games g
JOIN v_teams t ON t.team_id = g.team_id
GROUP BY 1,2
ORDER BY season DESC, win_pct DESC;

-- 2) Head-to-head win rate (unordered pair)
WITH h2h AS (
  SELECT
    LEAST(team_id, opponent_team_id) AS t1,
    GREATEST(team_id, opponent_team_id) AS t2,
    CASE WHEN wl='W' THEN 1 ELSE 0 END AS is_win
  FROM v_games
)
SELECT
  a.full_name AS team_a,
  b.full_name AS team_b,
  SUM(is_win) AS wins_team_a_side,
  COUNT(*) - SUM(is_win) AS losses_team_a_side,
  ROUND(1.0 * SUM(is_win)/COUNT(*), 3) AS winrate_team_a_side
FROM h2h
JOIN v_teams a ON a.team_id = h2h.t1
JOIN v_teams b ON b.team_id = h2h.t2
GROUP BY 1,2
ORDER BY winrate_team_a_side DESC;
