from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, Dict
import pandas as pd


@dataclass(frozen=True)
class StandardTables:
    teams: pd.DataFrame
    games: pd.DataFrame
    players: Optional[pd.DataFrame] = None
    player_games: Optional[pd.DataFrame] = None


def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def _pick(df: pd.DataFrame, candidates: Tuple[str, ...]) -> Optional[str]:
    cols = set(df.columns)
    for c in candidates:
        c2 = c.lower()
        if c2 in cols:
            return c2
    return None


def standardize_teams(teams_raw: pd.DataFrame) -> pd.DataFrame:
    df = _norm_cols(teams_raw)
    id_col = _pick(df, ("id", "team_id"))
    name_col = _pick(df, ("full_name", "team_name", "nickname"))
    abbr_col = _pick(df, ("abbreviation", "abbr"))
    city_col = _pick(df, ("city",))
    state_col = _pick(df, ("state",))
    year_col = _pick(df, ("year_founded", "founded"))

    if id_col is None:
        raise ValueError("Could not find a team id column in teams CSV. Expected one of: id/team_id.")

    out = pd.DataFrame({
        "team_id": df[id_col].astype("int64"),
        "full_name": df[name_col] if name_col else None,
        "abbreviation": df[abbr_col] if abbr_col else None,
        "city": df[city_col] if city_col else None,
        "state": df[state_col] if state_col else None,
        "year_founded": df[year_col] if year_col else None,
    })
    return out


def standardize_players(players_raw: pd.DataFrame) -> pd.DataFrame:
    df = _norm_cols(players_raw)
    id_col = _pick(df, ("id", "player_id"))
    if id_col is None:
        raise ValueError("Could not find player id column (id/player_id).")
    out = pd.DataFrame({
        "player_id": df[id_col].astype("int64"),
        "full_name": df[_pick(df, ("full_name",))] if _pick(df, ("full_name",)) else None,
        "first_name": df[_pick(df, ("first_name",))] if _pick(df, ("first_name",)) else None,
        "last_name": df[_pick(df, ("last_name",))] if _pick(df, ("last_name",)) else None,
        "is_active": df[_pick(df, ("is_active",))] if _pick(df, ("is_active",)) else None,
    })
    return out


def _derive_season_start_year(d: pd.Series) -> pd.Series:
    dt = pd.to_datetime(d, errors="coerce")
    return (dt.dt.year - (dt.dt.month < 10).astype(int)).astype("Int64")


def _parse_matchup(matchup: str) -> tuple[Optional[str], Optional[str], Optional[bool]]:
    if not isinstance(matchup, str):
        return None, None, None
    s = matchup.strip()
    parts = s.split()
    if len(parts) < 3:
        return None, None, None
    team = parts[0].upper()
    if "vs" in parts[1].lower():
        opp = parts[2].upper()
        return team, opp, True
    if parts[1] == "@":
        opp = parts[2].upper()
        return team, opp, False
    return None, None, None


def standardize_games(games_raw: pd.DataFrame, team_abbrev_to_id: Optional[Dict[str, int]] = None) -> pd.DataFrame:
    df = _norm_cols(games_raw)
    team_id_col = _pick(df, ("team_id",))
    wl_col = _pick(df, ("wl", "w_l"))
    matchup_col = _pick(df, ("matchup",))
    opp_id_col = _pick(df, ("opponent_team_id", "opp_team_id"))

    game_id_col = _pick(df, ("game_id", "id"))
    season_col = _pick(df, ("season", "season_id", "year"))
    date_col = _pick(df, ("game_date_real", "game_date_est", "game_date", "date"))

    if team_id_col is None:
        raise ValueError("Could not find TEAM_ID / team_id column in NBA_GAMES CSV.")
    if wl_col is None:
        raise ValueError("Could not find WL column in NBA_GAMES CSV.")
    if date_col is None:
        raise ValueError("Could not find a game date column (GAME_DATE_REAL / GAME_DATE / GAME_DATE_EST).")

    out = pd.DataFrame({
        "game_id": df[game_id_col].astype(str) if game_id_col else pd.RangeIndex(len(df)).astype(str),
        "game_date": pd.to_datetime(df[date_col], errors="coerce").dt.date,
        "team_id": df[team_id_col].astype("int64"),
        "wl": df[wl_col].astype(str).str.upper().str.strip(),
    })

    if season_col is not None:
        out["season"] = pd.to_numeric(df[season_col], errors="coerce").astype("Int64")
        if out["season"].isna().all():
            out["season"] = _derive_season_start_year(pd.to_datetime(df[date_col], errors="coerce"))
    else:
        out["season"] = _derive_season_start_year(pd.to_datetime(df[date_col], errors="coerce"))

    if opp_id_col is not None:
        out["opponent_team_id"] = pd.to_numeric(df[opp_id_col], errors="coerce").astype("Int64")
        is_home_col = _pick(df, ("is_home", "home"))
        out["is_home"] = df[is_home_col].astype(bool) if is_home_col else False
    else:
        if matchup_col is None:
            raise ValueError("No OPPONENT_TEAM_ID and no MATCHUP to parse opponent.")
        parsed = df[matchup_col].apply(_parse_matchup)
        out["is_home"] = parsed.apply(lambda t: t[2])
        opp_abbr = parsed.apply(lambda t: t[1])
        if team_abbrev_to_id is None:
            raise ValueError("Need team_abbrev_to_id mapping to convert opponent abbreviation -> opponent_team_id.")
        out["opponent_team_id"] = opp_abbr.map(lambda a: team_abbrev_to_id.get(str(a).upper()) if pd.notna(a) else None).astype("Int64")

    def add_stat(std_name: str, cand: Tuple[str, ...]):
        c = _pick(df, cand)
        out[std_name] = pd.to_numeric(df[c], errors="coerce") if c else None

    add_stat("pts", ("pts",))
    add_stat("fg_pct", ("fg_pct",))
    add_stat("fg3_pct", ("fg3_pct", "fg3_pct"))
    add_stat("ft_pct", ("ft_pct",))
    add_stat("reb", ("reb",))
    add_stat("ast", ("ast",))
    add_stat("tov", ("tov",))

    out = out.dropna(subset=["game_id", "game_date", "team_id", "opponent_team_id", "season"])
    out["is_home"] = out["is_home"].fillna(False).astype(bool)
    out["opponent_team_id"] = out["opponent_team_id"].astype("int64")
    out["season"] = out["season"].astype("int64")
    return out


def _min_to_float(x) -> Optional[float]:
    """Convert MIN column to float minutes.
    Accepts numeric or 'MM:SS' strings."""
    if pd.isna(x):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if ":" in s:
        mm, ss = s.split(":", 1)
        try:
            return float(mm) + float(ss) / 60.0
        except Exception:
            return None
    try:
        return float(s)
    except Exception:
        return None


def standardize_player_games(player_games_raw: pd.DataFrame, team_abbrev_to_id: Dict[str, int]) -> pd.DataFrame:
    df = _norm_cols(player_games_raw)

    player_id_col = _pick(df, ("player_id", "playerid", "player_id"))
    game_id_col = _pick(df, ("game_id", "id"))
    season_col = _pick(df, ("season_id", "season"))
    date_col = _pick(df, ("game_date_real", "game_date", "game_date_est"))
    matchup_col = _pick(df, ("matchup",))
    wl_col = _pick(df, ("wl", "w_l"))

    if player_id_col is None or game_id_col is None:
        raise ValueError("player_games must include Player_ID and Game_ID columns.")
    if date_col is None:
        raise ValueError("player_games must include GAME_DATE_REAL or GAME_DATE.")
    if matchup_col is None:
        raise ValueError("player_games must include MATCHUP for team/opponent parsing.")
    if wl_col is None:
        raise ValueError("player_games must include WL.")

    game_date = pd.to_datetime(df[date_col], errors="coerce")
    season = pd.to_numeric(df[season_col], errors="coerce").astype("Int64") if season_col else _derive_season_start_year(game_date)

    parsed = df[matchup_col].apply(_parse_matchup)
    team_abbr = parsed.apply(lambda t: t[0])
    opp_abbr = parsed.apply(lambda t: t[1])
    is_home = parsed.apply(lambda t: t[2])

    out = pd.DataFrame({
        "player_id": df[player_id_col].astype("int64"),
        "game_id": df[game_id_col].astype(str),
        "season": season.fillna(_derive_season_start_year(game_date)).astype("Int64"),
        "game_date": game_date.dt.date,
        "team_id": team_abbr.map(lambda a: team_abbrev_to_id.get(str(a).upper()) if pd.notna(a) else None).astype("Int64"),
        "opponent_team_id": opp_abbr.map(lambda a: team_abbrev_to_id.get(str(a).upper()) if pd.notna(a) else None).astype("Int64"),
        "is_home": is_home.fillna(False).astype(bool),
        "wl": df[wl_col].astype(str).str.upper().str.strip(),
    })

    # numeric stats (only those we use right now)
    def add_num(std_name: str, cand: Tuple[str, ...]):
        c = _pick(df, cand)
        out[std_name] = pd.to_numeric(df[c], errors="coerce") if c else None

    # minutes: special parser
    min_col = _pick(df, ("min", "minutes"))
    out["minutes"] = df[min_col].apply(_min_to_float) if min_col else None

    add_num("pts", ("pts",))
    add_num("reb", ("reb",))
    add_num("ast", ("ast",))
    add_num("tov", ("tov",))
    add_num("plus_minus", ("plus_minus",))
    add_num("fg_pct", ("fg_pct",))
    add_num("fg3_pct", ("fg3_pct", "fg3_pct"))
    add_num("ft_pct", ("ft_pct",))
    add_num("stl", ("stl",))
    add_num("blk", ("blk",))
    add_num("pf", ("pf",))

    out = out.dropna(subset=["player_id", "game_id", "game_date", "team_id", "opponent_team_id"])
    out["team_id"] = out["team_id"].astype("int64")
    out["opponent_team_id"] = out["opponent_team_id"].astype("int64")
    out["season"] = out["season"].astype("int64")
    return out


def build_standard_tables(
    teams_csv: str,
    games_csv: str,
    players_csv: Optional[str] = None,
    player_games_csv: Optional[str] = None
) -> StandardTables:
    teams_raw = pd.read_csv(teams_csv)
    games_raw = pd.read_csv(games_csv)

    teams = standardize_teams(teams_raw)
    mapping = {}
    if "abbreviation" in teams.columns and teams["abbreviation"].notna().any():
        mapping = dict(zip(teams["abbreviation"].astype(str).str.upper(), teams["team_id"].astype(int)))

    games = standardize_games(games_raw, team_abbrev_to_id=mapping)

    players = None
    player_games = None
    if players_csv and player_games_csv:
        players_raw = pd.read_csv(players_csv)
        pg_raw = pd.read_csv(player_games_csv)
        players = standardize_players(players_raw)
        player_games = standardize_player_games(pg_raw, team_abbrev_to_id=mapping)

    return StandardTables(teams=teams, games=games, players=players, player_games=player_games)
