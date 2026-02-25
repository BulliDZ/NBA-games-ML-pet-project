import json
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

try:
    import joblib  # optional for live predictions from the model
except Exception:
    joblib = None

def list_views(con) -> set[str]:
    df = con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'VIEW'
    """).df()
    return set(df["table_name"].tolist())

st.set_page_config(page_title="Matchup Win Probability (NBA)", layout="wide")
st.title("Matchup Win Probability (NBA)")
st.caption("Select two teams, choose one of their games, and compare recent form (last 5 games).")

st.sidebar.header("Files")
db_path = st.sidebar.text_input("DuckDB path", "nba.duckdb")
model_path = st.sidebar.text_input("Model bundle (optional)", "artifacts/model.joblib")

glossary_path = Path(__file__).parent / "feature_glossary.json"
glossary = json.loads(glossary_path.read_text(encoding="utf-8")) if glossary_path.exists() else {}

# Connect DB and load teams
try:
    con = duckdb.connect(db_path)
    teams = con.execute("SELECT team_id, full_name, abbreviation FROM v_teams ORDER BY full_name").df()
except Exception as e:
    st.sidebar.error(f"Failed to open DuckDB or read v_teams: {e}")
    st.stop()

name_opts = teams.apply(lambda r: f"{r['full_name']} ({r['abbreviation']})", axis=1).tolist()
id_opts = teams["team_id"].astype(int).tolist()
label_to_id = dict(zip(name_opts, id_opts))
id_to_abbr = dict(zip(id_opts, teams["abbreviation"].astype(str).tolist()))
id_to_name = dict(zip(id_opts, teams["full_name"].astype(str).tolist()))

st.sidebar.header("Pick teams")
team_a_label = st.sidebar.selectbox("Team A", name_opts, index=0)
team_b_label = st.sidebar.selectbox("Team B", name_opts, index=1 if len(name_opts) > 1 else 0)

team_a = label_to_id[team_a_label]
team_b = label_to_id[team_b_label]

if team_a == team_b:
    st.warning("Please select two different teams.")
    st.stop()

# Determine dataset view
views = list_views(con)
dataset_view = "v_training_dataset_enriched" if "v_training_dataset_enriched" in views else "v_training_dataset"

# Find head-to-head games (Team A perspective)
games_ab = con.execute(
    """
    SELECT game_id, game_date, is_home, wl
    FROM v_games
    WHERE team_id = ? AND opponent_team_id = ?
    ORDER BY game_date DESC
    """,
    [team_a, team_b]
).df()

if games_ab.empty:
    st.info("No games found between these teams in the dataset (Team A vs Team B). Try another pairing.")
    st.stop()

def fmt_game_row(r):
    loc = "Home" if bool(r["is_home"]) else "Away"
    res = str(r["wl"]).strip().upper()
    dt = pd.to_datetime(r["game_date"]).date()
    return f"{dt} — {loc} — Result: {res} — game_id={r['game_id']}"

game_labels = games_ab.apply(fmt_game_row, axis=1).tolist()
selected_game_label = st.sidebar.selectbox("Choose a game between them", game_labels, index=0)
selected_game_id = str(games_ab.iloc[game_labels.index(selected_game_label)]["game_id"])

# Pull feature rows for this game (both perspectives)
feat_a = con.execute(
    f"SELECT * FROM {dataset_view} WHERE game_id = ? AND team_id = ?",
    [selected_game_id, team_a]
).df()
feat_b = con.execute(
    f"SELECT * FROM {dataset_view} WHERE game_id = ? AND team_id = ?",
    [selected_game_id, team_b]
).df()

if feat_a.empty or feat_b.empty:
    st.warning("Could not find feature rows for both teams for this game (often happens early season before rolling features exist).")
    st.stop()

a_name = id_to_name.get(team_a, str(team_a))
b_name = id_to_name.get(team_b, str(team_b))
a_abbr = id_to_abbr.get(team_a, "")
b_abbr = id_to_abbr.get(team_b, "")

is_home_a = bool(feat_a.iloc[0]["is_home"])
matchup_title = f"{a_name} ({a_abbr}) vs {b_name} ({b_abbr})" if is_home_a else f"{a_name} ({a_abbr}) @ {b_name} ({b_abbr})"
game_date = pd.to_datetime(feat_a.iloc[0]["game_date"]).date()
actual_a = int(feat_a.iloc[0]["y_win"])

left, right = st.columns([2, 1])
with left:
    st.subheader(matchup_title)
    st.write({"date": str(game_date)})
with right:
    st.write(f"Actual result for Team A: {'WIN ✅' if actual_a == 1 else 'LOSS ❌'}")

# Live probability (optional)
proba_a = None
proba_b = None
if joblib is not None and Path(model_path).exists():
    try:
        bundle = joblib.load(model_path)
        model = bundle["model"]
        feature_cols = bundle["feature_cols"]

        Xa = feat_a[feature_cols]
        Xb = feat_b[feature_cols]
        proba_a = float(model.predict_proba(Xa)[:, 1][0])
        proba_b = float(model.predict_proba(Xb)[:, 1][0])
    except Exception as e:
        st.sidebar.warning(f"Could not compute live probabilities from model: {e}")

if proba_a is not None and proba_b is not None:
    denom = proba_a + proba_b
    pA = proba_a / denom if denom > 0 else proba_a
    pB = proba_b / denom if denom > 0 else proba_b

    m1, m2 = st.columns(2)
    with m1:
        st.metric(f"{a_abbr} win probability", f"{pA:.2%}")
    with m2:
        st.metric(f"{b_abbr} win probability", f"{pB:.2%}")
else:
    st.info("Train the model first to show probabilities here (`python src/train.py ...` creates artifacts/model.joblib).")

st.markdown("---")
st.subheader("Comparison (pre-game form, last 5 games)")
st.caption("These indicators are computed **before the game** using only the previous 5 games (no leakage).")

ignore = {"game_id", "season", "game_date", "team_id", "opponent_team_id", "y_win", "is_home"}
default_features = [
    "winrate_l5", "pts_avg_l5", "fg_pct_avg_l5", "fg3_pct_avg_l5",
    "tov_avg_l5", "reb_avg_l5", "ast_avg_l5", "rest_days"
]
for f in ["star_pts_avg_l5_sum", "star_min_avg_l5_avg", "star_pm_avg_l5_avg"]:
    if f in feat_a.columns:
        default_features.append(f)

available = [c for c in feat_a.columns if c not in ignore and not c.startswith("opp_")]
# put defaults first
available = [c for c in default_features if c in available] + [c for c in available if c not in default_features]

chosen = st.multiselect(
    "Select indicators to show",
    options=available,
    default=[f for f in default_features if f in available]
)

a = feat_a.iloc[0].to_dict()
b = feat_b.iloc[0].to_dict()

rows = []
for k in chosen:
    av = a.get(k)
    bv = b.get(k)
    diff = None
    try:
        if pd.notna(av) and pd.notna(bv):
            diff = float(av) - float(bv)
    except Exception:
        diff = None

    rows.append({
        "Indicator": k,
        "Explanation": glossary.get(k, ""),
        f"{a_abbr}": av,
        f"{b_abbr}": bv,
        "Difference (A - B)": diff
    })

table = pd.DataFrame(rows)
if not table.empty and "Difference (A - B)" in table.columns:
    table = table.assign(_abs=table["Difference (A - B)"].abs())
    table = table.sort_values("_abs", ascending=False).drop(columns=["_abs"])

st.dataframe(table, use_container_width=True)

st.markdown("---")
st.subheader("Head-to-head summary (from Team A perspective)")
h2h = con.execute(
    """
    SELECT
      COUNT(*) AS games_count,
      SUM(CASE WHEN wl='W' THEN 1 ELSE 0 END) AS team_a_wins,
      SUM(CASE WHEN wl='L' THEN 1 ELSE 0 END) AS team_a_losses
    FROM v_games
    WHERE team_id = ? AND opponent_team_id = ?
    """,
    [team_a, team_b]
).df().iloc[0].to_dict()

st.write({
    "games_in_dataset": int(h2h["games_count"]),
    "team_a_wins": int(h2h["team_a_wins"]),
    "team_a_losses": int(h2h["team_a_losses"]),
})

with st.expander("Show raw feature rows (for analysts)"):
    st.write("Team A feature row")
    st.dataframe(feat_a, use_container_width=True)
    st.write("Team B feature row")
    st.dataframe(feat_b, use_container_width=True)
