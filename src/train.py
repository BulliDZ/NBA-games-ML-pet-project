from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline

from features import build_standard_tables

BASE_SQL_ORDER = [
    "sql/01_create_schema.sql",
    "sql/02_team_form_features.sql",
    "sql/03_training_dataset.sql",
]

PLAYER_SQL_ORDER = [
    "sql/04_player_team_features.sql",
]

NAMED_VIEWS_SQL = "sql/05_named_views.sql"


def time_split(
    df: pd.DataFrame,
    date_col: str = "game_date",
    test_size: float = 0.2,
    val_size: float = 0.1,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = df.sort_values(date_col).reset_index(drop=True)
    n = len(df)
    n_test = int(round(n * test_size))
    n_val = int(round(n * val_size))

    test = df.iloc[-n_test:]
    val = df.iloc[-(n_test + n_val) : -n_test] if n_val > 0 else df.iloc[0:0]
    train = df.iloc[: -(n_test + n_val)]
    return train, val, test


def evaluate(model, X: pd.DataFrame, y: pd.Series, name: str) -> dict:
    proba = model.predict_proba(X)[:, 1]
    preds = (proba >= 0.5).astype(int)
    return {
        f"{name}_roc_auc": float(roc_auc_score(y, proba)) if len(np.unique(y)) > 1 else None,
        f"{name}_logloss": float(log_loss(y, proba)),
        f"{name}_accuracy": float(accuracy_score(y, preds)),
    }


def list_views(con: duckdb.DuckDBPyConnection) -> set[str]:
    """
    DuckDB-portable view listing (works even when SHOW VIEWS is unsupported).
    """
    df = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_type = 'VIEW'
        """
    ).df()
    return set(df["table_name"].tolist())


def _create_named_views(con: duckdb.DuckDBPyConnection, root: Path, has_players: bool) -> None:
    """
    Executes sql/05_named_views.sql safely.

    Always creates:
      - v_training_dataset_named

    Creates only when available:
      - v_training_dataset_enriched_named (requires player enrichment + enriched dataset view)
    """
    sql_path = root / NAMED_VIEWS_SQL
    if not sql_path.exists():
        return

    sql_text = sql_path.read_text(encoding="utf-8")

    marker = "CREATE OR REPLACE VIEW v_training_dataset_enriched_named AS"
    parts = sql_text.split(marker)

    # base named view (always safe)
    con.execute(parts[0])

    # enriched named view only if players present AND enriched dataset exists
    if has_players and len(parts) > 1:
        views = list_views(con)
        if "v_training_dataset_enriched" in views:
            con.execute(marker + parts[1])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data", help="Folder with NBA_*.csv files")
    ap.add_argument("--db-path", default="nba.duckdb", help="DuckDB file path (created if missing)")
    ap.add_argument("--artifacts-dir", default="artifacts", help="Where to save model & outputs")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    artifacts_dir = Path(args.artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    teams_csv = data_dir / "NBA_TEAMS.csv"
    games_csv = data_dir / "NBA_GAMES.csv"
    players_csv = data_dir / "NBA_PLAYERS.csv"
    player_games_csv = data_dir / "NBA_PLAYER_GAMES.csv"

    if not teams_csv.exists() or not games_csv.exists():
        raise SystemExit(
            f"Missing CSVs. Expected:\n  {teams_csv}\n  {games_csv}\n"
            "Download from Kaggle and place them into the data/ folder."
        )

    has_players = players_csv.exists() and player_games_csv.exists()

    std = build_standard_tables(
        str(teams_csv),
        str(games_csv),
        str(players_csv) if has_players else None,
        str(player_games_csv) if has_players else None,
    )

    con = duckdb.connect(str(args.db_path))

    # Recreate tables each run
    con.execute("DROP TABLE IF EXISTS teams")
    con.execute("DROP TABLE IF EXISTS games")
    con.execute("DROP TABLE IF EXISTS players")
    con.execute("DROP TABLE IF EXISTS player_games")

    con.register("teams_df", std.teams)
    con.register("games_df", std.games)
    con.execute("CREATE TABLE teams AS SELECT * FROM teams_df")
    con.execute("CREATE TABLE games AS SELECT * FROM games_df")

    if has_players and std.players is not None and std.player_games is not None:
        con.register("players_df", std.players)
        con.register("player_games_df", std.player_games)
        con.execute("CREATE TABLE players AS SELECT * FROM players_df")
        con.execute("CREATE TABLE player_games AS SELECT * FROM player_games_df")

    root = Path(__file__).resolve().parents[1]

    # 1) Base SQL pipeline
    for rel in BASE_SQL_ORDER:
        sql_text = (root / rel).read_text(encoding="utf-8")
        con.execute(sql_text)

    # 2) Player enrichment SQL
    if has_players:
        for rel in PLAYER_SQL_ORDER:
            sql_text = (root / rel).read_text(encoding="utf-8")
            con.execute(sql_text)

    # 3) Named views for UI/BI
    _create_named_views(con, root, has_players)

    # Train on numeric dataset view
    view_name = "v_training_dataset_enriched" if has_players else "v_training_dataset"

    df = con.execute(f"SELECT * FROM {view_name}").df()
    df["game_date"] = pd.to_datetime(df["game_date"])

    target = "y_win"
    drop_cols = ["game_id", "season", "game_date", "team_id", "opponent_team_id", target]
    feature_cols = [c for c in df.columns if c not in drop_cols]

    train_df, val_df, test_df = time_split(df, "game_date", test_size=0.2, val_size=0.1)

    X_train, y_train = train_df[feature_cols], train_df[target]
    X_val, y_val = val_df[feature_cols], val_df[target]
    X_test, y_test = test_df[feature_cols], test_df[target]

    # Linear regression
    lr = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("model", LogisticRegression(max_iter=2000)),
        ]
    )

    # Hist Gradient Boosting classifier
    hgb = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("model", HistGradientBoostingClassifier(random_state=args.seed)),
        ]
    )

    lr.fit(X_train, y_train)
    hgb.fit(X_train, y_train)

    metrics: dict = {}
    metrics.update(evaluate(lr, X_val, y_val, "lr_val"))
    metrics.update(evaluate(lr, X_test, y_test, "lr_test"))
    metrics.update(evaluate(hgb, X_val, y_val, "hgb_val"))
    metrics.update(evaluate(hgb, X_test, y_test, "hgb_test"))

    best_name = min(
        [("lr", metrics["lr_val_logloss"]), ("hgb", metrics["hgb_val_logloss"])],
        key=lambda t: t[1],
    )[0]
    best = lr if best_name == "lr" else hgb

    model_path = artifacts_dir / "model.joblib"
    joblib.dump(
        {
            "model_name": best_name,
            "feature_cols": feature_cols,
            "model": best,
            "dataset_view": view_name,
        },
        model_path,
    )

    (artifacts_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    test_proba = best.predict_proba(X_test)[:, 1]
    out = test_df[["game_id", "game_date", "team_id", "opponent_team_id", "is_home", target]].copy()
    out["pred_win_proba"] = test_proba
    out.to_csv(artifacts_dir / "test_predictions.csv", index=False)

    print("Done.")
    print("Dataset view:", view_name)
    print("Best model:", best_name)
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()