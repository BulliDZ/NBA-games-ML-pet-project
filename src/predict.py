from __future__ import annotations

import argparse
from pathlib import Path
import joblib
import duckdb
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default="nba.duckdb")
    ap.add_argument("--model-path", default="artifacts/model.joblib")
    ap.add_argument("--out", default="artifacts/predictions_full.csv")
    args = ap.parse_args()

    bundle = joblib.load(args.model_path)
    model = bundle["model"]
    feature_cols = bundle["feature_cols"]

    con = duckdb.connect(args.db_path)
    df = con.execute("SELECT * FROM v_training_dataset").df()
    df["game_date"] = pd.to_datetime(df["game_date"])

    X = df[feature_cols]
    proba = model.predict_proba(X)[:, 1]

    out = df[["game_id", "game_date", "team_id", "opponent_team_id", "is_home", "y_win"]].copy()
    out["pred_win_proba"] = proba
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
