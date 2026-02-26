# NBA Win Predictor (SQL + ML)

A pet project that demonstrates:
- **SQL** feature engineering (CTEs, window functions, joins)
- **Time-series safe ML** (no leakage; time-based split)
- A small **Streamlit** demo

## Dataset
Download the dataset from Kaggle and place these files into `data/`:
- `NBA_TEAMS.csv`
- `NBA_GAMES.csv`
- `NBA_PLAYER_GAMES.csv`
- `NBA_PLAYERS.csv`

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

- Train to create `nba.duckdb`.
```
python src/train.py --data-dir data --db-path nba.duckdb --artifacts-dir artifacts
```
- Start up framework based on results
```
streamlit run app/streamlit_app.py
```
### Streamlit visualisation sample

<img width="1919" height="979" alt="image" src="https://github.com/user-attachments/assets/cc2094e2-8791-40bb-931f-9e55c2902e90" />

After training, you get:
- `artifacts/model.joblib` (best of LogisticRegression / HistGradientBoosting)
- `artifacts/metrics.json`
- `artifacts/test_predictions.csv`

## Notebooks

- `notebooks/01_eda.ipynb` — dataset exploration + SQL view validation + baseline
- `notebooks/02_modeling.ipynb` — time split training + evaluation + LR interpretability

## SQL pipeline
The SQL lives in `sql/` and is executed by `src/train.py` in this order:
1. `01_create_schema.sql`  → creates `v_games`, `v_teams`
2. `02_team_form_features.sql` → creates `v_team_form` with rolling last-5 features
3. `03_training_dataset.sql` → creates `v_training_dataset` (joins team + opponent features)

Example query (standings):

```sql
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
```

## Notes on leakage
Rolling features use:
- `ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING`

That means the current game stats never appear in its own features.

## `src/predict.py` — batch inference (scoring)

`predict.py` is an optional script that loads the trained model from `artifacts/model.joblib` and runs **batch predictions** over the full feature dataset in DuckDB.

Use it when you want to:
- generate a full table of win probabilities for analysis/BI,
- compare model outputs across seasons/teams,
- create an export that Streamlit or other tools can consume (without retraining).

Example:

```bash
python src/predict.py --db-path nba.duckdb --model-path artifacts/model.joblib --out artifacts/predictions_full.csv
```

Output:

`artifacts/predictions_full.csv` with columns like `game_id, game_date, team_id, opponent_team_id, y_win, pred_win_proba`.
