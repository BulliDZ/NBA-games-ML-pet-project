# NOTES — NBA Win Predictor (SQL + ML + Streamlit)

## 1) What this project is

This project is a **mini data product** built on NBA historical game logs.

It combines:

- **SQL analytics + feature engineering** (DuckDB views)
- **Machine learning** (predict win probability)
- **A small UI** (Streamlit) to explore matchups and compare teams in plain language

The result is not just “a model”, but a repeatable pipeline:

**raw CSV → standardized tables → SQL feature mart → ML training → saved artifacts → interactive app**

---

## 2) Why ML is important here

SQL is great at answering **what happened** and computing indicators (averages, trends, joins).
But the core question we want to answer is:

> “Given what we knew *before a game*, what is the probability Team A will win vs Team B?”

ML solves the part that is difficult to do reliably with rules:

- combines many signals (recent scoring, turnovers, shooting %, rest, opponent strength, etc.)
- learns the **relative importance** of each signal (weights)
- captures **non‑linear effects & interactions** (especially with the tree model)
- outputs **probabilities**, not only a hard yes/no
- generalizes across many seasons/matchups (instead of hand-coded heuristics)

In other words:
- SQL creates the “inputs”
- ML learns the mapping **inputs → win probability**


## 3) Problem definition

### Target / label (supervised learning)
Binary classification:

- `y_win = 1` if the selected team won the game
- `y_win = 0` otherwise

### Prediction task
Given a game and the two teams involved, produce:

- `P(win)` for the selected team

### Granularity
**Team-game grain** (one row per team per game), not a single row per matchup.

This makes it easy to:
- compute rolling team form
- compute opponent features by joining the opponent’s row in the same `game_id`

---

## 4) Data sources & inputs

Required CSVs:
- `NBA_TEAMS.csv`
- `NBA_GAMES.csv`

Optional (adds player-informed features):
- `NBA_PLAYERS.csv`
- `NBA_PLAYER_GAMES.csv`

### Notes on the raw formats
Some Kaggle exports do not provide a `season` column in `NBA_GAMES`.
In that case we derive **season start year** from the game date (NBA season typically starts in Oct):
- if month >= 10 → season = year
- else → season = year - 1

Also, opponent and home/away can be derived from the `MATCHUP` text, for example:
- `MEM vs. OKC` → home game, opponent OKC
- `ORL @ BOS` → away game, opponent BOS

Team abbreviations are mapped to team IDs using `NBA_TEAMS.abbreviation`.

---

## 5) Data model (standardized tables)

After standardization the project operates on these tables inside DuckDB:

### `teams`
- `team_id`
- `full_name`
- `abbreviation`
- `city`, `state`, `year_founded`, ...

### `games` (team-game)
- `game_id`
- `season`
- `game_date`
- `team_id`
- `opponent_team_id`
- `is_home`
- `wl` (W/L)
- stats such as `pts`, `reb`, `ast`, `tov`, `fg_pct`, `fg3_pct`, `ft_pct`

### `players` (optional)
- `player_id`
- `full_name`, `first_name`, `last_name`, `is_active`

### `player_games` (optional; player-game)
- `player_id`, `game_id`, `season`, `game_date`
- `team_id`, `opponent_team_id`, `is_home`, `wl`
- stats like `minutes`, `pts`, `plus_minus`, etc.

---

## 6) SQL layer (feature mart)

The SQL is the backbone: it creates reproducible and inspectable features.

### Core views
- `v_games`, `v_teams` (clean “presentation” views over base tables)
- `v_team_form` — team rolling form features
- `v_training_dataset` — team features + opponent features in one row

Optional:
- `v_player_form` — rolling player form features
- `v_team_star_features` — aggregates “top players in form” features
- `v_training_dataset_enriched` — base dataset + player-informed features

### Leakage prevention (important)
Rolling features are computed excluding the current game:

`ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING`

This ensures we only use **past games** to predict the current game.

### Examples of engineered features (last 5 games)
Team-side:
- `pts_avg_l5`, `reb_avg_l5`, `ast_avg_l5`, `tov_avg_l5`
- `fg_pct_avg_l5`, `fg3_pct_avg_l5`, `ft_pct_avg_l5`
- `winrate_l5`
- `rest_days` (days since previous game)

Opponent-side (same metrics with `opp_` prefix):
- `opp_pts_avg_l5`, `opp_winrate_l5`, ...

Optional player-informed features:
- `star_pts_avg_l5_sum`: sum of last-5 scoring averages for top 3 in-form players
- `star_min_avg_l5_avg`, `star_pm_avg_l5_avg`
(and opponent equivalents)

### Named views (for UI readability)
To make the UI friendly (non-fans), we create named views:
- `v_training_dataset_named`
- `v_training_dataset_enriched_named`

They include:
- `team_name`, `team_abbr`
- `opponent_name`, `opponent_abbr`
while still keeping `team_id` and `opponent_team_id` at the end for traceability.

---

## 7) ML pipeline (the “predictive” part)

### 7.1 Dataset selection
If player tables exist:
- train on `v_training_dataset_enriched`

Else:
- train on `v_training_dataset`

### 7.2 Feature matrix and target
- `X`: all engineered numeric columns excluding identifiers
- `y`: `y_win`

Excluded from features to avoid leakage or identity shortcuts:
- `game_id`, `game_date`, `team_id`, `opponent_team_id`, `season`, `y_win`

### 7.3 Time-based split (realistic evaluation)
Instead of random splitting, we split by time:

- Train = oldest games
- Validation = more recent games
- Test = most recent games

This mimics real usage: predict future games from past data.
It also prevents accidental look-ahead bias.

### 7.4 Models used
We train and compare two models:

1) **Logistic Regression**
- interpretable baseline
- linear decision boundary
- produces probabilities
- good for explaining feature influence (sign/weight)

2) **HistGradientBoostingClassifier**
- strong tabular baseline
- captures non-linear relationships and feature interactions
- often outperforms linear models on structured data

### 7.5 Handling missing values
Rolling features can be missing early in the season or early in the team’s history.
We use a Pipeline with:

- `SimpleImputer(strategy="median")`

This is a robust default for numeric tabular features.

### 7.6 Optimization / selection
We select the best model using **validation LogLoss**.

Why LogLoss:
- we care about **probability quality**, not just hard classification accuracy
- LogLoss rewards correct confidence and penalizes wrong confident predictions

### 7.7 Metrics tracked
- **LogLoss** (primary)
- **ROC-AUC** (ranking quality)
- **Accuracy** (secondary, less informative for probability tasks)

### 7.8 Output artifacts
The training script writes:

- `artifacts/model.joblib`
  - best model
  - `feature_cols`
  - which dataset view was used
- `artifacts/metrics.json`
- `artifacts/test_predictions.csv`
  - predicted win probability for held-out test set

These artifacts power Streamlit without retraining each run.

---

## 8) Streamlit app (what the user experiences)

### Primary UI goal
Make the model useful even for non-basketball fans.

### Features
- Select **Team A** and **Team B**
- Choose a real head-to-head game from the dataset
- Show:
  - matchup title with team names/abbreviations
  - actual outcome (Team A win/loss)
  - optional model probabilities (if `model.joblib` exists)
  - a comparison table of key indicators (Team A vs Team B) with a plain-language glossary

### Why this matters
A lot of ML projects stop at notebooks.
This UI step demonstrates “product thinking”:
- model outputs become understandable and explorable
- metrics become interpretable insights

---

## 9) How to run

### Install
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Place data
Put into `data/`:
- `NBA_TEAMS.csv`
- `NBA_GAMES.csv`
Optionally:
- `NBA_PLAYERS.csv`
- `NBA_PLAYER_GAMES.csv`

### Train
```bash
python src/train.py --data-dir data --db-path nba.duckdb --artifacts-dir artifacts
```

### Run the app
```bash
streamlit run app/streamlit_app.py
```

---

## 10) Design choices & trade-offs

### Why DuckDB
- embedded (no server)
- excellent for analytics queries & window functions
- integrates naturally with pandas and ML pipelines

### Why team-game grain
- simplifies rolling features and opponent joins
- works well for both analytics and ML

### Why last-5 games
- easy to explain (“recent form”)
- reduces noise vs single-game stats
- can be extended to last-10 or exponentially weighted averages


## Glossary (for non-basketball fans)

- **Points (PTS):** score made by a team (more is better)
- **Assists (AST):** passes that directly lead to a score
- **Rebounds (REB):** regaining the ball after a missed shot
- **Turnovers (TOV):** losing possession without shooting (lower is better)
- **FG%:** overall shooting accuracy
- **3P%:** three-point shooting accuracy
- **FT%:** free-throw accuracy
- **Rest days:** days since last game
- **Win rate (last 5):** recent form from 0 to 1
