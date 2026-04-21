"""
Day 5 — Morning: Feature Engineering & Model Training
======================================================
Loads real data from the gold DuckDB layer, engineers temporal + lag features,
then trains three models (Linear Regression, Random Forest, LightGBM) with
full MLflow experiment tracking: hyperparameters, metrics, model artifacts,
and feature importance charts.
"""

import os
import warnings
import duckdb
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # non-interactive backend for saving PNGs
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
import mlflow.lightgbm
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from lightgbm import LGBMRegressor

warnings.filterwarnings("ignore")

# ── Config ──────────────────────────────────────────────────────────────────
DUCKDB_PATH = os.path.join(os.path.dirname(__file__), "../data/gold.duckdb")
MLFLOW_TRACKING_URI = "http://localhost:5000"
EXPERIMENT_NAME = "energy_consumption_forecast"


# ── 1. Load Data ─────────────────────────────────────────────────────────────
def load_gold_data() -> pd.DataFrame:
    """Load daily_consumption_summary from DuckDB, sorted chronologically."""
    print("📦 Loading data from DuckDB gold layer...")
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute(
        "SELECT * FROM daily_consumption_summary ORDER BY date"
    ).fetchdf()
    con.close()
    print(f"   ✅ {len(df):,} rows loaded  |  {df['date'].min()} → {df['date'].max()}")
    return df


# ── 2. Feature Engineering ───────────────────────────────────────────────────
def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build temporal + lag features from the 30-minute time series.
    Note: avg_temperature_celsius & heating_degree_days_sum are all-null
    (weather join was never run), so we exclude them and rely on calendar
    features + strong lag features instead.
    """
    print("⚙️  Engineering features...")
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Calendar features
    df["hour_of_day"]   = df["date"].dt.hour
    df["minute"]        = df["date"].dt.minute
    df["day_of_week"]   = df["date"].dt.dayofweek          # 0=Monday
    df["is_weekend"]    = (df["day_of_week"] >= 5).astype(int)
    df["month"]         = df["date"].dt.month
    df["day_of_year"]   = df["date"].dt.dayofyear

    # Season: 0=winter, 1=spring, 2=summer, 3=autumn
    season_map = {12: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1,
                  6: 2, 7: 2, 8: 2, 9: 3, 10: 3, 11: 3}
    df["season"] = df["month"].map(season_map)

    # Lag features (30-min intervals → 1h=2, 24h=48, 1 week=336 steps)
    df["consumption_lag_1h"]   = df["total_consumption_gwh"].shift(2)
    df["consumption_lag_24h"]  = df["total_consumption_gwh"].shift(48)
    df["consumption_lag_168h"] = df["total_consumption_gwh"].shift(336)

    # Renewable share (already % numeric)
    df["renewable_share_pct"] = df["avg_renewable_share_pct"]

    # Target: consumption at the NEXT 30-minute slot
    df["next_30min_consumption_gwh"] = df["total_consumption_gwh"].shift(-1)

    # Drop rows with NaN created by shifting
    df = df.dropna(subset=[
        "consumption_lag_1h", "consumption_lag_24h",
        "consumption_lag_168h", "next_30min_consumption_gwh"
    ]).reset_index(drop=True)

    print(f"   ✅ {len(df):,} clean rows after lag + target creation")
    return df


# ── 3. Train / Test Split ────────────────────────────────────────────────────
FEATURE_COLS = [
    "hour_of_day", "minute", "day_of_week", "is_weekend",
    "month", "day_of_year", "season",
    "consumption_lag_1h", "consumption_lag_24h", "consumption_lag_168h",
    "renewable_share_pct",
]
TARGET_COL = "next_30min_consumption_gwh"


def time_split(df: pd.DataFrame, test_frac: float = 0.20):
    """
    Chronological split — NEVER shuffle time series data.
    Last `test_frac` of rows are the test set (~5 months).
    """
    split_idx = int(len(df) * (1 - test_frac))
    train = df.iloc[:split_idx]
    test  = df.iloc[split_idx:]
    print(f"   📅 Train: {train['date'].min().date()} → {train['date'].max().date()}  "
          f"({len(train):,} rows)")
    print(f"   📅 Test:  {test['date'].min().date()} → {test['date'].max().date()}  "
          f"({len(test):,} rows)")
    return (
        train[FEATURE_COLS], train[TARGET_COL],
        test[FEATURE_COLS],  test[TARGET_COL],
    )


# ── 4. Metrics & Feature Importance Helpers ──────────────────────────────────
def compute_metrics(y_true, y_pred) -> dict:
    mae  = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2   = r2_score(y_true, y_pred)
    return {"mae": round(mae, 4), "rmse": round(rmse, 4), "r2": round(r2, 6)}


def save_feature_importance_chart(importances: np.ndarray,
                                  feature_names: list,
                                  title: str,
                                  path: str):
    """Save a horizontal bar chart of feature importances to `path`."""
    indices = np.argsort(importances)
    plt.figure(figsize=(9, 6))
    plt.barh(
        [feature_names[i] for i in indices],
        importances[indices],
        color="#4C72B0", edgecolor="white", height=0.7,
    )
    plt.xlabel("Importance", fontsize=12)
    plt.title(title, fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()


# ── 5. Training Runs ─────────────────────────────────────────────────────────
def run_linear_regression(X_train, y_train, X_test, y_test):
    print("\n🔵 Training Linear Regression...")
    params = {"model_type": "LinearRegression"}
    with mlflow.start_run(run_name="LinearRegression_baseline"):
        mlflow.log_params(params)
        model = LinearRegression()
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        metrics = compute_metrics(y_test, preds)
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(model, "model")

        # Coefficients as "importance"
        chart_path = "/tmp/feat_importance_lr.png"
        save_feature_importance_chart(
            np.abs(model.coef_), FEATURE_COLS,
            "Linear Regression — |Coefficient| Importance", chart_path
        )
        mlflow.log_artifact(chart_path, artifact_path="charts")

        print(f"   MAE={metrics['mae']:.4f} GWh | RMSE={metrics['rmse']:.4f} | R²={metrics['r2']:.4f}")
    return metrics


def run_random_forest(X_train, y_train, X_test, y_test):
    print("\n🟢 Training Random Forest...")
    params = {"n_estimators": 100, "max_depth": 12, "min_samples_leaf": 5,
              "model_type": "RandomForestRegressor"}
    with mlflow.start_run(run_name="RandomForest_v1"):
        mlflow.log_params(params)
        model = RandomForestRegressor(
            n_estimators=params["n_estimators"],
            max_depth=params["max_depth"],
            min_samples_leaf=params["min_samples_leaf"],
            n_jobs=-1,
            random_state=42,
        )
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        metrics = compute_metrics(y_test, preds)
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(model, "model")

        chart_path = "/tmp/feat_importance_rf.png"
        save_feature_importance_chart(
            model.feature_importances_, FEATURE_COLS,
            "Random Forest — Feature Importances", chart_path
        )
        mlflow.log_artifact(chart_path, artifact_path="charts")

        print(f"   MAE={metrics['mae']:.4f} GWh | RMSE={metrics['rmse']:.4f} | R²={metrics['r2']:.4f}")
    return metrics, model


def run_lightgbm(X_train, y_train, X_test, y_test):
    print("\n🟡 Training LightGBM...")
    params = {
        "n_estimators": 500, "learning_rate": 0.05, "num_leaves": 31,
        "min_child_samples": 20, "subsample": 0.8, "colsample_bytree": 0.8,
        "model_type": "LGBMRegressor",
    }
    with mlflow.start_run(run_name="LightGBM_v1"):
        mlflow.log_params(params)
        model = LGBMRegressor(
            n_estimators=params["n_estimators"],
            learning_rate=params["learning_rate"],
            num_leaves=params["num_leaves"],
            min_child_samples=params["min_child_samples"],
            subsample=params["subsample"],
            colsample_bytree=params["colsample_bytree"],
            random_state=42,
            verbose=-1,
        )
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            callbacks=[],
        )
        preds = model.predict(X_test)
        metrics = compute_metrics(y_test, preds)
        mlflow.log_metrics(metrics)
        mlflow.lightgbm.log_model(model, "model")

        chart_path = "/tmp/feat_importance_lgbm.png"
        save_feature_importance_chart(
            model.feature_importances_, FEATURE_COLS,
            "LightGBM — Feature Importances (Gain)", chart_path
        )
        mlflow.log_artifact(chart_path, artifact_path="charts")

        print(f"   MAE={metrics['mae']:.4f} GWh | RMSE={metrics['rmse']:.4f} | R²={metrics['r2']:.4f}")
    return metrics, model


# ── 6. Main ───────────────────────────────────────────────────────────────────
def main():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    df = load_gold_data()
    df = engineer_features(df)

    print("\n📊 Splitting into train / test sets (last 20% = test)...")
    X_train, y_train, X_test, y_test = time_split(df)

    lr_metrics              = run_linear_regression(X_train, y_train, X_test, y_test)
    rf_metrics, rf_model    = run_random_forest(X_train, y_train, X_test, y_test)
    lgbm_metrics, lgbm_model = run_lightgbm(X_train, y_train, X_test, y_test)

    # ── Comparison summary ───────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("📈 RESULTS SUMMARY (test set)")
    print("=" * 60)
    print(f"{'Model':<30} {'MAE (GWh)':>10} {'RMSE':>10} {'R²':>8}")
    print("-" * 60)
    for name, m in [
        ("Linear Regression",  lr_metrics),
        ("Random Forest",       rf_metrics),
        ("LightGBM",            lgbm_metrics),
    ]:
        print(f"{name:<30} {m['mae']:>10.4f} {m['rmse']:>10.4f} {m['r2']:>8.4f}")
    print("=" * 60)

    # Identify champion
    models = {
        "LinearRegression_baseline": lr_metrics,
        "RandomForest_v1":           rf_metrics,
        "LightGBM_v1":               lgbm_metrics,
    }
    champion = min(models, key=lambda k: models[k]["mae"])
    print(f"\n🏆 Champion: {champion}  (MAE = {models[champion]['mae']:.4f} GWh)")
    print("\n✅ All done! Open http://localhost:5000 to compare runs in MLflow UI.")
    print("   → Register the champion model under 'energy-forecast-prod'")


if __name__ == "__main__":
    main()