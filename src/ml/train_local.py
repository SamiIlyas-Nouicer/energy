"""
ml/train_local.py — Train & save a LightGBM model locally (no MLflow)
======================================================================
Reads from the DuckDB gold layer, engineers features, trains a LightGBM
model, and saves it as a joblib file for the FastAPI prediction endpoint.

Usage:
    python -m src.ml.train_local
"""

import os
import warnings
import duckdb
import numpy as np
import pandas as pd
import joblib
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from lightgbm import LGBMRegressor

warnings.filterwarnings("ignore")

# ── Config ──────────────────────────────────────────────────────────────────
DUCKDB_PATH = os.path.join(os.path.dirname(__file__), "../data/gold.duckdb")
MODEL_PATH  = os.path.join(os.path.dirname(__file__), "model.joblib")

FEATURE_COLS = [
    "hour_of_day", "minute", "day_of_week", "is_weekend",
    "month", "day_of_year", "season",
    "consumption_lag_1h", "consumption_lag_24h", "consumption_lag_168h",
    "renewable_share_pct",
]
TARGET_COL = "next_30min_consumption_gwh"


def load_and_engineer():
    """Load gold data and build features."""
    print("📦 Loading data from DuckDB gold layer...")
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute("SELECT * FROM daily_consumption_summary ORDER BY date").fetchdf()
    con.close()
    print(f"   ✅ {len(df):,} rows loaded  |  {df['date'].min()} → {df['date'].max()}")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Calendar features
    df["hour_of_day"]   = df["date"].dt.hour
    df["minute"]        = df["date"].dt.minute
    df["day_of_week"]   = df["date"].dt.dayofweek
    df["is_weekend"]    = (df["day_of_week"] >= 5).astype(int)
    df["month"]         = df["date"].dt.month
    df["day_of_year"]   = df["date"].dt.dayofyear

    season_map = {12: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1,
                  6: 2, 7: 2, 8: 2, 9: 3, 10: 3, 11: 3}
    df["season"] = df["month"].map(season_map)

    # Lag features (30-min intervals → 1h=2, 24h=48, 1 week=336 steps)
    df["consumption_lag_1h"]   = df["total_consumption_gwh"].shift(2)
    df["consumption_lag_24h"]  = df["total_consumption_gwh"].shift(48)
    df["consumption_lag_168h"] = df["total_consumption_gwh"].shift(336)

    df["renewable_share_pct"] = df["avg_renewable_share_pct"]

    # Target: consumption at the NEXT 30-minute slot
    df["next_30min_consumption_gwh"] = df["total_consumption_gwh"].shift(-1)

    df = df.dropna(subset=[
        "consumption_lag_1h", "consumption_lag_24h",
        "consumption_lag_168h", "next_30min_consumption_gwh"
    ]).reset_index(drop=True)

    print(f"   ✅ {len(df):,} clean rows after feature engineering")
    return df


def train_and_save():
    """Train LightGBM and save to disk."""
    df = load_and_engineer()

    # Chronological split — last 20% for test
    split_idx = int(len(df) * 0.80)
    train = df.iloc[:split_idx]
    test  = df.iloc[split_idx:]

    X_train, y_train = train[FEATURE_COLS], train[TARGET_COL]
    X_test,  y_test  = test[FEATURE_COLS],  test[TARGET_COL]

    print(f"\n📅 Train: {train['date'].min().date()} → {train['date'].max().date()} ({len(train):,} rows)")
    print(f"📅 Test:  {test['date'].min().date()} → {test['date'].max().date()} ({len(test):,} rows)")

    print("\n🟡 Training LightGBM...")
    model = LGBMRegressor(
        n_estimators=500,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        verbose=-1,
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)])

    preds = model.predict(X_test)
    mae  = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    r2   = r2_score(y_test, preds)

    print(f"\n📈 Test Metrics:")
    print(f"   MAE  = {mae:.4f} GWh")
    print(f"   RMSE = {rmse:.4f} GWh")
    print(f"   R²   = {r2:.6f}")

    # Save model + metadata
    artifact = {
        "model": model,
        "feature_cols": FEATURE_COLS,
        "metrics": {"mae": round(mae, 4), "rmse": round(rmse, 4), "r2": round(r2, 6)},
        "version": "lgbm-local-v1",
    }
    joblib.dump(artifact, MODEL_PATH)
    size_mb = os.path.getsize(MODEL_PATH) / (1024 * 1024)
    print(f"\n✅ Model saved to {MODEL_PATH} ({size_mb:.1f} MB)")
    print(f"🏆 Champion: LightGBM (MAE = {mae:.4f} GWh, R² = {r2:.4f})")


if __name__ == "__main__":
    train_and_save()
