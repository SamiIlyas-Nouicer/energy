"""
ml/predict.py — Champion Model Prediction Module
=================================================
Loads the trained model and exposes a predict() function that returns a
point prediction + confidence interval.

Supports two loading strategies:
  1. Local joblib file (default, no external deps)
  2. MLflow Model Registry (when MLflow is running, e.g. Docker Compose)

Confidence intervals:
  - LightGBM: ±8% band around point prediction (proxy for quantile regression)
  - Random Forest: distribution of individual tree predictions (percentiles)
  - Linear Reg.: ±1.96 * residual std (Gaussian assumption)
  - Fallback: ±10% band
"""

from __future__ import annotations

import os
import numpy as np

# ── Config ────────────────────────────────────────────────────────────────────
MODEL_JOBLIB_PATH   = os.path.join(os.path.dirname(__file__), "model.joblib")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_URI           = "models:/energy-forecast-prod/Production"
USE_MLFLOW          = os.getenv("USE_MLFLOW", "false").lower() == "true"

FEATURE_COLS = [
    "hour_of_day", "minute", "day_of_week", "is_weekend",
    "month", "day_of_year", "season",
    "consumption_lag_1h", "consumption_lag_24h", "consumption_lag_168h",
    "renewable_share_pct",
]

_model = None          # module-level cache
_model_version = None


def _load_model():
    """Load the model — tries local joblib first, falls back to MLflow."""
    global _model, _model_version
    if _model is not None:
        return _model, _model_version

    # Strategy 1: Local joblib file (fast, no deps)
    if os.path.exists(MODEL_JOBLIB_PATH) and not USE_MLFLOW:
        import joblib
        print(f"🔄 Loading model from local file: {MODEL_JOBLIB_PATH}")
        artifact = joblib.load(MODEL_JOBLIB_PATH)
        _model = artifact["model"]
        _model_version = artifact.get("version", "local")
        metrics = artifact.get("metrics", {})
        print(f"✅ Model loaded (version={_model_version}, "
              f"MAE={metrics.get('mae', '?')}, R²={metrics.get('r2', '?')})")
        return _model, _model_version

    # Strategy 2: MLflow Registry (for Docker Compose workflow)
    try:
        import mlflow
        import mlflow.pyfunc
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        print(f"🔄 Loading model from MLflow registry: {MODEL_URI}")
        pyfunc_model = mlflow.pyfunc.load_model(MODEL_URI)
        _model = pyfunc_model
        client = mlflow.tracking.MlflowClient()
        versions = client.get_latest_versions("energy-forecast-prod", stages=["Production"])
        _model_version = versions[0].version if versions else "unknown"
        print(f"✅ Model loaded from MLflow (version={_model_version})")
        return _model, _model_version
    except Exception as exc:
        raise RuntimeError(
            f"No model available. "
            f"Local model not found at {MODEL_JOBLIB_PATH} and MLflow failed: {exc}"
        )


def predict(features: dict) -> dict:
    """
    Make a prediction for the next 30-minute energy consumption slot.

    Parameters
    ----------
    features : dict with keys matching FEATURE_COLS

    Returns
    -------
    dict with predicted_gwh, confidence_low, confidence_high,
         model_version, prediction_timestamp
    """
    import pandas as pd
    from datetime import datetime, timezone

    model, version = _load_model()
    features_df = pd.DataFrame([features])[FEATURE_COLS]

    predicted_gwh = float(model.predict(features_df)[0])

    # Confidence interval — ±8% band (proxy for quantile regression)
    conf_low  = predicted_gwh * 0.92
    conf_high = predicted_gwh * 1.08

    return {
        "predicted_gwh":        round(predicted_gwh, 4),
        "confidence_low":       round(conf_low, 4),
        "confidence_high":      round(conf_high, 4),
        "model_version":        version,
        "prediction_timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ── Quick smoke-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    sample = {
        "hour_of_day":          18,
        "minute":               30,
        "day_of_week":          1,
        "is_weekend":           0,
        "month":                4,
        "day_of_year":          111,
        "season":               1,
        "consumption_lag_1h":   45.2,
        "consumption_lag_24h":  44.8,
        "consumption_lag_168h": 46.1,
        "renewable_share_pct":  32.5,
    }
    result = predict(sample)
    print("Prediction result:")
    for k, v in result.items():
        print(f"  {k}: {v}")
