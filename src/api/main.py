"""
api/main.py — FastAPI Prediction & Data Service
=================================================
Exposes prediction endpoints + data endpoints for the dashboard:
  GET  /           → API description
  GET  /health     → readiness check (model loaded?)
  POST /predict    → energy consumption forecast with confidence interval
  GET  /api/energy-mix       → last 48h generation data
  GET  /api/co2-latest       → latest CO₂ intensity
  GET  /api/regional         → regional weekly data
  GET  /api/regional/weeks   → available week list
  GET  /api/forecast/actual  → actual consumption for date range
  GET  /api/pipeline-health  → gold layer quality stats

Run with:
  uvicorn src.api.main:app --reload --port 8000

Swagger docs available at:
  http://localhost:8000/docs
"""

from __future__ import annotations

import sys
import os

# Ensure the project root is on sys.path so `src.ml.predict` resolves
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from datetime import datetime, timezone
from typing import Any, Optional

import duckdb
import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# ── Config ────────────────────────────────────────────────────────────────────
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "src/data/gold.duckdb")

# Physical bounds for anomaly detection
BOUNDS = {
    "consumption_mwh": (10_000, 120_000),
    "renewable_share_pct": (0, 100),
    "co2_intensity_gco2_per_kwh": (0, 200),
}

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="French Energy Intelligence — Prediction API",
    description=(
        "Production ML service for 30-minute-ahead electricity consumption "
        "forecasting. Powered by a champion model tracked and registered in MLflow."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS — allow the Next.js frontend to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Lazy model state ──────────────────────────────────────────────────────────
_model_loaded: bool = False
_model_load_error: str | None = None


def _ensure_model():
    """Load the model on first request, cache the result."""
    global _model_loaded, _model_load_error
    if not _model_loaded:
        try:
            from src.ml.predict import _load_model
            _load_model()
            _model_loaded = True
            _model_load_error = None
        except Exception as exc:
            _model_load_error = str(exc)
            _model_loaded = False


# ── Schemas ───────────────────────────────────────────────────────────────────
class PredictionRequest(BaseModel):
    """All features needed to predict next-30-min electricity consumption."""

    hour_of_day:          int   = Field(..., ge=0,  le=23,   description="Hour of day (0–23)")
    minute:               int   = Field(..., ge=0,  le=30,   description="Minute (0 or 30)")
    day_of_week:          int   = Field(..., ge=0,  le=6,    description="Day of week (0=Monday)")
    is_weekend:           int   = Field(..., ge=0,  le=1,    description="1 if Saturday/Sunday")
    month:                int   = Field(..., ge=1,  le=12,   description="Month (1–12)")
    day_of_year:          int   = Field(..., ge=1,  le=366,  description="Day of year (1–366)")
    season:               int   = Field(..., ge=0,  le=3,    description="0=winter 1=spring 2=summer 3=autumn")
    consumption_lag_1h:   float = Field(..., description="Consumption 1 hour ago (GWh per 30-min slot)")
    consumption_lag_24h:  float = Field(..., description="Consumption 24 hours ago (GWh per 30-min slot)")
    consumption_lag_168h: float = Field(..., description="Consumption 1 week ago (GWh per 30-min slot)")
    renewable_share_pct:  float = Field(..., ge=0, le=100, description="Current renewable share (%)")

    model_config = {
        "json_schema_extra": {
            "example": {
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
        }
    }


class PredictionResponse(BaseModel):
    predicted_gwh:        float
    confidence_low:       float
    confidence_high:      float
    model_version:        str
    prediction_timestamp: str


class HealthResponse(BaseModel):
    status:       str
    model_loaded: bool
    model_uri:    str
    timestamp:    str


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/", tags=["Info"])
async def root() -> dict[str, Any]:
    """
    Returns a brief description of the API and available endpoints.
    """
    return {
        "name":        "French Energy Intelligence — Prediction API",
        "version":     "1.0.0",
        "description": "30-minute-ahead electricity consumption forecasting for the French grid.",
        "model":       "MLflow Model Registry — energy-forecast-prod/Production",
        "endpoints": {
            "GET  /":        "This description",
            "GET  /health":  "Readiness check",
            "POST /predict": "Make a consumption forecast",
            "GET  /docs":    "Swagger interactive documentation",
        },
    }


@app.get("/health", response_model=HealthResponse, tags=["Ops"])
async def health() -> HealthResponse:
    """
    Returns the service health status and whether the ML model is loaded.
    A 503 is returned if the model failed to load.
    """
    _ensure_model()
    if not _model_loaded:
        raise HTTPException(
            status_code=503,
            detail=f"Model not loaded: {_model_load_error}",
        )
    return HealthResponse(
        status="ok",
        model_loaded=True,
        model_uri="models:/energy-forecast-prod/Production",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@app.post("/predict", response_model=PredictionResponse, tags=["Prediction"])
async def predict_endpoint(request: PredictionRequest) -> PredictionResponse:
    """
    Predict next-30-minute electricity consumption.

    Accepts all feature values as a JSON body and returns:
    - **predicted_gwh**: point prediction
    - **confidence_low / confidence_high**: 10th–90th percentile interval
    - **model_version**: MLflow model version used
    - **prediction_timestamp**: UTC ISO-8601 timestamp
    """
    _ensure_model()
    if not _model_loaded:
        raise HTTPException(
            status_code=503,
            detail=f"Model not available: {_model_load_error}",
        )
    try:
        from src.ml.predict import predict
        result = predict(request.model_dump())
        return PredictionResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Exception handler ─────────────────────────────────────────────────────────
@app.exception_handler(422)
async def validation_exception_handler(request: Request, exc):
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "hint": "Check /docs for the expected request schema."},
    )


# ══════════════════════════════════════════════════════════════════════════════
# Dashboard Data Endpoints
# ══════════════════════════════════════════════════════════════════════════════

def _get_con():
    """Get a read-only DuckDB connection."""
    return duckdb.connect(DUCKDB_PATH, read_only=True)


@app.get("/api/energy-mix", tags=["Dashboard Data"])
async def get_energy_mix():
    """Return last 96 rows (48h at 30-min intervals) from hourly_energy_mix."""
    try:
        con = _get_con()
        df = con.execute("""
            SELECT date, nuclear_mwh, solar_mwh, wind_mwh, hydro_mwh,
                   bio_mwh, gas_mwh, coal_mwh, oil_mwh,
                   total_production_mwh, renewable_share_pct, consumption_mwh
            FROM hourly_energy_mix
            ORDER BY date DESC
            LIMIT 96
        """).fetchdf()
        con.close()
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
        return {"data": df.sort_values("date").to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/co2-latest", tags=["Dashboard Data"])
async def get_co2_latest():
    """Return the most recent CO₂ intensity value."""
    try:
        con = _get_con()
        row = con.execute("""
            SELECT co2_intensity_gco2_per_kwh
            FROM co2_intensity
            ORDER BY timestamp DESC
            LIMIT 1
        """).fetchone()
        con.close()
        return {"co2_intensity": round(row[0], 1) if row else None}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/regional/weeks", tags=["Dashboard Data"])
async def get_regional_weeks():
    """Return available week_start dates for the regional data."""
    try:
        con = _get_con()
        df = con.execute("""
            SELECT DISTINCT week_start
            FROM regional_weekly
            ORDER BY week_start DESC
        """).fetchdf()
        con.close()
        weeks = pd.to_datetime(df["week_start"]).dt.strftime("%Y-%m-%d").tolist()
        return {"weeks": weeks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/regional", tags=["Dashboard Data"])
async def get_regional(week: Optional[str] = Query(None, description="Week start date YYYY-MM-DD")):
    """Return regional weekly data, optionally filtered by week."""
    try:
        con = _get_con()
        if week:
            df = con.execute(f"""
                SELECT week_start, region, population,
                       regional_consumption_gwh,
                       consumption_kwh_per_capita,
                       avg_renewable_share_pct
                FROM regional_weekly
                WHERE CAST(week_start AS DATE) = '{week}'
                ORDER BY consumption_kwh_per_capita DESC
            """).fetchdf()
        else:
            # Latest week
            df = con.execute("""
                SELECT week_start, region, population,
                       regional_consumption_gwh,
                       consumption_kwh_per_capita,
                       avg_renewable_share_pct
                FROM regional_weekly
                WHERE week_start = (SELECT MAX(week_start) FROM regional_weekly)
                ORDER BY consumption_kwh_per_capita DESC
            """).fetchdf()
        con.close()
        df["week_start"] = pd.to_datetime(df["week_start"]).dt.strftime("%Y-%m-%d")
        return {"data": df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/forecast/actual", tags=["Dashboard Data"])
async def get_forecast_actual(
    start: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
):
    """Return actual consumption data for a date range."""
    try:
        con = _get_con()
        if start and end:
            df = con.execute(f"""
                SELECT date, total_consumption_gwh, avg_renewable_share_pct
                FROM daily_consumption_summary
                WHERE date >= '{start}' AND date < '{end}'
                ORDER BY date
            """).fetchdf()
        else:
            df = con.execute("""
                SELECT date, total_consumption_gwh, avg_renewable_share_pct
                FROM daily_consumption_summary
                ORDER BY date DESC
                LIMIT 14
            """).fetchdf()
            df = df.sort_values("date")
        con.close()
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        return {
            "data": df.to_dict(orient="records"),
            "max_date": df["date"].max() if not df.empty else None,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pipeline-health", tags=["Dashboard Data"])
async def get_pipeline_health():
    """Return data quality stats for all gold layer tables."""
    try:
        con = _get_con()
        stats = {}

        # --- hourly_energy_mix ---
        df_mix = con.execute("""
            SELECT date, consumption_mwh, renewable_share_pct,
                   nuclear_mwh, solar_mwh, wind_mwh, hydro_mwh
            FROM hourly_energy_mix
            ORDER BY date DESC
        """).fetchdf()

        latest_mix = pd.to_datetime(df_mix["date"].max())
        stats["energy_mix"] = {
            "table": "hourly_energy_mix",
            "rows": len(df_mix),
            "latest": latest_mix.isoformat() if pd.notna(latest_mix) else None,
            "null_consumption_pct": round(float(df_mix["consumption_mwh"].isna().mean() * 100), 2),
            "null_renewable_pct": round(float(df_mix["renewable_share_pct"].isna().mean() * 100), 2),
            "anomalies_consumption": int(
                ((df_mix["consumption_mwh"] < BOUNDS["consumption_mwh"][0]) |
                 (df_mix["consumption_mwh"] > BOUNDS["consumption_mwh"][1])).sum()
            ),
            "anomalies_renewable": int(
                ((df_mix["renewable_share_pct"] < 0) |
                 (df_mix["renewable_share_pct"] > 100)).sum()
            ),
        }

        # --- co2_intensity ---
        df_co2 = con.execute("""
            SELECT timestamp, co2_intensity_gco2_per_kwh
            FROM co2_intensity
            ORDER BY timestamp DESC
        """).fetchdf()

        latest_co2 = pd.to_datetime(df_co2["timestamp"].max())
        stats["co2"] = {
            "table": "co2_intensity",
            "rows": len(df_co2),
            "latest": latest_co2.isoformat() if pd.notna(latest_co2) else None,
            "null_co2_pct": round(float(df_co2["co2_intensity_gco2_per_kwh"].isna().mean() * 100), 2),
            "anomalies_co2": int(
                ((df_co2["co2_intensity_gco2_per_kwh"] < 0) |
                 (df_co2["co2_intensity_gco2_per_kwh"] > 200)).sum()
            ),
        }

        # --- daily_consumption_summary ---
        df_cons = con.execute("""
            SELECT date, total_consumption_gwh, avg_renewable_share_pct,
                   avg_temperature_celsius
            FROM daily_consumption_summary
            ORDER BY date DESC
        """).fetchdf()

        latest_cons = pd.to_datetime(df_cons["date"].max())
        stats["consumption"] = {
            "table": "daily_consumption_summary",
            "rows": len(df_cons),
            "latest": latest_cons.isoformat() if pd.notna(latest_cons) else None,
            "null_consumption_pct": round(float(df_cons["total_consumption_gwh"].isna().mean() * 100), 2),
            "null_temperature_pct": round(float(df_cons["avg_temperature_celsius"].isna().mean() * 100), 2),
            "anomalies": int(
                ((df_cons["total_consumption_gwh"] < 10) |
                 (df_cons["total_consumption_gwh"] > 120)).sum()
            ),
        }

        # --- regional_weekly ---
        df_reg = con.execute("SELECT * FROM regional_weekly").fetchdf()
        latest_reg = pd.to_datetime(df_reg["week_start"].max())
        stats["regional"] = {
            "table": "regional_weekly",
            "rows": len(df_reg),
            "latest": latest_reg.isoformat() if pd.notna(latest_reg) else None,
            "null_pct": round(float(df_reg.isna().mean().mean() * 100), 2),
            "regions": int(df_reg["region"].nunique()),
        }

        con.close()

        # Compute overall freshness
        all_latest = [
            pd.Timestamp(s["latest"]) for s in stats.values()
            if s.get("latest") is not None
        ]
        overall_latest = max(all_latest).isoformat() if all_latest else None

        return {
            "tables": stats,
            "overall_latest": overall_latest,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

