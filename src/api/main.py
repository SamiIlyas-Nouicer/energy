"""
api/main.py — FastAPI Prediction Service
=========================================
Exposes three endpoints:
  GET  /           → API description
  GET  /health     → readiness check (model loaded?)
  POST /predict    → energy consumption forecast with confidence interval

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
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

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
