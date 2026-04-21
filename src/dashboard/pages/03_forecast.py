"""
Page 3 — Consumption Forecast
================================
• Reads last 7 days of actual consumption from DuckDB
• Builds feature vectors for the next 24h (48 × 30-min slots)
• Calls the FastAPI /predict endpoint for each slot
• Plots: actual (blue solid) + predicted (orange dashed) + confidence band (shaded)
• Includes a date picker to replay any past week (forecast vs actual comparison)
"""

from __future__ import annotations

import sys
import os
from datetime import datetime, timedelta, timezone

import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

DUCKDB_PATH = "src/data/gold.duckdb"
API_URL     = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="Consumption Forecast", page_icon="📈", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
[data-testid="stAppViewContainer"] { background: #0f172a; color: #e2e8f0; }
h1, h2, h3 { color: #f1f5f9 !important; }
[data-testid="metric-container"] {
    background: #1e293b; border: 1px solid #334155;
    border-radius: 12px; padding: 16px;
}
[data-testid="stMetricValue"] { color: #38bdf8 !important; font-weight: 700; }
</style>
""", unsafe_allow_html=True)


# ── Data Helpers ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_actual(start_date: str, end_date: str) -> pd.DataFrame:
    """Load actual consumption for a date range from DuckDB."""
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute(f"""
        SELECT date,
               total_consumption_gwh,
               avg_renewable_share_pct
        FROM daily_consumption_summary
        WHERE date >= '{start_date}'
          AND date <  '{end_date}'
        ORDER BY date
    """).fetchdf()
    con.close()
    df["date"] = pd.to_datetime(df["date"])
    return df


def build_feature_vector(dt: datetime, lag_1h: float, lag_24h: float,
                          lag_168h: float, renewable_pct: float) -> dict:
    """Build a feature dict for a given datetime slot."""
    season_map = {12:0,1:0,2:0,3:1,4:1,5:1,6:2,7:2,8:2,9:3,10:3,11:3}
    return {
        "hour_of_day":          dt.hour,
        "minute":               dt.minute,
        "day_of_week":          dt.weekday(),
        "is_weekend":           int(dt.weekday() >= 5),
        "month":                dt.month,
        "day_of_year":          dt.timetuple().tm_yday,
        "season":               season_map[dt.month],
        "consumption_lag_1h":   lag_1h,
        "consumption_lag_24h":  lag_24h,
        "consumption_lag_168h": lag_168h,
        "renewable_share_pct":  renewable_pct,
    }


def check_api() -> bool:
    """Check if the prediction API is reachable."""
    try:
        r = requests.get(f"{API_URL}/health", timeout=3)
        return r.status_code == 200
    except Exception:
        return False


def call_predict(features: dict) -> dict | None:
    """Call the FastAPI /predict endpoint."""
    try:
        r = requests.post(f"{API_URL}/predict", json=features, timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


# ── Page ──────────────────────────────────────────────────────────────────────
st.markdown("# 📈 Consumption Forecast")
st.markdown("*24-hour ahead prediction with LightGBM — actual vs forecast comparison*")

# ── Controls ──────────────────────────────────────────────────────────────────
col_left, col_right = st.columns([3, 1])
with col_left:
    mode = st.radio(
        "Mode",
        ["🔴 Live — next 24h forecast", "📅 Historical replay"],
        horizontal=True,
    )
with col_right:
    api_ok = check_api()
    if api_ok:
        st.success("✅ API online")
    else:
        st.error("❌ API offline — start uvicorn on :8000")

# Date range
if "Historical" in mode:
    replay_date = st.date_input(
        "Select week start for replay",
        value=datetime(2025, 10, 1).date(),
        min_value=datetime(2023, 1, 8).date(),
        max_value=datetime(2025, 12, 1).date(),
    )
    start_dt = datetime.combine(replay_date, datetime.min.time())
else:
    # Live: last 7 days
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    max_date_row = con.execute("SELECT MAX(date) FROM daily_consumption_summary").fetchone()
    con.close()
    max_date = pd.to_datetime(max_date_row[0]) if max_date_row else datetime.now()
    start_dt = max_date - timedelta(days=7)

end_actual_dt = start_dt + timedelta(days=7)

# Load actual data
df_actual = load_actual(
    start_dt.strftime("%Y-%m-%d"),
    (end_actual_dt + timedelta(days=2)).strftime("%Y-%m-%d"),
)

if df_actual.empty:
    st.warning("No actual data found for the selected range.")
    st.stop()

# ── Build forecasts ───────────────────────────────────────────────────────────
forecast_start = end_actual_dt
forecast_slots = []
forecast_low   = []
forecast_high  = []
forecast_ts    = []

if api_ok:
    with st.spinner("🔮 Generating 48-slot forecast via FastAPI..."):
        # Pre-build a lookup of actual values for lag construction
        df_lookup = df_actual.set_index("date")

        for i in range(48):  # 48 × 30-min = 24h
            slot_dt = forecast_start + timedelta(minutes=30 * i)
            slot_dt_tz = slot_dt  # naive

            # Lag values — look up from actual or previously predicted
            def get_lag(dt_back):
                try:
                    return float(df_lookup.loc[
                        df_lookup.index.asof(dt_back), "total_consumption_gwh"
                    ])
                except Exception:
                    return df_actual["total_consumption_gwh"].mean()

            lag_1h   = get_lag(slot_dt - timedelta(hours=1))
            lag_24h  = get_lag(slot_dt - timedelta(hours=24))
            lag_168h = get_lag(slot_dt - timedelta(hours=168))
            ren_pct  = float(df_actual["avg_renewable_share_pct"].iloc[-1])

            feats = build_feature_vector(slot_dt, lag_1h, lag_24h, lag_168h, ren_pct)
            result = call_predict(feats)

            if result:
                forecast_ts.append(slot_dt)
                forecast_slots.append(result["predicted_gwh"])
                forecast_low.append(result["confidence_low"])
                forecast_high.append(result["confidence_high"])
            else:
                break  # API failed mid-way

df_forecast = pd.DataFrame({
    "date":   forecast_ts,
    "pred":   forecast_slots,
    "low":    forecast_low,
    "high":   forecast_high,
})

# ── Metrics ───────────────────────────────────────────────────────────────────
df_show_actual = df_actual[df_actual["date"] >= pd.Timestamp(start_dt)].copy()

col1, col2, col3, col4 = st.columns(4)
col1.metric("📊 Actual rows", f"{len(df_show_actual):,}")
col2.metric("🔮 Forecast slots", f"{len(df_forecast)}")
if not df_show_actual.empty:
    col3.metric("⚡ Current consumption", f"{df_show_actual['total_consumption_gwh'].iloc[-1]:.2f} GWh")
if not df_forecast.empty:
    col4.metric("📈 Next slot forecast", f"{df_forecast['pred'].iloc[0]:.2f} GWh")

# ── Plot ──────────────────────────────────────────────────────────────────────
fig = go.Figure()

# Actual consumption — solid blue
fig.add_trace(go.Scatter(
    x=df_show_actual["date"],
    y=df_show_actual["total_consumption_gwh"],
    name="Actual",
    mode="lines",
    line=dict(color="#3B82F6", width=2),
    hovertemplate="<b>Actual</b><br>%{x}<br>%{y:.3f} GWh<extra></extra>",
))

if not df_forecast.empty:
    # Convert dates to lists for Plotly 6 compatibility
    dates_fwd = df_forecast["date"].tolist()
    dates_rev = df_forecast["date"].tolist()[::-1]

    # Confidence band — shaded orange
    fig.add_trace(go.Scatter(
        x=dates_fwd + dates_rev,
        y=df_forecast["high"].tolist() + df_forecast["low"].tolist()[::-1],
        fill="toself",
        fillcolor="rgba(249,115,22,0.18)",
        line=dict(color="rgba(0,0,0,0)"),
        name="90% confidence band",
        hoverinfo="skip",
        showlegend=True,
    ))

    # Predicted — dashed orange
    fig.add_trace(go.Scatter(
        x=df_forecast["date"],
        y=df_forecast["pred"],
        name="Forecast (LightGBM)",
        mode="lines",
        line=dict(color="#F97316", width=2.5, dash="dash"),
        hovertemplate="<b>Forecast</b><br>%{x}<br>%{y:.3f} GWh<extra></extra>",
    ))

    # Vertical "now" line — use Scatter for Plotly 6 compatibility
    y_min = min(df_show_actual["total_consumption_gwh"].min(), df_forecast["low"].min())
    y_max = max(df_show_actual["total_consumption_gwh"].max(), df_forecast["high"].max())
    fig.add_trace(go.Scatter(
        x=[forecast_start, forecast_start],
        y=[y_min * 0.98, y_max * 1.02],
        mode="lines",
        name="Forecast start",
        line=dict(color="#94a3b8", width=1, dash="dot"),
        hoverinfo="skip",
        showlegend=False,
    ))

fig.update_layout(
    paper_bgcolor="#0f172a",
    plot_bgcolor="#0f172a",
    font=dict(color="#e2e8f0", family="Inter"),
    xaxis=dict(gridcolor="#1e293b", showgrid=True),
    yaxis=dict(gridcolor="#1e293b", showgrid=True, title="Consumption (GWh / 30-min slot)"),
    legend=dict(
        bgcolor="#1e293b", bordercolor="#334155", borderwidth=1,
        orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
    ),
    hovermode="x unified",
    margin=dict(l=0, r=0, t=40, b=0),
    height=480,
)

st.plotly_chart(fig, use_container_width=True)

# ── Forecast Table ────────────────────────────────────────────────────────────
if not df_forecast.empty:
    with st.expander("📋 Forecast data table"):
        show = df_forecast.copy()
        show["date"]  = show["date"].dt.strftime("%Y-%m-%d %H:%M")
        show.columns  = ["Timestamp", "Forecast (GWh)", "Low (GWh)", "High (GWh)"]
        st.dataframe(show, use_container_width=True, hide_index=True)

elif not api_ok:
    st.info("💡 Start the API to see forecasts: `uvicorn src.api.main:app --port 8000`")
