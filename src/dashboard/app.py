"""
dashboard/app.py — French Energy Intelligence Platform
========================================================
Main Streamlit entry point. Configures global page settings,
applies the custom CSS theme, and renders the sidebar navigation.
Each page lives in pages/ and is auto-discovered by Streamlit's
multi-page app system.
"""

import streamlit as st

st.set_page_config(
    page_title="French Energy Intelligence Platform",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  /* Import font */
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

  /* Dark sidebar */
  [data-testid="stSidebar"] {
      background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%);
  }
  [data-testid="stSidebar"] * { color: #e2e8f0 !important; }

  /* Metric cards */
  [data-testid="metric-container"] {
      background: #1e293b;
      border: 1px solid #334155;
      border-radius: 12px;
      padding: 16px 20px;
  }
  [data-testid="stMetricValue"] { color: #38bdf8 !important; font-weight: 700; }
  [data-testid="stMetricLabel"] { color: #94a3b8 !important; }

  /* Main background */
  .main .block-container { padding-top: 2rem; }
  [data-testid="stAppViewContainer"] { background: #0f172a; color: #e2e8f0; }

  /* Headers */
  h1, h2, h3 { color: #f1f5f9 !important; }

  /* Divider */
  hr { border-color: #334155; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ Energy Platform")
    st.markdown("**French Energy Intelligence**")
    st.markdown("---")
    st.markdown("### Navigation")
    st.markdown("""
    - 🔴 **Live Energy Mix** — Real-time generation
    - 🗺️ **Regional Map** — Per-capita consumption
    - 📈 **Forecast** — 24h ahead predictions
    - 🔧 **Pipeline Health** — Data quality
    """)
    st.markdown("---")
    st.markdown("**Data:** RTE France Open API")
    st.markdown("**Model:** LightGBM — MAE 0.33 GWh")
    st.markdown("**Stack:** DuckDB · MLflow · FastAPI")

# ── Hero ───────────────────────────────────────────────────────────────────────
st.markdown("# ⚡ French Energy Intelligence Platform")
st.markdown("*Real-time monitoring, regional analysis, ML-powered forecasting, and pipeline observability.*")
st.markdown("---")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.info("🔴 **Live Energy Mix**\n\nReal-time generation mix and CO₂ intensity for the last 48 hours.")
with col2:
    st.info("🗺️ **Regional Map**\n\nChoropleth map of per-capita consumption by French region.")
with col3:
    st.info("📈 **Consumption Forecast**\n\n24-hour ahead LightGBM predictions with confidence bands.")
with col4:
    st.info("🔧 **Pipeline Health**\n\nData quality dashboard — null rates, anomalies, freshness.")

st.markdown("---")
st.markdown("*Use the sidebar to navigate between pages →*")
