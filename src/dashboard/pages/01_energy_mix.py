"""
Page 1 — Live Energy Mix (Hero Page)
======================================
Shows:
  • 3 bold metric cards: renewable %, CO₂ intensity, national consumption
  • Stacked area chart: last 48h of generation by source (Plotly)
  • Auto-refreshes every 60 seconds via st.rerun(interval=60)
"""

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────
DUCKDB_PATH = "src/data/gold.duckdb"

SOURCE_COLORS = {
    "nuclear_mwh": ("#3B82F6", "Nuclear"),
    "wind_mwh":    ("#34D399", "Wind"),
    "solar_mwh":   ("#FBBF24", "Solar"),
    "hydro_mwh":   ("#14B8A6", "Hydro"),
    "bio_mwh":     ("#A78BFA", "Bio"),
    "gas_mwh":     ("#F97316", "Gas"),
    "coal_mwh":    ("#6B7280", "Coal"),
    "oil_mwh":     ("#EF4444", "Oil"),
}

st.set_page_config(page_title="Live Energy Mix", page_icon="🔴", layout="wide")

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
[data-testid="stAppViewContainer"] { background: #0f172a; color: #e2e8f0; }
[data-testid="metric-container"] {
    background: #1e293b; border: 1px solid #334155;
    border-radius: 12px; padding: 16px 20px;
}
[data-testid="stMetricValue"] { color: #38bdf8 !important; font-weight: 700; }
[data-testid="stMetricLabel"] { color: #94a3b8 !important; }
h1, h2, h3 { color: #f1f5f9 !important; }
</style>
""", unsafe_allow_html=True)


# ── Data Loading ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_last_48h():
    """Load last 96 rows (48h at 30-min intervals) from hourly_energy_mix."""
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute("""
        SELECT date, nuclear_mwh, solar_mwh, wind_mwh, hydro_mwh,
               bio_mwh, gas_mwh, coal_mwh, oil_mwh,
               total_production_mwh, renewable_share_pct, consumption_mwh
        FROM hourly_energy_mix
        ORDER BY date DESC
        LIMIT 96
    """).fetchdf()
    con.close()
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


@st.cache_data(ttl=60)
def load_latest_co2():
    """Load most recent CO₂ intensity value."""
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    row = con.execute("""
        SELECT co2_intensity_gco2_per_kwh
        FROM co2_intensity
        ORDER BY timestamp DESC
        LIMIT 1
    """).fetchone()
    con.close()
    return round(row[0], 1) if row else None


# ── Page ──────────────────────────────────────────────────────────────────────
st.markdown("# 🔴 Live Energy Mix")
st.markdown("*Real-time French electricity generation — last 48 hours · refreshes every 60 s*")

df = load_last_48h()
co2 = load_latest_co2()

if df.empty:
    st.error("No data available in the gold layer.")
    st.stop()

# Latest row for KPIs
latest = df.iloc[-1]
prev   = df.iloc[-2] if len(df) > 1 else latest

renewable_pct = round(latest["renewable_share_pct"], 1)
renewable_delta = round(renewable_pct - prev["renewable_share_pct"], 1)
consumption_gw  = round(latest["consumption_mwh"] / 1000, 1)
consumption_delta = round((consumption_gw - prev["consumption_mwh"] / 1000), 1)

# ── KPI Metrics ───────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("⚡ Renewable Share",  f"{renewable_pct} %",  f"{renewable_delta:+.1f}%")
col2.metric("🌫️ CO₂ Intensity",   f"{co2} gCO₂/kWh" if co2 else "N/A")
col3.metric("🔌 Consumption",      f"{consumption_gw} GW", f"{consumption_delta:+.1f} GW")
col4.metric("📅 Last Reading",     str(latest["date"].strftime("%d %b %Y %H:%M")))

st.markdown("---")

# ── Stacked Area Chart ────────────────────────────────────────────────────────
st.markdown("### ⚡ Generation by Source (last 48h)")

fig = go.Figure()

for col_name, (color, label) in SOURCE_COLORS.items():
    if col_name in df.columns:
        fig.add_trace(go.Scatter(
            x=df["date"],
            y=df[col_name] / 1000,   # MWh → GWh
            name=label,
            mode="lines",
            stackgroup="one",
            line=dict(width=0.5, color=color),
            fillcolor=color,
            hovertemplate=f"<b>{label}</b><br>%{{x}}<br>%{{y:.2f}} GWh<extra></extra>",
        ))

# Overlay consumption line
fig.add_trace(go.Scatter(
    x=df["date"],
    y=df["consumption_mwh"] / 1000,
    name="Demand",
    mode="lines",
    line=dict(color="#F8FAFC", width=2, dash="dash"),
    hovertemplate="<b>Demand</b><br>%{x}<br>%{y:.2f} GWh<extra></extra>",
))

fig.update_layout(
    paper_bgcolor="#0f172a",
    plot_bgcolor="#0f172a",
    font=dict(color="#e2e8f0", family="Inter"),
    legend=dict(
        bgcolor="#1e293b", bordercolor="#334155", borderwidth=1,
        orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
    ),
    xaxis=dict(gridcolor="#1e293b", showgrid=True),
    yaxis=dict(gridcolor="#1e293b", showgrid=True, title="GWh per 30-min slot"),
    hovermode="x unified",
    margin=dict(l=0, r=0, t=40, b=0),
    height=450,
)

st.plotly_chart(fig, use_container_width=True)

# ── Renewable breakdown ───────────────────────────────────────────────────────
st.markdown("### 🌱 Current Renewable Breakdown")
renewable_cols = ["wind_mwh", "solar_mwh", "hydro_mwh", "bio_mwh"]
ren_vals  = {SOURCE_COLORS[c][1]: latest[c] for c in renewable_cols if c in latest}
total_ren = sum(ren_vals.values())

rcols = st.columns(len(ren_vals))
for i, (name, val) in enumerate(ren_vals.items()):
    pct = (val / latest["total_production_mwh"] * 100) if latest["total_production_mwh"] > 0 else 0
    rcols[i].metric(f"🌿 {name}", f"{val/1000:.2f} GWh", f"{pct:.1f}% of total")

# ── Auto-refresh ──────────────────────────────────────────────────────────────
st.markdown("---")
st.caption("⟳ Auto-refreshes every 60 seconds")
st.rerun(interval=60)
