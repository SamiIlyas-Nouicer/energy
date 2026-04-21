"""
Page 2 — Regional Map of France
=================================
Shows a Plotly choropleth_mapbox map of French metropolitan regions,
colored by weekly per-capita electricity consumption (kWh).
Includes a week selector and hover details.
"""

import json

import duckdb
import pandas as pd
import plotly.express as px
import requests
import streamlit as st

DUCKDB_PATH = "src/data/gold.duckdb"
GEOJSON_URL = (
    "https://raw.githubusercontent.com/gregoiredavid/"
    "france-geojson/master/regions.geojson"
)

st.set_page_config(page_title="Regional Map", page_icon="🗺️", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
[data-testid="stAppViewContainer"] { background: #0f172a; color: #e2e8f0; }
h1, h2, h3 { color: #f1f5f9 !important; }
[data-testid="stSelectbox"] label { color: #94a3b8 !important; }
</style>
""", unsafe_allow_html=True)


# ── Data Loaders ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_geojson() -> dict:
    """Download and cache French regions GeoJSON."""
    try:
        resp = requests.get(GEOJSON_URL, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        st.error(f"Could not download GeoJSON: {e}")
        return None


@st.cache_data(ttl=300)
def load_regional_data() -> pd.DataFrame:
    """Load regional_weekly table from DuckDB gold layer."""
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute("""
        SELECT week_start, region, population,
               regional_consumption_gwh,
               consumption_kwh_per_capita,
               avg_renewable_share_pct
        FROM regional_weekly
        ORDER BY week_start DESC
    """).fetchdf()
    con.close()
    df["week_start"] = pd.to_datetime(df["week_start"]).dt.date
    return df


# ── Page ──────────────────────────────────────────────────────────────────────
st.markdown("# 🗺️ Regional Energy Map — France")
st.markdown("*Weekly per-capita electricity consumption by metropolitan region*")

geojson = load_geojson()
df_all   = load_regional_data()

if df_all.empty:
    st.error("No regional data found in the gold layer.")
    st.stop()

# ── Controls ──────────────────────────────────────────────────────────────────
weeks = sorted(df_all["week_start"].unique(), reverse=True)

col_ctrl1, col_ctrl2, _ = st.columns([2, 2, 4])
with col_ctrl1:
    selected_week = st.selectbox(
        "📅 Select week",
        options=weeks,
        format_func=lambda d: d.strftime("%d %b %Y") if hasattr(d, "strftime") else str(d),
    )
with col_ctrl2:
    metric_choice = st.selectbox(
        "📊 Color by",
        options=["consumption_kwh_per_capita", "avg_renewable_share_pct", "regional_consumption_gwh"],
        format_func=lambda x: {
            "consumption_kwh_per_capita": "Per-capita consumption (kWh)",
            "avg_renewable_share_pct":    "Renewable share (%)",
            "regional_consumption_gwh":   "Total consumption (GWh)",
        }[x],
    )

df_week = df_all[df_all["week_start"] == selected_week].copy()

if df_week.empty:
    st.warning("No data for the selected week.")
    st.stop()

# ── Map ───────────────────────────────────────────────────────────────────────
color_labels = {
    "consumption_kwh_per_capita": "kWh / capita",
    "avg_renewable_share_pct":    "Renewable %",
    "regional_consumption_gwh":   "GWh total",
}
color_scales = {
    "consumption_kwh_per_capita": "Blues",
    "avg_renewable_share_pct":    "Greens",
    "regional_consumption_gwh":   "Oranges",
}

if geojson:
    fig = px.choropleth_mapbox(
        df_week,
        geojson=geojson,
        locations="region",
        featureidkey="properties.nom",
        color=metric_choice,
        color_continuous_scale=color_scales[metric_choice],
        mapbox_style="carto-darkmatter",
        zoom=4.5,
        center={"lat": 46.8, "lon": 2.3},
        opacity=0.75,
        labels={metric_choice: color_labels[metric_choice]},
        hover_name="region",
        hover_data={
            "region": False,
            "consumption_kwh_per_capita": ":.1f",
            "avg_renewable_share_pct":    ":.1f",
            "regional_consumption_gwh":   ":.2f",
            "population":                 ":,",
        },
        custom_data=["consumption_kwh_per_capita", "avg_renewable_share_pct",
                     "regional_consumption_gwh", "population"],
    )
    fig.update_traces(
        hovertemplate=(
            "<b>%{hovertext}</b><br><br>"
            "Per-capita: <b>%{customdata[0]:.1f} kWh</b><br>"
            "Renewable:  <b>%{customdata[1]:.1f}%</b><br>"
            "Total:      <b>%{customdata[2]:.2f} GWh</b><br>"
            "Population: <b>%{customdata[3]:,}</b>"
            "<extra></extra>"
        )
    )
    fig.update_layout(
        paper_bgcolor="#0f172a",
        font=dict(color="#e2e8f0", family="Inter"),
        coloraxis_colorbar=dict(
            bgcolor="#1e293b", tickcolor="#e2e8f0",
            title=dict(text=color_labels[metric_choice], font=dict(color="#e2e8f0")),
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        height=580,
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("GeoJSON unavailable — showing table instead.")

# ── Data Table ────────────────────────────────────────────────────────────────
st.markdown("### 📋 Region Data Table")
display_df = df_week[[
    "region", "population", "regional_consumption_gwh",
    "consumption_kwh_per_capita", "avg_renewable_share_pct",
]].sort_values("consumption_kwh_per_capita", ascending=False).reset_index(drop=True)

display_df.columns = [
    "Region", "Population", "Total (GWh)", "Per-capita (kWh)", "Renewable %"
]
display_df["Population"]    = display_df["Population"].apply(lambda x: f"{x:,.0f}")
display_df["Total (GWh)"]   = display_df["Total (GWh)"].apply(lambda x: f"{x:.2f}")
display_df["Per-capita (kWh)"] = display_df["Per-capita (kWh)"].apply(lambda x: f"{x:.1f}")
display_df["Renewable %"]   = display_df["Renewable %"].apply(lambda x: f"{x:.1f}%")

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
)

# ── Stats ─────────────────────────────────────────────────────────────────────
st.markdown("---")
col1, col2, col3 = st.columns(3)
col1.metric("🏆 Highest consumption", df_week.loc[df_week["consumption_kwh_per_capita"].idxmax(), "region"])
col2.metric("🌿 Most renewable", df_week.loc[df_week["avg_renewable_share_pct"].idxmax(), "region"])
col3.metric("📊 National avg per-capita", f"{df_week['consumption_kwh_per_capita'].mean():.1f} kWh")
