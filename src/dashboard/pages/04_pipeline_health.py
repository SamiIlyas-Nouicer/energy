"""
Page 4 — Pipeline Health
==========================
Monitors data quality across the gold layer:
  • Freshness: how many minutes since last data ingestion
  • Row counts per table
  • Null rates on key columns
  • Physical anomaly detection (consumption outside valid range)
  • Pass/Fail status icon per check
"""

import duckdb
import pandas as pd
import streamlit as st
from datetime import datetime, timezone

DUCKDB_PATH = "src/data/gold.duckdb"

# Physical bounds for French national grid (MW / MWh per 30-min slot)
BOUNDS = {
    "consumption_mwh": (10_000, 120_000),   # 10 GW – 120 GW
    "renewable_share_pct": (0, 100),
    "co2_intensity_gco2_per_kwh": (0, 200),
}

st.set_page_config(page_title="Pipeline Health", page_icon="🔧", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
[data-testid="stAppViewContainer"] { background: #0f172a; color: #e2e8f0; }
h1, h2, h3 { color: #f1f5f9 !important; }
[data-testid="metric-container"] {
    background: #1e293b; border: 1px solid #334155;
    border-radius: 12px; padding: 14px;
}
[data-testid="stMetricValue"] { color: #38bdf8 !important; }
</style>
""", unsafe_allow_html=True)


# ── Data Loaders ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_health_stats() -> dict:
    """Pull quality statistics from the gold DuckDB layer."""
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    stats = {}

    # --- hourly_energy_mix ---
    df_mix = con.execute("""
        SELECT date, consumption_mwh, renewable_share_pct,
               nuclear_mwh, solar_mwh, wind_mwh, hydro_mwh
        FROM hourly_energy_mix
        ORDER BY date DESC
    """).fetchdf()

    stats["energy_mix"] = {
        "table":    "hourly_energy_mix",
        "rows":     len(df_mix),
        "latest":   pd.to_datetime(df_mix["date"].max()),
        "null_consumption_pct": df_mix["consumption_mwh"].isna().mean() * 100,
        "null_renewable_pct":   df_mix["renewable_share_pct"].isna().mean() * 100,
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

    stats["co2"] = {
        "table":    "co2_intensity",
        "rows":     len(df_co2),
        "latest":   pd.to_datetime(df_co2["timestamp"].max()),
        "null_co2_pct": df_co2["co2_intensity_gco2_per_kwh"].isna().mean() * 100,
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

    stats["consumption"] = {
        "table":    "daily_consumption_summary",
        "rows":     len(df_cons),
        "latest":   pd.to_datetime(df_cons["date"].max()),
        "null_consumption_pct": df_cons["total_consumption_gwh"].isna().mean() * 100,
        "null_temperature_pct": df_cons["avg_temperature_celsius"].isna().mean() * 100,
        "anomalies": int(
            ((df_cons["total_consumption_gwh"] < 10) |
             (df_cons["total_consumption_gwh"] > 120)).sum()
        ),
    }

    # --- regional_weekly ---
    df_reg = con.execute("SELECT * FROM regional_weekly").fetchdf()
    stats["regional"] = {
        "table":  "regional_weekly",
        "rows":   len(df_reg),
        "latest": pd.to_datetime(df_reg["week_start"].max()),
        "null_pct": df_reg.isna().mean().mean() * 100,
        "regions": df_reg["region"].nunique(),
    }

    con.close()
    return stats


def freshness_label(latest_dt: pd.Timestamp) -> tuple[str, bool]:
    """Return human-readable freshness and pass/fail."""
    if pd.isna(latest_dt):
        return "Unknown", False
    now = datetime.now()
    delta = now - latest_dt.to_pydatetime().replace(tzinfo=None)
    mins  = int(delta.total_seconds() / 60)
    if mins < 60:
        return f"{mins} min ago", True
    elif mins < 1440:
        return f"{mins // 60}h {mins % 60}m ago", True
    else:
        return f"{delta.days} days ago", False


# ── Page ──────────────────────────────────────────────────────────────────────
st.markdown("# 🔧 Pipeline Health Monitor")
st.markdown("*Data quality checks, freshness, null rates, and anomaly detection*")

stats = load_health_stats()

# ── Overall Freshness Banner ──────────────────────────────────────────────────
latest_across_all = max(
    (v["latest"] for v in stats.values() if pd.notna(v.get("latest"))),
    default=None,
)
freshness_str, is_fresh = freshness_label(latest_across_all) if latest_across_all else ("Unknown", False)

if is_fresh:
    st.success(f"✅ **Pipeline healthy** — latest data: **{freshness_str}**")
else:
    st.error(f"⚠️ **Stale data** — latest ingestion: **{freshness_str}**")

st.markdown("---")

# ── Per-Table Checks ──────────────────────────────────────────────────────────
st.markdown("### 📊 Table-by-Table Status")

def status_icon(ok: bool) -> str:
    return "✅" if ok else "❌"

check_rows = []

# Build summary table
for key, s in stats.items():
    freshness, fresh_ok = freshness_label(s.get("latest"))
    null_pct = s.get("null_consumption_pct", s.get("null_pct", s.get("null_co2_pct", 0)))
    null_ok  = null_pct < 5  # pass if <5% null
    anomalies = s.get("anomalies_consumption", s.get("anomalies", s.get("anomalies_co2", 0)))
    anom_ok  = anomalies == 0

    overall = fresh_ok and null_ok and anom_ok
    check_rows.append({
        "Status":       status_icon(overall),
        "Table":        s["table"],
        "Rows":         f"{s['rows']:,}",
        "Latest data":  freshness,
        "Null rate":    f"{null_pct:.2f}%",
        "Anomalies":    str(anomalies),
        "Freshness":    status_icon(fresh_ok),
        "Nulls OK":     status_icon(null_ok),
        "Anomalies OK": status_icon(anom_ok),
    })

check_df = pd.DataFrame(check_rows)
st.dataframe(check_df, use_container_width=True, hide_index=True)

# ── KPI Row ───────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### 🔢 Gold Layer Summary")

col1, col2, col3, col4 = st.columns(4)
col1.metric("hourly_energy_mix rows",   f"{stats['energy_mix']['rows']:,}")
col2.metric("co2_intensity rows",        f"{stats['co2']['rows']:,}")
col3.metric("daily_consumption rows",    f"{stats['consumption']['rows']:,}")
col4.metric("Regions tracked",           str(stats["regional"]["regions"]))

# ── Anomaly Detail ────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("### ⚠️ Anomaly Details")

with st.expander("Energy Mix — consumption_mwh anomalies"):
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df_anom = con.execute(f"""
        SELECT date, consumption_mwh, renewable_share_pct
        FROM hourly_energy_mix
        WHERE consumption_mwh < {BOUNDS['consumption_mwh'][0]}
           OR consumption_mwh > {BOUNDS['consumption_mwh'][1]}
        ORDER BY date DESC
        LIMIT 20
    """).fetchdf()
    con.close()
    if df_anom.empty:
        st.success("No anomalies found ✅")
    else:
        st.warning(f"{len(df_anom)} anomalous rows detected")
        st.dataframe(df_anom, use_container_width=True, hide_index=True)

with st.expander("Temperature data coverage"):
    null_temp = stats["consumption"]["null_temperature_pct"]
    if null_temp > 95:
        st.warning(
            f"⚠️ `avg_temperature_celsius` is **{null_temp:.0f}% null** — "
            "the weather join (silver_join.py) has not been run on this dataset. "
            "Temperature features are excluded from the ML model for this reason."
        )
    else:
        st.success(f"Temperature coverage: {100 - null_temp:.1f}% complete")

# ── Refresh ───────────────────────────────────────────────────────────────────
st.markdown("---")
if st.button("🔄 Refresh now"):
    st.cache_data.clear()
    st.rerun()

st.caption("⟳ Auto-refreshes every 60 seconds")
st.rerun(interval=60)
