import glob
import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR  = os.path.join(BASE_DIR, "..", "data", "raw")

COLS = [
    "perimetre", "nature", "date", "heure",
    "consumption_mw", "forecast_j1_mw", "forecast_j_mw",
    "oil_mw", "coal_mw", "gas_mw", "nuclear_mw",
    "wind_mw", "solar_mw", "hydro_mw", "pumping_mw", "bio_mw",
    "physical_flows_mw", "co2_rate_gco2_per_kwh",
    "flow_england_mw", "flow_spain_mw", "flow_italy_mw",
    "flow_switzerland_mw", "flow_germany_belgium_mw",
    "oil_tac_mw", "oil_cogen_mw", "oil_other_mw",
    "gas_tac_mw", "gas_cogen_mw", "gas_ccg_mw", "gas_other_mw",
    "hydro_river_mw", "hydro_lake_mw", "hydro_pumped_mw",
    "bio_waste_mw", "bio_biomass_mw", "bio_biogas_mw",
    "battery_charge_mw", "battery_discharge_mw",
    "wind_onshore_mw", "wind_offshore_mw", "extra"
]

files = glob.glob(os.path.join(RAW_DIR, "*.xls"))
dfs = []
for f in files:
    tmp = pd.read_csv(f, sep="\t", encoding="latin-1",
                      header=None, names=COLS, skiprows=1)
    dfs.append(tmp)

df = pd.concat(dfs).drop_duplicates()

# Drop the extra column and rows that are headers repeated mid-file
df = df.drop(columns=["extra"], errors="ignore")
df = df[df["date"].str.match(r"\d{4}-\d{2}-\d{2}", na=False)]  # keep only real data rows

# Build timestamp
df["timestamp"] = pd.to_datetime(df["date"] + " " + df["heure"], dayfirst=False)

# Drop helper columns
df = df.drop(columns=["perimetre", "nature", "date", "heure"])
df = df.sort_values("timestamp").reset_index(drop=True)

# Convert all MW columns to numeric
for col in df.columns:
    if col != "timestamp":
        df[col] = pd.to_numeric(df[col], errors="coerce")

# print(f"Shape     : {df.shape}")
# print(f"Date range: {df['timestamp'].min()} → {df['timestamp'].max()}")
# print(f"Nulls:\n{df.isnull().sum()}")
# print(df.head(3))

# Keep only rows where we have actual measurements
df = df.dropna(subset=["consumption_mw"])
df = df.reset_index(drop=True)

# print(f"Shape after dropping forecast-only rows: {df.shape}")
# print(f"Date range: {df['timestamp'].min()} → {df['timestamp'].max()}")
# print(f"Max nulls remaining: {df.isnull().sum().max()}")
# print(df.head(3))


print(df[["consumption_mw", "nuclear_mw", "solar_mw", "wind_mw", "co2_rate_gco2_per_kwh"]].describe().round(1))