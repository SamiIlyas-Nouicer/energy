"""
ingestion/backfill_historical.py

One-off script: loads all éCO2mix .xls files from data/raw/, cleans them
using the same logic as load_data.py, and writes partitioned JSON files
to MinIO under bronze/historical/year=YYYY/month=MM/.

Run once:
    python ingestion/backfill_historical.py
"""

import glob
import json
import logging
import os
from io import BytesIO

from minio import Minio
from minio.error import S3Error
import pandas as pd

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR  = os.path.join(BASE_DIR, "..", "data", "raw")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE     = os.getenv("MINIO_SECURE",     "false").lower() == "true"
BUCKET           = os.getenv("MINIO_BUCKET",     "energy-lake")

# Positional column names matching the éCO2mix xls tab-separated format
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
    "wind_onshore_mw", "wind_offshore_mw", "extra",
]

# ---------------------------------------------------------------------------
# Load & clean
# ---------------------------------------------------------------------------

def load_all_xls(raw_dir: str) -> pd.DataFrame:
    files = glob.glob(os.path.join(raw_dir, "*.xls"))
    if not files:
        raise FileNotFoundError(f"No .xls files found in {raw_dir}")

    log.info("Found %d .xls file(s):", len(files))
    for f in files:
        log.info("  %s", os.path.basename(f))

    dfs = []
    for f in files:
        tmp = pd.read_csv(
            f,
            sep="\t",
            encoding="latin-1",
            header=None,
            names=COLS,
            skiprows=1,
        )
        dfs.append(tmp)

    df = pd.concat(dfs).drop_duplicates()
    df = df.drop(columns=["extra"], errors="ignore")

    # Keep only real data rows (skip repeated header rows mid-file)
    df = df[df["date"].str.match(r"\d{4}-\d{2}-\d{2}", na=False)]

    # Build timestamp
    df["timestamp"] = pd.to_datetime(df["date"] + " " + df["heure"], dayfirst=False)

    # Drop helper columns
    df = df.drop(columns=["perimetre", "nature", "date", "heure"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    # Cast all metric columns to numeric
    for col in df.columns:
        if col != "timestamp":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Keep only rows with actual measurements (drop forecast-only rows)
    df = df.dropna(subset=["consumption_mw"])
    df = df.reset_index(drop=True)

    log.info(
        "Loaded %d rows  |  %s -> %s",
        len(df),
        df["timestamp"].min(),
        df["timestamp"].max(),
    )
    return df

# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

def upload_to_minio(df: pd.DataFrame) -> None:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

    try:
        if not client.bucket_exists(BUCKET):
            client.make_bucket(BUCKET)
            log.info("Created bucket: %s", BUCKET)
    except S3Error as exc:
        log.error("MinIO connection failed: %s", exc)
        raise

    log.info("Starting upload to MinIO  bucket=%s", BUCKET)
    uploaded_rows = 0
    partition_count = 0

    for (year, month), group in df.groupby(
        [df["timestamp"].dt.year, df["timestamp"].dt.month]
    ):
        path = (
            f"bronze/historical/"
            f"year={int(year)}/month={int(month):02d}/"
            f"historical_batch.json"
        )

        batch = group.copy()
        batch["timestamp"] = batch["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")
        content = json.dumps(batch.to_dict(orient="records")).encode("utf-8")

        client.put_object(
            BUCKET,
            path,
            BytesIO(content),
            len(content),
            content_type="application/json",
        )
        log.info("  uploaded: %s  (%d rows)", path, len(batch))
        uploaded_rows += len(batch)
        partition_count += 1

    log.info(
        "Backfill complete -- %d rows uploaded across %d partitions.",
        uploaded_rows,
        partition_count,
    )

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    df = load_all_xls(RAW_DIR)
    upload_to_minio(df)


if __name__ == "__main__":
    main()