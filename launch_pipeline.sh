#!/usr/bin/env bash
# =============================================================================
# Real-Time Energy & Weather Data Pipeline — One-Shot Launcher
# Tested on Debian 13. Run from the root of your project directory.
# Usage: bash launch_pipeline.sh [--backfill]
# =============================================================================

set -euo pipefail

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
die()     { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

BACKFILL=false
[[ "${1:-}" == "--backfill" ]] && BACKFILL=true

# ── 0. Sanity checks ─────────────────────────────────────────────────────────
info "Checking prerequisites…"

command -v docker   >/dev/null 2>&1 || die "Docker is not installed."
command -v python3  >/dev/null 2>&1 || die "python3 is not installed."

[[ -f ".env" ]]          || die ".env file not found in $(pwd). Add your RTE & MinIO credentials."
[[ -d ".venv" ]]         || die ".venv not found. Create it with: python3 -m venv .venv && pip install -r requirements.txt"
[[ -f "requirements.txt" ]] || warn "requirements.txt not found — skipping dependency install."

success "Prerequisites OK"

# ── 1. Install / sync Python dependencies ────────────────────────────────────
info "Activating venv and syncing dependencies…"
# shellcheck disable=SC1091
source .venv/bin/activate

if [[ -f "requirements.txt" ]]; then
    pip install -q -r requirements.txt
    success "Dependencies installed"
fi

# ── 2. Start infrastructure ──────────────────────────────────────────────────
info "Starting Docker infrastructure (Kafka + Zookeeper + MinIO)…"
docker compose up -d
info "Waiting 25 s for brokers to fully initialise…"
sleep 25
success "Infrastructure is up"

# ── 3. Optional historical backfill ──────────────────────────────────────────
if $BACKFILL; then
    info "Running historical backfill (this may take a while)…"
    python3 src/ingestion/backfill_historical.py
    success "Backfill complete"
fi

# ── 4. Start the three ingestion processes in the background ─────────────────
info "Launching ingestion layer (3 background processes)…"

python3 src/ingestion/producer_rte.py     > logs/producer_rte.log     2>&1 &
PID_RTE=$!

python3 src/ingestion/producer_weather.py > logs/producer_weather.log 2>&1 &
PID_WEATHER=$!

python3 src/ingestion/consumer_bronze.py  > logs/consumer_bronze.log  2>&1 &
PID_BRONZE=$!

success "Ingestion PIDs — RTE: $PID_RTE | Weather: $PID_WEATHER | Bronze: $PID_BRONZE"
info "Logs are in ./logs/"

# Give producers a head-start before processing begins
info "Waiting 30 s for initial data to land in MinIO…"
sleep 30

# ── 5. Bronze → Silver processing ────────────────────────────────────────────
info "Running Bronze → Silver processing (PySpark)…"

python3 src/processing/bronze_to_silver_consumption.py
python3 src/processing/bronze_to_silver_generation.py
python3 src/processing/bronze_to_silver_weather.py
python3 src/processing/silver_join.py

success "Bronze → Silver complete"

# ── 6. Silver → Gold transformation (dbt) ────────────────────────────────────
info "Running dbt transformations (Silver → Gold)…"

(
    cd src/dbt/energy_platform
    dbt run
)

success "dbt run complete — Gold layer is ready"

# ── 7. Summary ───────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${GREEN}════════════════════════════════════════${NC}"
echo -e "${BOLD}${GREEN}  Pipeline launched successfully!        ${NC}"
echo -e "${BOLD}${GREEN}════════════════════════════════════════${NC}"
echo ""
echo -e "  Streaming producers running in background:"
echo -e "    • producer_rte     PID ${PID_RTE}"
echo -e "    • producer_weather PID ${PID_WEATHER}"
echo -e "    • consumer_bronze  PID ${PID_BRONZE}"
echo ""
echo -e "  To stop all background processes:"
echo -e "    kill ${PID_RTE} ${PID_WEATHER} ${PID_BRONZE}"
echo ""
echo -e "  To stop Docker infrastructure:"
echo -e "    docker compose down"
echo ""
echo -e "  Connect Looker Studio to your MinIO Gold layer to build dashboards."
echo ""

# Keep script alive so Ctrl-C cleanly kills the background jobs
trap "info 'Shutting down background processes…'; kill ${PID_RTE} ${PID_WEATHER} ${PID_BRONZE} 2>/dev/null; exit 0" INT TERM

info "Press Ctrl-C to stop all streaming processes and exit."
wait
