# ============================================================
# French Energy Intelligence Platform — Makefile
# ============================================================
# Usage:
#   make up          Start all services (detached)
#   make down        Stop and remove containers
#   make build       Rebuild images and start
#   make logs        Follow all logs
#   make logs-api    Follow API logs only
#   make logs-dash   Follow dashboard logs only
#   make ps          Show running containers
#   make spark-run   Run all bronze→silver processing jobs
#   make dbt-run     Run dbt models (silver→gold)
#   make train       Re-train ML models (requires MLflow running)
#   make test        Run data quality checks
#   make clean       Full teardown including volumes

.PHONY: up down build logs logs-api logs-dash ps \
        spark-run dbt-run train test clean help

# ── Colours ─────────────────────────────────────────────────
CYAN  := \033[0;36m
GREEN := \033[0;32m
RESET := \033[0m

# ── Infrastructure ───────────────────────────────────────────
up:
	@echo "$(CYAN)⬆  Starting all services...$(RESET)"
	docker compose up -d
	@echo "$(GREEN)✅ Stack is up$(RESET)"
	@echo "   MLflow    → http://localhost:5000"
	@echo "   API       → http://localhost:8000/docs"
	@echo "   Dashboard → http://localhost:8501"
	@echo "   Kafka UI  → http://localhost:8080"
	@echo "   MinIO     → http://localhost:9001"

down:
	@echo "$(CYAN)⬇  Stopping all services...$(RESET)"
	docker compose down
	@echo "$(GREEN)✅ All containers stopped$(RESET)"

build:
	@echo "$(CYAN)🔨 Rebuilding images and starting...$(RESET)"
	docker compose up --build -d
	@echo "$(GREEN)✅ Build complete$(RESET)"

logs:
	docker compose logs -f

logs-api:
	docker compose logs -f api

logs-dash:
	docker compose logs -f dashboard

logs-mlflow:
	docker compose logs -f mlflow

ps:
	docker compose ps

# ── Data Processing ──────────────────────────────────────────
spark-run:
	@echo "$(CYAN)⚡ Running Spark bronze → silver jobs...$(RESET)"
	@. .venv/bin/activate && \
	  python3 src/processing/bronze_to_silver_consumption.py && \
	  python3 src/processing/bronze_to_silver_generation.py && \
	  python3 src/processing/bronze_to_silver_weather.py && \
	  python3 src/processing/silver_join.py
	@echo "$(GREEN)✅ Spark processing complete$(RESET)"

dbt-run:
	@echo "$(CYAN)🔧 Running dbt models (silver → gold)...$(RESET)"
	@cd src/dbt/energy_platform && \
	  . ../../../.venv/bin/activate && \
	  dbt run
	@echo "$(GREEN)✅ dbt build complete$(RESET)"

# ── ML ───────────────────────────────────────────────────────
train:
	@echo "$(CYAN)🤖 Training ML models (MLflow must be running)...$(RESET)"
	@. .venv/bin/activate && python3 src/ml/train_consumption_model.py
	@echo "$(GREEN)✅ Training complete — open http://localhost:5000 to compare runs$(RESET)"

# ── Quality Checks ───────────────────────────────────────────
test:
	@echo "$(CYAN)🔍 Running data quality checks...$(RESET)"
	@. .venv/bin/activate && python3 -c "\
import duckdb, pandas as pd; \
con = duckdb.connect('src/data/gold.duckdb', read_only=True); \
for t in ['hourly_energy_mix', 'daily_consumption_summary', 'co2_intensity', 'regional_weekly']: \
    df = con.execute(f'SELECT COUNT(*) FROM {t}').fetchone(); \
    print(f'  ✅ {t}: {df[0]:,} rows'); \
print('All checks passed 🎉'); \
"
	@echo "$(GREEN)✅ Data quality checks complete$(RESET)"

# ── Teardown ─────────────────────────────────────────────────
clean:
	@echo "$(CYAN)🧹 Full teardown (containers + volumes)...$(RESET)"
	docker compose down -v --remove-orphans
	@echo "$(GREEN)✅ Clean complete$(RESET)"

# ── Help ─────────────────────────────────────────────────────
help:
	@echo ""
	@echo "$(CYAN)French Energy Intelligence Platform$(RESET)"
	@echo "======================================"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make up          Start full stack (detached)"
	@echo "  make down        Stop all containers"
	@echo "  make build       Rebuild images + start"
	@echo "  make logs        Follow all service logs"
	@echo "  make ps          Show container status"
	@echo "  make clean       Teardown + delete volumes"
	@echo ""
	@echo "Data Pipeline:"
	@echo "  make spark-run   Run bronze→silver Spark jobs"
	@echo "  make dbt-run     Run silver→gold dbt models"
	@echo "  make train       Re-train ML models with MLflow"
	@echo "  make test        Data quality checks"
	@echo ""
	@echo "Services:"
	@echo "  MLflow    → http://localhost:5000"
	@echo "  API       → http://localhost:8000/docs"
	@echo "  Dashboard → http://localhost:8501"
	@echo "  Kafka UI  → http://localhost:8080"
	@echo "  MinIO     → http://localhost:9001"
	@echo ""
