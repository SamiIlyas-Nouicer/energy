#!/bin/bash

# Define the project name
PROJECT_NAME="french-energy-grid-intelligence"

echo "🚀 Starting setup for $PROJECT_NAME..."

# 1. Create Directory Structure
mkdir -p $/{ingestion,processing,dbt,ml,api,dashboard,infra,docs,explore,data/raw}

cd $PROJECT_NAME

# 2. Initialize Git
git init
cat <<EOF > .gitignore
.venv/
__pycache__/
*.pyc
.env
data/raw/*
!data/raw/.gitkeep
target/
dbt_packages/
.DS_Store
EOF

# Ensure data/raw is tracked but empty
touch data/raw/.gitkeep

# 3. Create placeholder for environment variables
cat <<EOF > .env
# RTE API Credentials
RTE_CLIENT_ID=your_id_here
RTE_CLIENT_SECRET=your_secret_here

# Infrastructure
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password
EOF

# 4. Setup Python Virtual Environment (Debian 13 specific)
# Ensure python3-venv is installed: sudo apt install python3-venv
python3 -m venv .venv
source .venv/bin/activate

# 5. Create requirements.txt
cat <<EOF > requirements.txt
requests
python-dotenv
kafka-python
pyspark==3.5.0
delta-spark==3.0.0
pandas
dbt-duckdb
mlflow
scikit-learn
fastapi
uvicorn
streamlit
plotly
EOF

# 6. Initial Commit
git add .
git commit -m "Initial commit: Production-grade folder structure and env setup"

echo "✅ Setup complete!"
echo "📂 Location: $(pwd)"
echo "💡 To start, run: cd $PROJECT_NAME && source .venv/bin/activate && pip install -r requirements.txt"