Here is the complete, unbroken README text inside a single code block so you can copy it all with one click. I also went ahead and added the Looker Studio visualization section at the bottom to round out the project structure!

**Markdown**

```
# Real-Time Energy & Weather Data Pipeline

This project is an end-to-end Data Engineering pipeline that streams, processes, and transforms real-time energy data (from RTE France) and weather data. It is built using a modern data stack and follows the **Medallion Architecture** (Bronze ➔ Silver ➔ Gold).

## 🏗️ Architecture & Tech Stack
* **Infrastructure:** Docker Compose (Kafka, Zookeeper, MinIO)
* **Ingestion (Streaming):** Python 3.13, `kafka-python-ng`, RTE Open API
* **Processing (Batch/Micro-batch):** Apache Spark (PySpark)
* **Transformation & Modeling:** dbt (Data Build Tool)
* **Storage:** MinIO (S3-compatible Data Lake)
* **Visualization:** Looker Studio (connected to the Gold layer)

## ⚙️ Prerequisites
1. **Docker & Docker Compose:** Ensure Docker is running on your system with sufficient storage space allocated.
2. **Python 3.13:** A virtual environment (`.venv`) is recommended.
3. **Environment Variables:** You must have a `.env` file in the root directory containing:
    * RTE API credentials (Client ID / Secret)
    * MinIO storage access keys

## 🚀 How to Launch the Pipeline

### Step 1: Start the Infrastructure
Bring up the local Kafka cluster (with Zookeeper) and the MinIO storage layer using Docker Compose.
```bash
docker compose up -d
```

*Wait ~20 seconds for the brokers to fully initialize.*

### Step 2: Prepare the Python Environment

Activate your virtual environment and install dependencies.

**Bash**

```
source .venv/bin/activate
pip install -r requirements.txt
```

### Step 3: Start the Ingestion Layer (Continuous Streaming)

The ingestion layer requires continuous running processes. Open **three separate terminal windows/tabs** (or use a multiplexer like Tilix), ensure the `.venv` is active in each, and run:

**Terminal 1:** Start the RTE data producer

**Bash**

```
python3 src/ingestion/producer_rte.py
```

**Terminal 2:** Start the Weather data producer

**Bash**

```
python3 src/ingestion/producer_weather.py
```

**Terminal 3:** Start the Bronze consumer (Reads from Kafka, saves raw JSON to MinIO)

**Bash**

```
python3 src/ingestion/consumer_bronze.py
```

*(Leave these running in the background to continuously populate the Data Lake).*

### Step 4: Run the Processing Layer (Bronze ➔ Silver)

Once raw data is landing in the Bronze layer, process it into structured Delta tables. In a  **new terminal** , run the PySpark scripts sequentially:

**Bash**

```
# Clean and structure individual data streams
python3 src/processing/bronze_to_silver_consumption.py
python3 src/processing/bronze_to_silver_generation.py
python3 src/processing/bronze_to_silver_weather.py

# Join the streams into a unified Silver dataset
python3 src/processing/silver_join.py
```

### Step 5: Run the Transformation Layer (Silver ➔ Gold)

Generate the final analytical models using dbt.

**Bash**

```
cd src/dbt/energy_platform
dbt run
```

---

### 🗄️ Optional: Historical Backfill

To populate the data lake with historical data instead of waiting for the real-time streams to accumulate, run this script before executing Steps 4 and 5:

**Bash**

```
python3 src/ingestion/backfill_historical.py
```

---

### 📊 Visualization

The final processed data in the Gold layer is structured and optimized for BI consumption. You can seamlessly connect this output to Looker Studio to build interactive dashboards monitoring real-time energy consumption, generation, and weather impacts.

```

Would you like to start planning out the specific charts and button sequences you want to build for that Looker Studio dashboard next?
```
