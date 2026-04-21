FROM python:3.11-slim

# Non-root user for security
RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir lightgbm

# Copy only what the API needs
COPY src/api/   ./src/api/
COPY src/ml/    ./src/ml/
COPY src/data/  ./src/data/

# Create empty __init__.py so imports work
RUN touch src/__init__.py src/api/__init__.py src/ml/__init__.py

RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

ENV MLFLOW_TRACKING_URI=http://mlflow:5000
ENV PYTHONPATH=/app

CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
