FROM python:3.11-slim

RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy dashboard source + gold data
COPY src/dashboard/ ./src/dashboard/
COPY src/data/      ./src/data/

RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8501

ENV API_URL=http://api:8000
ENV PYTHONPATH=/app

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Run from the dashboard directory so relative paths work
CMD ["streamlit", "run", "src/dashboard/app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
