# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install minimal system deps (duckdb works without build-essential; keep it if you later compile extras)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY app ./app

# (Optional) Pre-fetch a DuckDB extension at build time to warm cache (not required for csv/parquet)
# RUN python - <<'PY'
# import duckdb
# con = duckdb.connect('dummy.db')
# con.execute('INSTALL httpfs;')
# PY

# Export unbuffered logs by default
ENV PYTHONUNBUFFERED=1

# Start API (worker uses same image with different command in docker-compose)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
