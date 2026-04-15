# ─────────────────────────────────────────────────────────────
# Realtime Market Data Ingestion Server
# ─────────────────────────────────────────────────────────────
# Build context: project root (not realtime/ directory)
#
# Build:
#   docker build -f realtime/Dockerfile -t realtime-ingest .
#
# Run:
#   docker run --env-file realtime/.env realtime-ingest
# ─────────────────────────────────────────────────────────────

FROM python:3.12-slim

WORKDIR /app

# Install dependencies first (layer caching — only re-runs if requirements change)
COPY realtime/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy realtime server code
COPY realtime/ ./

# Copy shared data files (alias map used by the normalizer)
COPY data/coin_aliases.json /app/data/coin_aliases.json

# Tell config.py where the alias file is inside the container
ENV ALIAS_JSON_PATH=/app/data/coin_aliases.json

# Cloud Run injects PORT env var; default to 8080
ENV PORT=8080
EXPOSE 8080

CMD ["python", "main.py"]
