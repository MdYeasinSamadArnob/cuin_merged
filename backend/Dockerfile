# ============================================
# CUIN v2 Backend - Dockerfile
# ============================================
# Multi-stage build for production optimization
# ============================================

# Stage 1: Builder
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# requirements-docker.txt is requirements.txt with pyspark/splink/py4j/
# scikit-learn/scipy/igraph/altair/oracledb/Faker removed -- confirmed
# unused by anything the container actually runs (the live API and the
# DuckDB pipeline never import them; only pipeline.spark_orchestrator's
# legacy/rollback engine does, and that import is now lazy -- see
# api/routes_datasource.py -- specifically so the app boots fine without
# them). This cuts build time and image size substantially since pyspark
# alone bundles a full Spark distribution. Regenerate this file from
# requirements.txt with the same exclusion list if dependencies change;
# see the comment at the top of requirements-docker.txt.
COPY requirements-docker.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements-docker.txt

# Stage 2: Runtime
FROM python:3.11-slim

WORKDIR /app

# libpq5: Postgres client library (psycopg2-binary still links against it
#   at runtime on some platforms). curl: healthcheck.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application code
COPY . .

# data/, data_source/, and policies/ are mounted as volumes at runtime
# (see infra/docker-compose.yml) -- runtime state and the source dataset
# are not baked into the image. Create the mount points now so they exist
# with the right ownership before the volume is attached.
RUN mkdir -p data/runs data_source && \
    useradd --create-home --shell /bin/bash cuin && \
    chown -R cuin:cuin /app
USER cuin

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
# Conservative defaults for a container sharing the host with other
# services -- see pipeline/duckdb_orchestrator.py's _connect(). Override
# via docker-compose environment for a dedicated host.
ENV DUCKDB_MEMORY_LIMIT=4GB
ENV DUCKDB_THREADS=4

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
