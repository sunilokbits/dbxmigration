# ── Stage 1: build pyodbc + databricks-sql-connector wheels ──────────────────
FROM python:3.11-slim AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ unixodbc-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip wheel --no-cache-dir --wheel-dir /tmp/wheels -r /tmp/requirements.txt gunicorn

# ── Stage 2: lean runtime image ──────────────────────────────────────────────
FROM python:3.11-slim

# Install ODBC Driver 18 (required for source SQL Server connectivity via pyodbc)
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl gnupg2 ca-certificates && \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | \
        gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] \
https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 unixodbc libltdl7 && \
    apt-get purge -y --auto-remove curl gnupg2 && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps from pre-built wheels
COPY --from=builder /tmp/wheels /tmp/wheels
RUN pip install --no-cache-dir /tmp/wheels/*.whl && rm -rf /tmp/wheels

WORKDIR /app

COPY migration_utility/ ./migration_utility/
COPY src/ ./src/
COPY resources/ ./resources/

WORKDIR /app/migration_utility

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--threads", "8", "--timeout", "180", "app:app"]
