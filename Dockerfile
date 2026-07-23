FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY config ./config
RUN python -m pip install --no-cache-dir .

RUN addgroup --system gateway \
    && adduser --system --ingroup gateway --home /app gateway \
    && mkdir -p /app/data \
    && chown -R gateway:gateway /app/data

USER gateway

VOLUME ["/app/data"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 \
  CMD ["python", "-m", "polymarket_btc.data_collection.market_data.cli", "status", "--health-file", "/app/data/runtime/health.json"]

CMD ["python", "-m", "polymarket_btc.data_collection.market_data.cli", "run", "--config", "/app/config/market_data.toml"]
