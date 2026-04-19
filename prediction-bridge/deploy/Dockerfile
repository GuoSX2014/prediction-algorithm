# ---- builder ----
FROM python:3.11-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /build
COPY requirements.txt .
RUN pip install --prefix=/install -r requirements.txt

# ---- runtime ----
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PREDICTION_BRIDGE_CONFIG_PATH=/app/config/config.yaml

RUN useradd --system --uid 10001 --home-dir /app --shell /usr/sbin/nologin prediction-bridge \
 && mkdir -p /var/lib/prediction-bridge/downloads /var/lib/prediction-bridge/reports /var/log/prediction-bridge \
 && chown -R prediction-bridge:prediction-bridge /var/lib/prediction-bridge /var/log/prediction-bridge

WORKDIR /app
COPY --from=builder /install /usr/local
COPY app ./app
COPY config ./config

RUN chown -R prediction-bridge:prediction-bridge /app
USER prediction-bridge

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://127.0.0.1:8080/health', timeout=3).raise_for_status()" || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
