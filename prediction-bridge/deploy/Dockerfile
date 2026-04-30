# ---- runtime (based on pre-built base image with dependencies) ----
# Build base image first:  docker build -f deploy/Dockerfile.base -t prediction-bridge-base:latest .
FROM prediction-bridge-base:latest

WORKDIR /app
COPY app ./app
COPY config ./config

RUN chown -R prediction-bridge:prediction-bridge /app
USER prediction-bridge

EXPOSE 8042
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://127.0.0.1:8042/health', timeout=3).raise_for_status()" || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8042"]
