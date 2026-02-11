FROM python:3.11-slim AS builder
WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /install /usr/local
WORKDIR /app
COPY app/ app/
COPY config/ config/
RUN useradd -m -r appuser && chown -R appuser:appuser /app
USER appuser
HEALTHCHECK --interval=10s --timeout=3s CMD curl -f http://localhost:8081/docs || exit 1
ENV PYTHONUNBUFFERED=1 IMGSRCH_ENV=production
EXPOSE 8081
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8081", "--workers", "2", "--loop", "uvloop"]
