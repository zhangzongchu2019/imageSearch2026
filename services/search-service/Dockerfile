FROM python:3.11-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.11-slim

# 系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1-mesa-glx libglib2.0-0 curl && \
    rm -rf /var/lib/apt/lists/*

# Python 依赖
COPY --from=builder /install /usr/local

# 应用代码
WORKDIR /app
COPY app app/
COPY config/ config/
COPY models/ models/

# 非 root 用户
RUN useradd -m -r appuser && chown -R appuser:appuser /app
USER appuser

# 健康检查
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD curl -f http://localhost:8080/healthz || exit 1

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    IMGSRCH_ENV=production

EXPOSE 8080

CMD ["python", "-m", "uvicorn", "app.main:app", \
     "--host", "0.0.0.0", "--port", "8080", \
     "--workers", "4", "--loop", "uvloop", "--http", "httptools", \
     "--access-log", "--log-level", "warning"]
