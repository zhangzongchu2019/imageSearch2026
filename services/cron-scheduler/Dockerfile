FROM python:3.11-slim
RUN pip install --no-cache-dir apscheduler asyncpg pymilvus structlog pyyaml
WORKDIR /app
COPY app/ app/
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "app.main"]
