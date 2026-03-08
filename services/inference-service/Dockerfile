FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-downloaded wheel files and requirements
COPY torch-*.whl torchvision-*.whl /tmp/wheels/
COPY requirements.txt .

# Install PyTorch from local wheels, then the rest
RUN pip install --no-cache-dir /tmp/wheels/*.whl \
    && pip install --no-cache-dir --timeout 300 --retries 5 -r requirements.txt \
    && rm -rf /tmp/wheels

# Copy CLIP model
COPY ViT-B-32.pt /models/ViT-B-32.pt

COPY app/ app/

EXPOSE 8090

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8090/healthz || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8090", "--timeout-keep-alive", "120", "--log-level", "info"]
