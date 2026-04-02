FROM nvcr.io/nvidia/pytorch:25.02-py3

WORKDIR /app

# Install dependencies (torch/torchvision already in base image)
COPY requirements.txt .
RUN pip install --no-cache-dir --timeout 300 --retries 5 -r requirements.txt

# Models via volume mount at /models (see docker-compose.yml)
# - /models/ViT-L-14.pt
# - /models/marqo-fashionsiglip/

COPY app/ app/

EXPOSE 8090

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8090/healthz || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8090", "--timeout-keep-alive", "120", "--log-level", "info"]
