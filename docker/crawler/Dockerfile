FROM python:3.11-slim

# 安装 Playwright 和 Chromium 依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    libatk1.0-0 libatk-bridge2.0-0 libcups2 libxdamage1 libxrandr2 \
    libgbm1 libpango-1.0-0 libcairo2 libasound2 libnspr4 libnss3 \
    libxcomposite1 libxfixes3 libxkbcommon0 libx11-6 libxext6 libxcb1 \
    fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖
RUN pip install --no-cache-dir \
    playwright==1.58.0 \
    aiohttp \
    Pillow \
    beautifulsoup4 \
    lxml

# 安装 Chromium
RUN python -m playwright install chromium

# 工作目录
WORKDIR /app
COPY scripts/ /app/scripts/

# 输出目录
VOLUME ["/data"]

# 默认命令
CMD ["python", "/app/scripts/crawl_brand_products.py"]
