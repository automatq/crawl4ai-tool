FROM python:3.12-slim

# Install ALL Chromium/Playwright dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget gnupg ca-certificates \
    # X11 / display libs
    libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxcursor1 \
    libxdamage1 libxext6 libxfixes3 libxi6 libxrandr2 libxrender1 \
    libxss1 libxtst6 libxshmfence1 libxkbcommon0 \
    # GTK / rendering
    libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libgbm1 \
    libgtk-3-0 libpango-1.0-0 libpangocairo-1.0-0 libcairo2 \
    # Media / audio
    libasound2 libdbus-1-3 libnss3 libnspr4 \
    # Fonts
    fonts-liberation fonts-noto-color-emoji \
    # Utils
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Chromium + all its deps via Playwright
RUN python -m playwright install --with-deps chromium

# Copy app
COPY . .

# Railway sets PORT env var
ENV PORT=5000
EXPOSE 5000

# Use gunicorn for production stability
# 1 worker, 8 threads, 600s timeout for long scrape jobs
CMD gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 600 web:app
