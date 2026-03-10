# South Florida Tee Times — Python + Chrome for Selenium scrapers
# For free hosting (e.g. Render): same app and UI, all scrapers work.

FROM python:3.11-slim-bookworm

# Install Google Chrome (required for Chronogolf, TeeItUp, Club Caddie, Eagle Club, GolfNow)
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget gnupg ca-certificates \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux-signing-key.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-key.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y --no-install-recommends google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/google-chrome-stable
ENV PYTHONUNBUFFERED=1
# Render and most hosts set PORT at runtime; default for local Docker
ENV PORT=5000
ENV HOST=0.0.0.0

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY golf_server.py golf_ui.html ./

EXPOSE 5000
CMD ["python", "golf_server.py"]
