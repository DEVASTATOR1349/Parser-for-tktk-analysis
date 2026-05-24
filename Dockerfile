FROM python:3.11-slim

WORKDIR /app

# Системные зависимости + браузерные библиотеки для Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    libnss3 libnspr4 libatk-bridge2.0-0 libatk1.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 libpango-1.0-0 libcairo2 libasound2t64 \
    && rm -rf /var/lib/apt/lists/*

# Копируем зависимости и устанавливаем
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем Chromium для Playwright (только браузер, без системных deps — они уже установлены)
RUN playwright install chromium

# Копируем исходники
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY .env ./

# Сервисный ключ Google Sheets API
COPY eva-bot-api-key.json /app/eva-bot-api-key.json

# Папка для логов
RUN mkdir -p /app/logs

# Настройка cron (запуск каждый день в 5:00 UTC = 8:00 МСК)
RUN echo "0 5 * * * cd /app && /usr/local/bin/python src/main.py >> /app/logs/cron.log 2>&1" > /etc/cron.d/parser-cron
RUN chmod 0644 /etc/cron.d/parser-cron
RUN crontab /etc/cron.d/parser-cron

# Запуск cron и разовый прогон при старте (для теста)
COPY docker-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
