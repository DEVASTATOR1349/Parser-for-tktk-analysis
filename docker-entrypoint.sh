#!/bin/bash
set -e

echo "=== Парсер подписчиков ==="
echo "Дата: $(date -u)"
echo "Ручной режим — cron отключён"
echo ""

# Держим контейнер живым
touch /app/logs/cron.log
tail -f /app/logs/cron.log
