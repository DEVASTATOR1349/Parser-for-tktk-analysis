FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/config

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    GOOGLE_APPLICATION_CREDENTIALS=/app/config/service_account.json \
    CLIENTS_CONFIG=/app/config/clients.yaml \
    SCOUT_EXTRA_ENV_FILE=
