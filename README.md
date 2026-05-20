# Project Content Parser Handoff

Минимальный пакет алгоритма парсера клиентских аккаунтов.

## Назначение

Скрипт читает лист **«Админка проекта»** у клиента, парсит аккаунты/ролики по платформам и добавляет новые видео со статистикой в лист **«База Данных видео по проекту»**.

## Состав

- `workers/project_content_daily_worker.py` — точка запуска синка.
- `services/project_content_pipeline.py` — основная логика парсинга, дедупликации и записи.
- `examples/clients.example.yaml` — пример конфига клиента без реальных данных.
- `examples/env.example` — пример переменных окружения без ключей.

## Запуск

```bash
python3 workers/project_content_daily_worker.py --once
```

Один клиент:

```bash
python3 workers/project_content_daily_worker.py --once --client demo_client
```

## Что нужно настроить

1. Google service account / OAuth доступ к таблицам.
2. YouTube Data API key для YouTube.
3. Apify token для Instagram, если используется Instagram-парсинг.
4. `clients.yaml` по примеру из `examples/clients.example.yaml`.

## Важно

В репозитории нет:

- реальных client spreadsheet IDs;
- API-ключей;
- баз данных;
- дампов таблиц;
- приватных runtime-файлов.
