# Parser-for-tktk-analysis

Парсер для мониторинга контента подписчиков/проектов по площадкам (Instagram, YouTube, TikTok, Facebook, VK, Rutube и др.).

---

## Архитектура

Проект имеет **две стабильные версии**, разделённые по веткам:

| Версия | Ветка | Instagram Apify | Google Sheets | Риск кредитов Apify |
|--------|-------|----------------|---------------|---------------------|
| **v1** | `master` | 1 run на всех (fast) | 1 batchGet + 1 batchUpdate | Высокий — все 30 аккаунтов в 1 run |
| **v2** | `v2` | Батчи по `--max-apify-runs 5` | 1 batchGet + 1 batchUpdate | Контролируемый — лимит запусков за цикл |

### v1 (master)
- **Один Apify run** на всю платформу (Instagram, Facebook, TikTok).
- Быстро, но дорого: если у 30 клиентов 14 аккаунтов Instagram — 1 run сразу по всем.
- Использует `fetch_per_account=1` (1 пост на профиль), минимальная цена ~$0.056.
- `INSTAGRAM_MAX_APIFY_RUNS_PER_CYCLE = 1`
- `FETCH_PER_ACCOUNT = 1` — только 1 пост на профиль для детекции новых видео.

**Когда выбирать v1:**
- Мало клиентов (1-5).
- Важна скорость, лимит Apify кредитов не критичен.
- Тестовые/демо-прогоны.

### v2 (v2 — рекомендовано)
- Аккаунты делятся на **батчи**, каждый батч = 1 Apify run.
- Максимум `--max-apify-runs 5` Apify запусков за цикл.
- Если аккаунтов 20, они делятся на 5 батчей по 4 аккаунта, а не на 20 отдельных запусков.
- Контролируемый расход Apify-кредитов.
- `INSTAGRAM_MAX_APIFY_RUNS_PER_CYCLE = 5`

**Когда выбирать v2:**
- Много клиентов (10+).
- Нужно контролировать бюджет Apify.
- Регулярные ежедневные прогоны.

---

## Структура

```
Parser-for-tktk-analysis/
├── services/
│   └── project_content_pipeline.py    # Основная логика парсинга
│       ├── fetch_instagram_accounts_batch()  # Apify Instagram
│       ├── fetch_facebook_accounts_batch()   # Apify Facebook
│       ├── fetch_tiktok_accounts_batch()     # Apify TikTok (отключён)
│       ├── build_instagram_apify_batches()   # Делит аккаунты на батчи
│       ├── batched_sync_client()             # 1 batchGet + 1 batchUpdate
│       └── ... helper functions
├── workers/
│   ├── project_content_daily_worker.py    # Ежедневный воркер
│   └── common.py                          # Общие утилиты
├── config/
│   └── service_account.json               # Google Service Account
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

---

## Площадки

| Площадка | Движок | Apify | Ограничения |
|----------|--------|-------|-------------|
| **Instagram** | `apify~instagram-reel-scraper` | ✅ | `--max-apify-runs 5/6` |
| **YouTube** | YouTube Data API v3 | ❌ (бесплатно) | `SCOUT_PROJECT_YOUTUBE_RESULTS_LIMIT=250` |
| **Facebook** | `apify~facebook-pages-scraper` | ✅ | `--max-apify-runs 5/6` |
| **TikTok** | `clockworks~tiktok-profile-scraper` | ✅ | **Отключён** (в разработке) |
| **VK** | VK API | ❌ (бесплатно) | `SCOUT_PROJECT_VK_RESULTS_LIMIT=500` |
| **Rutube** | Rutube API | ❌ (бесплатно) | `SCOUT_PROJECT_RUTUBE_RESULTS_LIMIT=500` |
| **Dzen / OK / Pinterest / Likee** | — | ❌ | Отключены |

---

## Модель данных

### Лист «Админка проекта»
- **B2**: главный тумблер (`TRUE` = парсер активен, `FALSE` = пропустить)
- **A2:A200**: ссылки на площадки
- **C:H**: обновляемые поля (подписчики, видео, дата чека, статус)
- **I**: дата сегодняшнего чека
- **K:N**: разбивка по Instagram/YouTube

### Лист «База Данных видео по проекту»
- Колонки A–S: дата публикации, ссылка площадка, ссылка видео, описание, комментарии, лайки, просмотры, хэштеги и т.д.
- **C:D**: ключи для дедупликации (платформа + URL видео)

### Лист «История проекта по дням»
- A–I: ежедневный снимок (подписчики, видео, статус по каждой площадке)

---

## Экономия Apify

### Для Instagram
- Актор: `apify~instagram-reel-scraper` — тянет **только рилсы**, без фото/sidecar
- Выключены дорогие опции:
  - `includeTranscript: False`
  - `includeSharesCount: False`
  - `includeDownloadedVideo: False`
  - `skipPinnedPosts: True`
- `resultsLimit` = количество аккаунтов в батче × 1 (fetch_per_account=1)

### Для Facebook
- Актор: `apify~facebook-pages-scraper`
- `resultsLimit: 1` (1 пост на страницу)

---

## Запуск

### С Docker

```bash
cd /tmp/Parser-for-tktk-analysis

# Все клиенты (v1 — master)
docker compose run --rm daily-worker --once

# Все клиенты (v2 — v2, c батчингом)
docker compose run --rm daily-worker --once --max-apify-runs 5

# Один клиент (v2)
docker compose run --rm daily-worker --once --client dip-sound --max-apify-runs 5

# Тест (3 поста на аккаунт)
docker compose run --rm daily-worker --once --test-limit 3 --max-apify-runs 5

# Тест одной площадки (v2)
docker compose run --rm daily-worker --once --platform instagram --max-apify-runs 5
```

### Без Docker

```bash
cd /tmp/Parser-for-tktk-analysis
python3 workers/project_content_daily_worker.py --once --max-apify-runs 5
```

### Аргументы

| Аргумент | По умолчанию | Описание |
|----------|-------------|----------|
| `--once` | — | Один цикл синхронизации |
| `--client KEY` | все | Только один клиент (ключ из clients.yaml) |
| `--max-apify-runs N` | 5 | Макс. Apify запусков на платформу (v2) |
| `--test-limit N` | 0 | Лимит постов на аккаунт (для теста) |
| `--platform NAME` | все | Фильтр по площадке |

---

## Переменные окружения (.env)

```bash
# Apify
APIFY_TOKEN=apify_api_***

# Google Sheets
CLIENTS_SHEET_ID=1E4TmhKLulzI9y7ag3yJJPIt8LmL_DGm1kzjuOFgOxMM
GOOGLE_APPLICATION_CREDENTIALS=config/service_account.json

# Instagram
SCOUT_PROJECT_INSTAGRAM_RESULTS_LIMIT=250
SCOUT_PROJECT_MAX_APIFY_RUNS_PER_CYCLE=5     # v2

# YouTube
SCOUT_PROJECT_YOUTUBE_RESULTS_LIMIT=250
YOUTUBE_API_KEY=AIzaSy***

# VK
VK_ACCESS_TOKEN=vk1.a.***

# TikTok
SCOUT_PROJECT_TIKTOK_RESULTS_LIMIT=200

# Facebook
SCOUT_PROJECT_FACEBOOK_RESULTS_LIMIT=200

# Общие
SCOUT_PROJECT_APIFY_TIMEOUT=180
SCOUT_INTER_CLIENT_DELAY_SEC=3
```

---

## Миграция между версиями

### v1 → v2 (усиление контроля Apify)
```bash
git checkout v2
git merge master         # если нужны последние изменения из v1
```

### v2 → v1 (откат, если нужно быстрее)
```bash
git checkout master
```
