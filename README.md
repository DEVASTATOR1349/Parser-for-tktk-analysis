# apify-parser
# Парсер подписчиков через Apify → Google Sheets

## Установка

```bash
cd /path/to/apify-parser
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Отредактируй .env — вставь токены
```

## Запуск

```bash
# Разовый прогон
source venv/bin/activate
python src/main.py

# Или через cron (см. crontab.example)
crontab crontab.example
```

## Логи

Логи пишутся в `logs/parser_YYYY-MM-DD.log`. Авто-ротация раз в 30 дней.

## Как это работает

1. Читает Google Sheets — список проектов и ссылки на соцсети
2. Для каждой ссылки определяет платформу
3. Через Apify API запускает нужный актор
4. Ждёт результат, извлекает количество подписчиков
5. **Одним POST-запросом** пишет в листы "Статистика (raw)" и "Статистика"
6. Ошибки пишет в лист "Ошибки" (тоже одним батчем)

## Поддерживаемые платформы

| Платформа | Apify актор | Статус |
|---|---|---|
| Instagram | apify/instagram-profile-scraper | ✅ |
| YouTube | streamers/youtube-scraper | ✅ |
| TikTok | clockworks/tiktok-profile-scraper | ✅ |
| Facebook | apify/facebook-pages-scraper | ✅ |
| Pinterest | easyapi/pinterest-profile-scraper | ✅ |
| Дзен | apify/puppeteer-scraper | 🧪 тестовый |
| VK | — | ❌ |
| Telegram | — | ❌ |
| OK | — | ❌ |
| Rutube | — | ❌ |
| Twitter/X | — | ❌ |
| Snapchat | — | ❌ |
| Likee | — | ❌ |

## Лимиты

- Макс 2 повторных запроса при ошибке
- Задержка 1.5с между запросами
```
