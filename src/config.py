"""Конфигурация парсера подписчиков."""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# === Apify ===
APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")
APIFY_API_TOKEN_BACKUP = os.getenv("APIFY_API_TOKEN_BACKUP")

# === Прямые API ===
VK_API_KEY = os.getenv("VK_API_KEY")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# === Прокси для Playwright (Facebook) ===
FB_PROXY = os.getenv("FB_PROXY", "")

# === Режим ===
TEST_MODE = os.getenv("TEST_MODE", "").lower() in ("1", "true", "yes")

# === Apps Script (Google Sheets bridge) ===
APPS_SCRIPT_URL = os.getenv("APPS_SCRIPT_URL")
# Таблица: читаем «ВсеИсточники» и пишем результаты в неё же
SHEET_ID = os.getenv("SHEET_ID", "10S1xijZ4ZNXVB4JQKyBylFmc7N_jwazHKSTc9pNj-t8")
APIFY_API_TOKEN_NEW = os.getenv("APIFY_API_TOKEN_NEW")
# Обратная совместимость
SOURCE_SHEET_ID = os.getenv("SOURCE_SHEET_ID", SHEET_ID)
TARGET_SHEET_ID = os.getenv("TARGET_SHEET_ID", SHEET_ID)

# === Парсер ===
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY_SECONDS", "1.5"))

# === Листы в Google Sheets ===
# Лист «ВсеИсточники»: 3 ключевые колонки (Клиент, Источник, Ссылка)
SHEET_LINKS_GID = 425357122
# Лист куда пишем результаты
SHEET_RESULTS_TAB = "ДанныеПарсинга"
# Лист для лога ошибок
SHEET_ERRORS_TAB = "Ошибки"

# === Маппинг доменов к Apify акторам ===
PLATFORM_ACTORS = {
    "instagram.com": {
        "actor": "apify/instagram-profile-scraper",
        "field": "followersCount",
    },
    "youtube.com": {
        "actor": "native",  # YouTube Data API v3
        "field": "statistics.subscriberCount",
    },
    "tiktok.com": {
        "actor": "clockworks/tiktok-profile-scraper",
        "field": "authorMeta.fans",  # Apify
    },
    "vk.com": {
        "actor": "native",  # VK API
        "field": "members_count",
    },
    "vk.ru": {
        "actor": "native",  # VK API (зеркало)
        "field": "members_count",
    },
    "facebook.com": {
        "actor": "apify/facebook-pages-scraper",  # 1 запрос, без логина для публичных страниц
        "field": "followers",
    },
    "ok.ru": {
        "actor": "native",  # парсинг HTML
        "field": "membersCount",
    },
    "dzen.ru": {
        "actor": "native",  # Playwright (React SPA)
        "field": "subscribers",
    },
    "rutube.ru": {
        "actor": "native",  # /api/video/person/
        "field": "subscribers_count",
    },
    "t.me": {
        "actor": None,  # нет работающего Apify актора (все 404)
        "field": None,
    },
    "pinterest.com": {
        "actor": "native",  # Playwright (React SPA)
        "field": "follower_count",
    },
    "x.com": {
        "actor": None,  # нет работающего Apify актора
        "field": None,
    },
    "twitter.com": {
        "actor": None,  # нет работающего Apify актора
        "field": None,
    },
    "snapchat.com": {
        "actor": None,  # Snapchat пока не парсим
        "field": None,
    },
    "likee.video": {
        "actor": None,  # нет работающего Apify актора
        "field": None,
    },
}

# Маппинг названий из «ВсеИсточники» → ключ платформы
SOURCE_NAME_MAP = {
    "Instagram": "instagram.com",
    "Instagram1": "instagram.com",
    "Instagram2": "instagram.com",
    "Youtube": "youtube.com",
    "YouTube": "youtube.com",
    "Facebook": "facebook.com",
    "Tiktok": "tiktok.com",
    "TikTok": "tiktok.com",
    "VK": "vk.com",
    "VK.RU": "vk.ru",
    "Vk.ru": "vk.ru",
    "Telegram": "t.me",
    "Rutube": "rutube.ru",
    "Odnoklassniki": "ok.ru",
    "OK": "ok.ru",
    "Dzen": "dzen.ru",
    "Дзен": "dzen.ru",
    "Likee": "likee.video",
    "Pinterest": "pinterest.com",
    "SnapChat": "snapchat.com",
    "Snapchat": "snapchat.com",
    "Twiiter": "twitter.com",  # опечатка в источнике
    "Twitter": "twitter.com",
    "X": "x.com",
}

# Названия площадок для человекочитаемого вывода
PLATFORM_NAMES = {
    "instagram.com": "Instagram",
    "youtube.com": "YouTube",
    "tiktok.com": "TikTok",
    "vk.com": "VK",
    "vk.ru": "VK",
    "facebook.com": "Facebook",
    "ok.ru": "OK",
    "dzen.ru": "Дзен",
    "rutube.ru": "Rutube",
    "t.me": "Telegram",
    "pinterest.com": "Pinterest",
    "x.com": "Twitter",
    "twitter.com": "Twitter",
    "snapchat.com": "Snapchat",
    "likee.video": "Likee",
}
