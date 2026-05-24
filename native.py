"""Прямые API-запросы к платформам (без Apify)."""
from __future__ import annotations
import re
import json
import requests
from urllib.parse import urlparse
from loguru import logger

from config import (
    VK_API_KEY,
    YOUTUBE_API_KEY,
    FB_PROXY,
)


# ──────────────────────────────────────────────────
# VK
# ──────────────────────────────────────────────────
def vk_followers(url: str, client_name: str) -> int | None:
    """VK: группы через groups.getById, пользователи через users.get."""
    if not VK_API_KEY:
        logger.warning(f"[{client_name}] VK_API_KEY не задан")
        return None

    screen_name = _vk_extract_screen_name(url)
    if not screen_name:
        logger.warning(f"[{client_name}] Не удалось извлечь screen_name из VK: {url}")
        return None

    # Пробуем как группу
    try:
        r = requests.get("https://api.vk.com/method/groups.getById", params={
            "group_id": screen_name,
            "fields": "members_count",
            "access_token": VK_API_KEY,
            "v": "5.199",
        }, timeout=15)
        data = r.json()
        groups = data.get("response", {}).get("groups")
        if groups:
            count = groups[0].get("members_count")
            if count:
                return int(count)
    except Exception:
        pass

    # Пробуем как пользователя
    try:
        r = requests.get("https://api.vk.com/method/users.get", params={
            "user_ids": screen_name,
            "fields": "followers_count",
            "access_token": VK_API_KEY,
            "v": "5.199",
        }, timeout=15)
        data = r.json()
        users = data.get("response", [])
        if users:
            count = users[0].get("followers_count")
            if count is not None:
                return int(count)
    except Exception:
        pass

    return None


def _vk_extract_screen_name(url: str) -> str | None:
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    parts = path.split("/")
    for part in parts:
        if part and part not in ("public", "club", "id", "event"):
            return part.split("?")[0]
    if parts and parts[-1]:
        return parts[-1].split("?")[0]
    return None


# ──────────────────────────────────────────────────
# YouTube Data API v3
# ──────────────────────────────────────────────────
def youtube_subscribers(url: str, client_name: str) -> int | None:
    """YouTube через Google API v3 (бесплатная квота 10K юнитов/день)."""
    if not YOUTUBE_API_KEY:
        logger.warning(f"[{client_name}] YOUTUBE_API_KEY не задан")
        return None

    channel_id = _youtube_extract_channel_id(url)
    if not channel_id:
        logger.warning(f"[{client_name}] Не удалось извлечь channel_id из YouTube: {url}")
        return None

    try:
        r = requests.get("https://www.googleapis.com/youtube/v3/channels", params={
            "part": "statistics",
            "id": channel_id if channel_id.startswith("UC") else None,
            "forUsername": None if (channel_id.startswith("UC") or channel_id.startswith("@")) else channel_id,
            "forHandle": channel_id.lstrip("@") if channel_id.startswith("@") else None,
            "key": YOUTUBE_API_KEY,
        }, timeout=15)
        # Очищаем None-параметры
        from urllib.parse import urlencode
        params = {"part": "statistics", "key": YOUTUBE_API_KEY}
        if channel_id.startswith("UC"):
            params["id"] = channel_id
        elif channel_id.startswith("@"):
            params["forHandle"] = channel_id[1:]
        else:
            params["forUsername"] = channel_id

        r2 = requests.get("https://www.googleapis.com/youtube/v3/channels", params=params, timeout=15)
        data = r2.json()
        items = data.get("items", [])
        if items:
            count = items[0].get("statistics", {}).get("subscriberCount")
            if count:
                return int(count)

        # Если не нашли — пробуем через search
        if not items and not channel_id.startswith("UC"):
            logger.debug(f"[{client_name}] YouTube search for: {channel_id}")
            sr = requests.get("https://www.googleapis.com/youtube/v3/search", params={
                "part": "snippet",
                "q": channel_id,
                "type": "channel",
                "maxResults": 1,
                "key": YOUTUBE_API_KEY,
            }, timeout=15)
            sr_data = sr.json()
            sr_items = sr_data.get("items", [])
            if sr_items:
                real_id = sr_items[0].get("id", {}).get("channelId")
                if real_id:
                    sr2 = requests.get("https://www.googleapis.com/youtube/v3/channels", params={
                        "part": "statistics",
                        "id": real_id,
                        "key": YOUTUBE_API_KEY,
                    }, timeout=15)
                    data2 = sr2.json()
                    items2 = data2.get("items", [])
                    if items2:
                        count = items2[0].get("statistics", {}).get("subscriberCount")
                        if count:
                            return int(count)

    except Exception as e:
        logger.warning(f"[{client_name}] YouTube API error: {e}")

    return None


def _youtube_extract_channel_id(url: str) -> str | None:
    parsed = urlparse(url)
    # Берём полный путь (без strip)
    path = parsed.path.rstrip("/") or "/"

    # /channel/UCxxx
    m = re.search(r"/channel/([^/?]+)", path)
    if m:
        return m.group(1)

    # /c/name — нужно резолвить через поиск
    m = re.search(r"/c/([^/?]+)", path)
    if m:
        return m.group(1)

    # /@handle
    m = re.search(r"/@([^/?]+)", path)
    if m:
        return "@" + m.group(1)

    # /user/name (старый формат)
    m = re.search(r"/user/([^/?]+)", path)
    if m:
        return m.group(1)

    return None


# ──────────────────────────────────────────────────
# Rutube
# ──────────────────────────────────────────────────
def rutube_subscribers(url: str, client_name: str) -> int | None:
    """Rutube через скрытый API /api/video/person/"""
    channel_id = _rutube_extract_id(url)
    if not channel_id:
        logger.warning(f"[{client_name}] Не удалось извлечь ID из Rutube: {url}")
        return None

    try:
        r = requests.get(
            f"https://rutube.ru/api/profile/user/{channel_id}/",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            count = data.get("subscribers_count")
            if count is not None:
                return int(count)
        logger.debug(f"[{client_name}] Rutube ответ: keys={list(data.keys())[:15]}")
    except Exception as e:
        logger.warning(f"[{client_name}] Rutube error: {e}")

    return None


def _rutube_extract_id(url: str) -> str | None:
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") or "/"
    # /channel/73826838/ или /u/username/
    m = re.search(r"/(?:channel|u)/([^/?]+)", path)
    if m:
        return m.group(1)
    return None


# ──────────────────────────────────────────────────
# Дзен — Playwright
# ──────────────────────────────────────────────────
DZEN_CACHE: dict[str, int | None] = {}

def dzen_subscribers(url: str, client_name: str) -> int | None:
    """Дзен: Playwright → парсинг subscriber count из React SPA."""
    if url in DZEN_CACHE:
        return DZEN_CACHE[url]

    SyncPlaywright = _fb_playwright()

    try:
        with SyncPlaywright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-gpu',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--single-process',
                ]
            )
            ctx = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
            )
            page = ctx.new_page()
            page.goto(url, timeout=20000, wait_until='domcontentloaded')
            page.wait_for_timeout(4000)

            text = page.inner_text('body')
            text_clean = text.replace('\u00a0', ' ').replace('\n', ' ')

            # "Нет подписчиков" → 0
            if re.search(r'(?:Нет|нет|0)\s+(?:подписчик|читател|подписчиков|читателей)', text_clean, re.I):
                count = 0
                DZEN_CACHE[url] = count
                browser.close()
                return count

            # "X подписчиков" / "X читателей"
            m = re.search(r'(\d[\d\s,.]*(?:\d|тыс\.?|млн\.?)?)\s*(?:подписчик|читател|подписчиков|читателей|subscriber|follower)', text_clean, re.I)
            if m:
                count = _fb_parse_count(m.group(1).replace(' ', '').strip())
                DZEN_CACHE[url] = count
                browser.close()
                return count

            browser.close()

    except Exception as e:
        logger.warning(f"[{client_name}] Дзен Playwright error: {str(e)[:150]}")

    DZEN_CACHE[url] = None
    return None

# ──────────────────────────────────────────────────
# OK (Одноклассники)
# ──────────────────────────────────────────────────
def ok_subscribers(url: str, client_name: str) -> int | None:
    """OK: Playwright → парсинг membersCount/followersCount из React SPA."""
    SyncPlaywright = _fb_playwright()

    try:
        with SyncPlaywright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-gpu',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--single-process',
                ]
            )
            ctx = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
            )
            page = ctx.new_page()
            page.goto(url, timeout=20000, wait_until='domcontentloaded')
            page.wait_for_timeout(4000)

            text = page.inner_text('body')
            text_clean = text.replace('\u00a0', ' ').replace('\n', ' ')

            # "X участников" / "X подписчиков"
            m = re.search(r'(\d[\d\s,.]*(?:\d|тыс\.?|млн\.?)?)\s*(?:участник|подписчик|участников|подписчиков|subscriber|member)', text_clean, re.I)
            if m:
                count = _fb_parse_count(m.group(1).replace(' ', '').strip())
                browser.close()
                return count

            # Ищем в JSON/скриптах
            page_content = page.content()
            for pat in [
                r'"membersCount"\s*:\s*(\d+)',
                r'"members_count"\s*:\s*(\d+)',
                r'"friendsCount"\s*:\s*(\d+)',
                r'"followersCount"\s*:\s*(\d+)',
                r'"subscribers"\s*:\s*(\d+)',
            ]:
                m = re.search(pat, page_content)
                if m:
                    count = int(m.group(1))
                    browser.close()
                    return count

            browser.close()

    except Exception as e:
        logger.warning(f"[{client_name}] OK Playwright error: {str(e)[:150]}")

    return None

# ──────────────────────────────────────────────────
# Facebook (Playwright через российский прокси)
# ──────────────────────────────────────────────────
FB_CACHE: dict[str, int | None] = {}


def _fb_playwright():
    """Ленивый импорт Playwright (тяжёлый, только когда нужен)."""
    from playwright.sync_api import sync_playwright
    return sync_playwright


def _fb_parse_count(raw: str) -> int:
    """Парсит '1,3 тыс.' → 1300, '62 тыс.' → 62000, '3' → 3."""
    s = raw.replace('\u00a0', ' ').strip().rstrip('.')
    if 'тыс' in s or 'k' in s.lower():
        s = s.replace('тыс.', '').replace('тыс', '').replace('k', '').replace('K', '').strip()
        return int(float(s.replace(',', '.')) * 1000)
    if 'млн' in s or 'm' in s.lower():
        s = s.replace('млн', '').replace('m', '').replace('M', '').strip()
        return int(float(s.replace(',', '.')) * 1000000)
    return int(float(s.replace(',', '.')))


def facebook_followers(url: str, client_name: str) -> int | None:
    """Facebook: Playwright через российский прокси → парсинг HTML."""
    if url in FB_CACHE:
        return FB_CACHE[url]

    if not FB_PROXY:
        logger.warning(f"[{client_name}] FB_PROXY не задан, пропускаю Facebook")
        return None

    # Чистим URL от мусора
    clean_url = url.split('&mibextid')[0].split('?mibextid')[0].split('&sk=')[0]
    clean_url = clean_url.split('&rdid=')[0].split('&share_url=')[0].rstrip('?')

    # Парсим прокси-строку
    # Формат: http://user:pass@host:port
    proxy_match = re.match(r'https?://([^:]+):([^@]+)@([^:]+):(\d+)', FB_PROXY)
    if not proxy_match:
        logger.error(f"[{client_name}] FB_PROXY в неверном формате: нужен http://user:pass@host:port")
        return None
    proxy_user, proxy_pass, proxy_host, proxy_port = proxy_match.groups()

    SyncPlaywright = _fb_playwright()

    try:
        with SyncPlaywright() as p:
            browser = p.chromium.launch(
                headless=True,
                proxy={
                    'server': f'http://{proxy_host}:{proxy_port}',
                    'username': proxy_user,
                    'password': proxy_pass,
                },
                args=['--disable-blink-features=AutomationControlled']
            )
            ctx = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
            )
            page = ctx.new_page()
            page.goto(clean_url, timeout=20000, wait_until='domcontentloaded')
            page.wait_for_timeout(4000)
            text = page.inner_text('body')
            browser.close()

            # Логин-волл
            if 'Этот контент сейчас недоступен' in text or 'Выполните вход' in text:
                logger.info(f"[{client_name}] Facebook: приватный профиль")
                FB_CACHE[url] = None
                return None

            # Парсим: «число( тыс.)? [—–-] подписчик»
            text_clean = text.replace('\u00a0', ' ')
            m = re.search(r'([\d ,]+(?:\s*тыс\.?)?)\s*(?:[—–-]\s*)?подписчик', text_clean, re.I)
            if m:
                count = _fb_parse_count(m.group(1))
                FB_CACHE[url] = count
                return count

            logger.debug(f"[{client_name}] Facebook: нет подписчиков в тексте (title={page})")
            FB_CACHE[url] = None
            return None

    except Exception as e:
        logger.warning(f"[{client_name}] Playwright error: {str(e)[:150]}")
        FB_CACHE[url] = None
        return None


# ──────────────────────────────────────────────────
# Pinterest — Playwright
# ──────────────────────────────────────────────────
PIN_CACHE: dict[str, int | None] = {}

def pinterest_followers(url: str, client_name: str) -> int | None:
    """Pinterest: Playwright → парсинг follower_count из React SPA профиля."""
    if url in PIN_CACHE:
        return PIN_CACHE[url]

    SyncPlaywright = _fb_playwright()

    try:
        with SyncPlaywright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-gpu',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--single-process',
                ]
            )
            ctx = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
            )
            page = ctx.new_page()
            page.goto(url, timeout=20000, wait_until='domcontentloaded')
            page.wait_for_timeout(4000)

            text = page.inner_text('body')
            text_clean = text.replace('\u00a0', ' ')

            # English: "X followers" / Russian: "X подписчик"
            m = re.search(r'([\d,]+(?:[.,]\d+)?\s*(?:k|K|m|M|тыс\.?|млн\.?)?)\s*(?:followers|подписчик|подписчиков|follower)', text_clean, re.I)
            if m:
                count = _fb_parse_count(m.group(1).replace(',', '').strip())
                PIN_CACHE[url] = count
                browser.close()
                return count

            # Ищем во всех script/json блоках follower_count
            page_content = page.content()
            for pat in [
                r'"follower_count"\s*:\s*(\d+)',
                r'"followerCount"\s*:\s*(\d+)',
                r'"followersCount"\s*:\s*(\d+)',
                r'"totalFollowers"\s*:\s*(\d+)',
            ]:
                m = re.search(pat, page_content)
                if m:
                    count = int(m.group(1))
                    PIN_CACHE[url] = count
                    browser.close()
                    return count

            browser.close()

    except Exception as e:
        logger.warning(f"[{client_name}] Pinterest Playwright error: {str(e)[:150]}")

    PIN_CACHE[url] = None
    return None

def _get_nested_or(obj: dict, key: str):
    """Достаёт значение из вложенного словаря."""
    if isinstance(obj, dict):
        v = obj.get(key)
        if v is not None:
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        uv = item.get("userInteractionCount")
                        if uv is not None:
                            return uv
            return v
    return None
