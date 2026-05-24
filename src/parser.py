"""Парсинг подписчиков: Apify + нативные API."""

from __future__ import annotations

import time
from urllib.parse import urlparse

from apify_client import ApifyClient
from loguru import logger

from config import (
    APIFY_API_TOKEN,
    APIFY_API_TOKEN_BACKUP,
    APIFY_API_TOKEN_NEW,
    MAX_RETRIES,
    PLATFORM_ACTORS,
    PLATFORM_NAMES,
    REQUEST_DELAY,
    TEST_MODE,
)
from native import (
    vk_followers,
    youtube_subscribers,
    rutube_subscribers,
    dzen_subscribers,
    ok_subscribers,
    pinterest_followers,
    facebook_followers,
)


def _detect_platform(url: str) -> str | None:
    """Определяет платформу по URL."""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    if ":" in domain:
        domain = domain.split(":")[0]
    if domain in PLATFORM_ACTORS:
        return domain
    # vk.ru → VK (зеркало)
    if domain == "vk.ru":
        return "vk.ru"
    if domain == "pin.it":
        return "pinterest.com"
    for key in PLATFORM_ACTORS:
        if key in domain:
            return key
    if "t.me" in domain or "telegram.org" in domain:
        return "t.me"
    return None


def get_platform_name(url: str) -> str:
    platform_key = _detect_platform(url)
    return PLATFORM_NAMES.get(platform_key, url)


def _get_nested(obj: dict, path: str):
    parts = path.split(".")
    current = obj
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _find_followers_in_item(item: dict, field_name: str) -> int | None:
    val = _get_nested(item, field_name)
    if val is not None:
        return int(val)
    for variant in [
        "followersCount", "subscribersCount", "subscriberCount", "followerCount",
        "fansCount", "totalFollowers", "membersCount", "totalMembers",
        "numberOfSubscribers", "subscriber_count",
    ]:
        v = item.get(variant)
        if v is not None:
            return int(v)
    author = item.get("authorMeta", {})
    if isinstance(author, dict):
        for f in ("fans", "followers", "followerCount", "totalFollowers"):
            v = author.get(f)
            if v is not None:
                return int(v)
    stats = item.get("channelStats", {})
    if isinstance(stats, dict):
        v = stats.get("subscriberCount") or stats.get("totalSubscribers")
        if v is not None:
            return int(v)
    return None


# ──────────────────────────────────────────────────
# Диспетчер: нативные API + Apify
# ──────────────────────────────────────────────────
_NATIVE_HANDLERS = {
    "vk.com": vk_followers,
    "vk.ru": vk_followers,  # зеркало VK
    "youtube.com": youtube_subscribers,
    "rutube.ru": rutube_subscribers,
    "ok.ru": ok_subscribers,
    "pinterest.com": pinterest_followers,
    "dzen.ru": dzen_subscribers,
    "facebook.com": facebook_followers,
}


def fetch_followers(url: str, client_name: str) -> int | None:
    platform_key = _detect_platform(url)
    if not platform_key:
        logger.warning(f"[{client_name}] Неизвестная платформа: {url}")
        return None

    platform_config = PLATFORM_ACTORS.get(platform_key)
    if not platform_config:
        logger.info(f"[{client_name}] Нет конфига: {platform_key}")
        return None

    actor_name = platform_config.get("actor")
    field_name = platform_config.get("field")

    # ── Нативный API ──
    if actor_name == "native":
        handler = _NATIVE_HANDLERS.get(platform_key)
        if handler:
            try:
                result = handler(url, client_name)
                if result is not None:
                    logger.info(f"[{client_name}] {platform_key}: {result:,} подписчиков")
                    return result
            except Exception as e:
                logger.warning(f"[{client_name}] native {platform_key} error: {e}")
        logger.warning(f"[{client_name}] native {platform_key}: не удалось получить подписчиков")
        return None

    if not actor_name:
        logger.info(f"[{client_name}] Нет парсера: {platform_key}")
        return None

    # ── Apify ──
    return _fetch_via_apify(actor_name, field_name, url, platform_key, client_name)


def _fetch_via_apify(actor_name: str, field_name: str | None,
                     url: str, platform_key: str, client_name: str) -> int | None:
    run_input = _build_run_input(actor_name, url, client_name)
    if run_input is None:
        return None

    tokens_to_try = [APIFY_API_TOKEN]
    if APIFY_API_TOKEN_BACKUP:
        tokens_to_try.append(APIFY_API_TOKEN_BACKUP)
    if APIFY_API_TOKEN_NEW:
        tokens_to_try.append(APIFY_API_TOKEN_NEW)

    last_error = None
    for token_index, token in enumerate(tokens_to_try):
        if token_index > 0:
            logger.info(f"[{client_name}] Переключаюсь на резервный Apify-токен...")
        client = ApifyClient(token=token)

        for attempt in range(1, MAX_RETRIES + 2):
            try:
                if attempt > 1:
                    logger.info(f"[{client_name}] Попытка #{attempt} для {platform_key}")
                    time.sleep(REQUEST_DELAY * attempt)

                run = client.actor(actor_name).call(run_input=run_input)
                dataset = client.dataset(run["defaultDatasetId"])
                items = dataset.list_items().items

                if not items:
                    logger.warning(f"[{client_name}] Актор {actor_name} не вернул данных")
                    return None

                result = _find_followers_in_item(items[0], field_name or "")
                if result is not None:
                    logger.info(f"[{client_name}] {platform_key}: {result:,} подписчиков")
                    return result

                logger.debug(f"[{client_name}] {actor_name} keys={list(items[0].keys())[:20]}")
                logger.warning(f"[{client_name}] Нет поля {field_name} в ответе {actor_name}")
                return None

            except Exception as e:
                last_error = e
                err_msg = str(e)
                logger.warning(f"[{client_name}] Ошибка #{attempt}: {err_msg[:120]}")
                if "limit exceeded" in err_msg.lower():
                    break
                if attempt <= MAX_RETRIES:
                    continue

    logger.error(f"[{client_name}] Все попытки исчерпаны: {last_error}")
    return None


# ═══════════════════════════════════════════════
# Grouped (batch) fetching — 1 Apify запуск на платформу
# ═══════════════════════════════════════════════


def batch_fetch_all(
    platform_groups: dict[str, list[tuple[str, str]]],
) -> dict[str, int | None]:
    """Групповой парсинг: 1 запрос на платформу вместо N отдельных.

    Args:
        platform_groups: {platform_key: [(client_name, url), ...]}

    Returns: {url: followers_count | None}
    """
    results: dict[str, int | None] = {}

    for platform_key, items in platform_groups.items():
        platform_config = PLATFORM_ACTORS.get(platform_key)
        if not platform_config:
            for _, url in items:
                results[url] = None
            continue

        actor_name = platform_config.get("actor")
        field_name = platform_config.get("field")

        # ── Нет актора ──
        if actor_name is None:
            for _, url in items:
                results[url] = None
                logger.info(f"Нет парсера: {platform_key}")
            continue

        # ── Нативные API (дешёвые, по одному) ──
        if actor_name == "native":
            handler = _NATIVE_HANDLERS.get(platform_key)
            if handler:
                for name, url in items:
                    try:
                        count = handler(url, name)
                        results[url] = count
                        if count is not None:
                            logger.info(f"[{name}] {platform_key}: {count:,} подписчиков")
                        else:
                            logger.warning(f"[{name}] native {platform_key}: не удалось")
                    except Exception as e:
                        logger.warning(f"[{name}] native {platform_key} error: {e}")
                        results[url] = None
            else:
                for _, url in items:
                    results[url] = None
            continue

        # ── Facebook profile.php → Playwright (native) ──
        if actor_name == "apify/facebook-pages-scraper":
            # Разделяем: profile.php → native, остальное → Apify batch
            normal_urls = []
            php_urls = []
            for name, url in items:
                if "profile.php" in url:
                    php_urls.append((name, url))
                else:
                    normal_urls.append((name, url))

            # Native для profile.php
            handler = _NATIVE_HANDLERS.get("facebook.com")
            if handler and php_urls:
                for name, url in php_urls:
                    results[url] = _safe_native(handler, url, name)
            else:
                for _, url in php_urls:
                    results[url] = None

            # Apify batch для остальных
            if normal_urls:
                urls_only = [u for _, u in normal_urls]
                batch_results = _batch_run_apify(
                    actor_name, field_name, urls_only, platform_key
                )
                for name, url in normal_urls:
                    results[url] = _match_url_in_batch(url, batch_results, actor_name, field_name)
            continue

        # ── Остальные Apify акторы с batch-поддержкой ──
        urls_only = [u for _, u in items]

        # Пробуем batch
        run_input = _build_batch_run_input(actor_name, urls_only)
        if run_input is not None:
            batch_results = _batch_run_apify(
                actor_name, field_name, urls_only, platform_key, run_input=run_input
            )
            for _, url in items:
                results[url] = _match_url_in_batch(url, batch_results, actor_name, field_name)
        else:
            # fallback: по одному
            for name, url in items:
                results[url] = _fetch_via_apify(actor_name, field_name, url, platform_key, name)

    return results


def _batch_run_apify(
    actor_name: str,
    field_name: str | None,
    urls: list[str],
    platform_key: str,
    run_input: dict | None = None,
) -> list[dict]:
    """Запуск Apify актора с групповым input. Возвращает список items."""
    if run_input is None:
        run_input = _build_batch_run_input(actor_name, urls)
    if run_input is None:
        return []

    tokens = [t for t in [APIFY_API_TOKEN, APIFY_API_TOKEN_BACKUP, APIFY_API_TOKEN_NEW] if t]

    for token_index, token in enumerate(tokens):
        if token_index > 0:
            logger.info(f"[{platform_key}] Переключаюсь на резервный Apify-токен...")
        client = ApifyClient(token=token)

        last_err_msg = ""
        for attempt in range(1, MAX_RETRIES + 2):
            try:
                if attempt > 1:
                    logger.info(f"[{platform_key}] Попытка #{attempt}...")
                    time.sleep(REQUEST_DELAY * attempt)

                run = client.actor(actor_name).call(run_input=run_input)
                dataset = client.dataset(run["defaultDatasetId"])
                return list(dataset.list_items().items)

            except Exception as e:
                last_err_msg = str(e)
                logger.warning(f"[{platform_key}] Ошибка #{attempt}: {last_err_msg[:120]}")
                if "limit exceeded" in last_err_msg.lower():
                    break
                if attempt <= MAX_RETRIES:
                    continue

        # Если ошибка "requires full access", не долбим остальные токены
        if "requires full access" in last_err_msg.lower():
            break

    return []


def _build_batch_run_input(actor_name: str, urls: list[str]) -> dict | None:
    """Построить input для batch-запуска (несколько URL в 1 запрос)."""
    if actor_name == "apify/instagram-profile-scraper":
        usernames = []
        for url in urls:
            u = _extract_instagram_username(url)
            if u:
                usernames.append(u)
        if not usernames:
            return None
        # Убираем дубликаты, сохраняя порядок
        seen = set()
        unique = []
        for u in usernames:
            if u not in seen:
                seen.add(u)
                unique.append(u)
        return {"usernames": unique}

    if actor_name == "clockworks/tiktok-profile-scraper":
        profiles = []
        for url in urls:
            u = _extract_tiktok_username(url)
            if u:
                profiles.append(u)
        if not profiles:
            return None
        seen = set()
        unique = []
        for u in profiles:
            if u not in seen:
                seen.add(u)
                unique.append(u)
        return {
            "profiles": unique,
            "shouldDownloadCovers": False,
            "shouldDownloadSlideshowImages": False,
            "shouldDownloadSubtitles": False,
            "shouldDownloadVideos": False,
            "maxPosts": 1,
            "resultsPerPage": 1,
        }

    if actor_name == "apify/facebook-pages-scraper":
        return {"startUrls": [{"url": url} for url in urls], "resultsLimit": 1}

    if "pinterest" in actor_name:
        return {"startUrls": [{"url": url} for url in urls]}

    if actor_name == "apify/puppeteer-scraper":
        return {
            "pageFunction": (
                "async function pageFunction(context) {"
                "  const { page } = context;"
                "  await page.waitForTimeout(3000);"
                "  const r = await page.evaluate(() => {"
                "    const el = document.querySelector('[data-test-id=\"subscribers-count\"]');"
                "    if (el) return { subscribers: parseInt(el.textContent.replace(/[^0-9]/g, '')) };"
                "    const txt = document.body.innerText;"
                "    const m = txt.match(/(\\d[\\d\\s]*)\\s*(?:подписчик|subscriber|follower)/i);"
                "    if (m) return { subscribers: parseInt(m[1].replace(/\\s/g, '')) };"
                "    return { subscribers: 0, raw: txt.slice(0, 500) };"
                "  }); return r; }"
            ),
            "startUrls": [{"url": url} for url in urls],
            "maxPagesPerCrawl": len(urls),
            "maxResultsPerCrawl": len(urls),
            "proxyConfiguration": {"useApifyProxy": True},
        }

    return None  # нельзя забатчить


def _match_url_in_batch(
    target_url: str,
    batch_items: list[dict],
    actor_name: str,
    field_name: str | None,
) -> int | None:
    """Найти результат для target_url среди batch_items."""
    for item in batch_items:
        # Прямое совпадение URL
        for key in ("url", "inputUrl", "pageUrl", "input_url", "page_url"):
            val = item.get(key)
            if val and (val == target_url or val.rstrip("/") == target_url.rstrip("/")):
                return _find_followers_in_item(item, field_name or "")

    # Совпадение по username
    for item in batch_items:
        username = item.get("username")
        if username and username.lower() in target_url.lower():
            return _find_followers_in_item(item, field_name or "")

        # TikTok: authorMeta.uniqueId
        author = item.get("authorMeta", {}) or {}
        if isinstance(author, dict):
            uid = author.get("uniqueId") or author.get("name", "")
            if uid and uid.lower() in target_url.lower():
                return _find_followers_in_item(item, field_name or "")

        # Pinterest: username из URL
        if "pinterest" in actor_name:
            username = item.get("username")
            if username and username.lower() in target_url.lower():
                return _find_followers_in_item(item, field_name or "")

    # Если не нашли, возвращаем первый результат как fallback
    if batch_items:
        return _find_followers_in_item(batch_items[0], field_name or "")

    return None


def _safe_native(handler, url: str, name: str) -> int | None:
    """Безопасный вызов native-обработчика."""
    try:
        return handler(url, name)
    except Exception as e:
        logger.warning(f"[{name}] native error: {e}")
        return None


# ═══════════════════════════════════════════════
# Apify run_input builders (single URL — legacy / fallback)
# ═══════════════════════════════════════════════

def _build_run_input(actor_name: str, url: str, client_name: str) -> dict | None:
    if actor_name == "apify/instagram-profile-scraper":
        username = _extract_instagram_username(url)
        if not username:
            return None
        return {"usernames": [username]}

    if actor_name == "clockworks/tiktok-profile-scraper":
        username = _extract_tiktok_username(url)
        if not username:
            return None
        return {
            "profiles": [username],
            "shouldDownloadCovers": False,
            "shouldDownloadSlideshowImages": False,
            "shouldDownloadSubtitles": False,
            "shouldDownloadVideos": False,
            "maxPosts": 1,
            "resultsPerPage": 1,
        }

    if actor_name == "apify/facebook-pages-scraper":
        if "profile.php" in url:
            return None  # → native
        return {"startUrls": [{"url": url}], "resultsLimit": 1}

    if actor_name == "apify/puppeteer-scraper":
        return {
            "pageFunction": (
                "async function pageFunction(context) {"
                "  const { page } = context;"
                "  await page.waitForTimeout(3000);"
                "  const r = await page.evaluate(() => {"
                "    const el = document.querySelector('[data-test-id=\"subscribers-count\"]');"
                "    if (el) return { subscribers: parseInt(el.textContent.replace(/[^0-9]/g, '')) };"
                "    const txt = document.body.innerText;"
                "    const m = txt.match(/(\\d[\\d\\s]*)\\s*(?:подписчик|subscriber|follower)/i);"
                "    if (m) return { subscribers: parseInt(m[1].replace(/\\s/g, '')) };"
                "    return { subscribers: 0, raw: txt.slice(0, 500) };"
                "  }); return r; }"
            ),
            "startUrls": [{"url": url}],
            "maxPagesPerCrawl": 1,
            "maxResultsPerCrawl": 1,
            "proxyConfiguration": {"useApifyProxy": True},
        }

    # fallback
    return {"startUrls": [{"url": url}]}


def _extract_instagram_username(url: str) -> str | None:
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    parts = path.split("/")
    if parts and parts[0]:
        return parts[0].split("?")[0]
    return None


def _extract_tiktok_username(url: str) -> str | None:
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    for part in path.split("/"):
        if part.startswith("@"):
            return part[1:].split("?")[0]
    return None
