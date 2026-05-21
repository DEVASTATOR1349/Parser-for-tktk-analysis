import asyncio
import json
import logging
import os
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import aiohttp
import gspread
import requests

from workers.common import get_client, is_triggered, load_clients_config
from services.downloader import APIFY_TOKEN
from services.competitor_pipeline import _parse_dt, _format_dt, _parse_int, _metrics_text, _sanitize_caption, _join_text_list
import db as scout_db

log = logging.getLogger("scout.project_content")

DEFAULT_PROJECT_ADMIN_SHEET = "Админка проекта"
DEFAULT_PROJECT_VIDEOS_SHEET = "База Данных видео по проекту"
DEFAULT_PROJECT_HISTORY_SHEET = "История проекта по дням"
DEFAULT_ROWS = 1000
ADMIN_DATA_START_ROW = 2
VIDEO_DATA_START_ROW = 5
MAIN_SHEET_DATA_START_ROW = 5
HISTORY_DATA_START_ROW = 2
INSTAGRAM_ACTOR_ID = os.getenv("APIFY_INSTAGRAM_ACTOR_ID", "shu8hvrXbJbY3Eb9W")
INSTAGRAM_REEL_ACTOR_ID = os.getenv("APIFY_INSTAGRAM_REEL_ACTOR_ID", "apify~instagram-reel-scraper")
YOUTUBE_ACTOR_ID = os.getenv("APIFY_YOUTUBE_ACTOR_ID", "streamers~youtube-channel-scraper")
TIKTOK_APIFY_ACTOR_ID = os.getenv("SCOUT_TIKTOK_APIFY_ACTOR_ID", "clockworks~tiktok-profile-scraper")
FACEBOOK_PAGE_APIFY_ACTOR_ID = os.getenv("SCOUT_FACEBOOK_PAGE_APIFY_ACTOR_ID", "apify~facebook-pages-scraper")
FACEBOOK_POSTS_APIFY_ACTOR_ID = os.getenv("SCOUT_FACEBOOK_POSTS_APIFY_ACTOR_ID", "apify~facebook-posts-scraper")
TIKTOK_POST_APIFY_ACTOR_ID = os.getenv("SCOUT_TIKTOK_POST_APIFY_ACTOR_ID", "clockworks~tiktok-scraper")
INSTAGRAM_RESULTS_LIMIT = int(os.getenv("SCOUT_PROJECT_INSTAGRAM_RESULTS_LIMIT", "250"))
INSTAGRAM_MAIN_SHEET_EXTRA_URLS_LIMIT = int(os.getenv("SCOUT_PROJECT_INSTAGRAM_MAIN_SHEET_EXTRA_URLS_LIMIT", "100"))
YOUTUBE_RESULTS_LIMIT = int(os.getenv("SCOUT_PROJECT_YOUTUBE_RESULTS_LIMIT", "250"))
YOUTUBE_SHORTS_RESULTS_LIMIT = int(os.getenv("SCOUT_PROJECT_YOUTUBE_SHORTS_RESULTS_LIMIT", str(YOUTUBE_RESULTS_LIMIT)))
FULL_BACKFILL_RESULTS_LIMIT = int(os.getenv("SCOUT_PROJECT_FULL_BACKFILL_RESULTS_LIMIT", "5000"))
INSTAGRAM_MAX_APIFY_RUNS_PER_CYCLE = int(os.getenv("SCOUT_PROJECT_MAX_APIFY_RUNS_PER_CYCLE", "1"))
TIKTOK_MAX_APIFY_RUNS_PER_CYCLE = int(os.getenv("SCOUT_PROJECT_TIKTOK_MAX_APIFY_RUNS_PER_CYCLE", "1"))
FACEBOOK_MAX_APIFY_RUNS_PER_CYCLE = int(os.getenv("SCOUT_PROJECT_FACEBOOK_MAX_APIFY_RUNS_PER_CYCLE", "1"))
TIKTOK_RESULTS_LIMIT = int(os.getenv("SCOUT_PROJECT_TIKTOK_RESULTS_LIMIT", "200"))
FACEBOOK_RESULTS_LIMIT = int(os.getenv("SCOUT_PROJECT_FACEBOOK_RESULTS_LIMIT", "200"))
VK_RESULTS_LIMIT = int(os.getenv("SCOUT_PROJECT_VK_RESULTS_LIMIT", "500"))
RUTUBE_RESULTS_LIMIT = int(os.getenv("SCOUT_PROJECT_RUTUBE_RESULTS_LIMIT", "500"))
RUN_TIMEOUT_SECONDS = int(os.getenv("SCOUT_PROJECT_APIFY_TIMEOUT", "180"))
MASTER_TRIGGER_CELL = os.getenv("SCOUT_PROJECT_SYNC_TRIGGER_CELL", "B2")
MASTER_SUMMARY_ROW = int(os.getenv("SCOUT_PROJECT_SYNC_SUMMARY_ROW", "2"))
MSK = ZoneInfo("Europe/Moscow")
YOUTUBE_API_BASE_URL = "https://www.googleapis.com/youtube/v3"
EXTRA_ENV_FILE = os.getenv("SCOUT_EXTRA_ENV_FILE", "/root/.openclaw/workspace/scripts/api-keys.env")
VK_API_BASE_URL = "https://api.vk.com/method"
RUTUBE_API_BASE_URL = "https://rutube.ru/api"

PLATFORM_LABELS = {
    "instagram": "Инстаграм",
    "youtube": "Ютуб",
    "tiktok": "ТикТок",
    "facebook": "Фейсбук",
    "vk": "ВК",
    "rutube": "Рутуб",
    "dzen": "Дзен",
    "ok": "ОК",
    "pinterest": "Пинтерест",
    "likee": "Likee",
}

PLATFORM_ALIASES = {
    "инстаграм": "instagram",
    "instagram": "instagram",
    "ютуб": "youtube",
    "youtube": "youtube",
    "tiktok": "tiktok",
    "тикток": "tiktok",
    "facebook": "facebook",
    "фейсбук": "facebook",
    "vk": "vk",
    "вк": "vk",
    "rutube": "rutube",
    "рутуб": "rutube",
    "dzen": "dzen",
    "дзен": "dzen",
    "ok": "ok",
    "ок": "ok",
    "pinterest": "pinterest",
    "пинтерест": "pinterest",
    "likee": "likee",
}

DISABLED_PROJECT_PLATFORMS = {"pinterest", "likee", "dzen", "ok"}

UNSUPPORTED_PLATFORM_REASONS = {
    "dzen": "No stable public source for full project-owned video backfill in this runtime",
    "ok": "No stable public source for full project-owned video backfill in this runtime",
    "pinterest": "Account-level video feed fetch is not reliable here without paid/unavailable source",
    "likee": "No stable public source for full account video history in this runtime",
}

HISTORY_HEADERS = [[
    "Дата",
    "Подписчики Instagram",
    "Видео Instagram",
    "Подписчики YouTube",
    "Видео YouTube",
    "Последний чек Instagram",
    "Последний чек YouTube",
    "Статус Instagram",
    "Статус YouTube",
]]

PROJECT_VIDEO_SUMMARY_FIELDS = [
    (0, "Дата публикации"),
    (1, "Ссылка на аккаунт"),
    (2, "Платформа"),
    (3, "Ссылка на ролик"),
    (4, "Подпись ролика"),
    (5, "Первый комментарий"),
    (6, "Последние комментарии"),
    (7, "Хештеги"),
    (8, "Упоминания"),
    (9, "Дочерние посты"),
    (10, "Комментарии"),
    (11, "Лайки"),
    (12, "Просмотры (play)"),
    (13, "Просмотры (view)"),
    (14, "Длительность, сек"),
    (15, "Повторный сбор метрик"),
    (16, "Чек через сутки"),
    (17, "Чек через месяц"),
    (18, "Дата первого импорта"),
]


@dataclass
class ProjectAccountRow:
    row_idx: int
    account_url: str
    platform: str
    followers: str = ""
    total_videos: str = ""
    last_checked_at: str = ""
    last_full_import_at: str = ""
    new_videos_count: str = ""
    status: str = ""


@dataclass
class ProjectVideoRecord:
    published_at: str
    account_url: str
    platform_label: str
    video_url: str
    caption: str
    first_comment: str
    latest_comments: str
    hashtags: str
    mentions: str
    child_posts: str
    comments: int
    likes: int
    play_views: int
    raw_views: int
    duration_seconds: float | int | str
    recheck_enabled: bool
    day1_check: str
    month1_check: str
    imported_at: str

    def unique_key(self) -> tuple[str, str]:
        platform = canonical_platform_name(self.platform_label)
        return (platform, normalize_video_url(self.video_url, platform))


@dataclass
class ProjectDailySnapshot:
    date: str
    instagram_followers: str = ""
    instagram_videos: str = ""
    youtube_followers: str = ""
    youtube_videos: str = ""
    instagram_checked_at: str = ""
    youtube_checked_at: str = ""
    instagram_status: str = ""
    youtube_status: str = ""

    def as_row(self, existing_row: list[str] | None = None) -> list[str]:
        base = list(existing_row or [])
        while len(base) < 9:
            base.append("")
        return [
            self.date or base[0],
            self.instagram_followers or base[1],
            self.instagram_videos or base[2],
            self.youtube_followers or base[3],
            self.youtube_videos or base[4],
            self.instagram_checked_at or base[5],
            self.youtube_checked_at or base[6],
            self.instagram_status or base[7],
            self.youtube_status or base[8],
        ]


def _now_msk() -> datetime:
    return datetime.now(MSK)


def _now_str() -> str:
    return _now_msk().strftime("%d.%m.%Y %H:%M МСК")


def _today_label() -> str:
    return _now_msk().strftime("%d.%m")


def _today_full_date() -> str:
    return _now_msk().strftime("%d.%m.%Y")


def _format_dt_msk(dt: datetime | None) -> str:
    if not dt:
        return ""
    return dt.astimezone(MSK).strftime("%d.%m.%Y %H:%M")


def canonical_platform_name(value: str | None) -> str:
    return PLATFORM_ALIASES.get((value or "").strip().lower(), (value or "").strip().lower())


def detect_platform(url: str) -> str:
    value = (url or "").strip().lower()
    if not value:
        return ""
    if "instagram.com" in value:
        return "instagram"
    if "youtube.com" in value or "youtu.be" in value:
        return "youtube"
    if "tiktok.com" in value:
        return "tiktok"
    if "facebook.com" in value or "fb.com" in value:
        return "facebook"
    if "vk.com" in value or "vk.ru" in value:
        return "vk"
    if "rutube.ru" in value:
        return "rutube"
    if "dzen.ru" in value or "zen.yandex.ru" in value:
        return "dzen"
    if "ok.ru" in value:
        return "ok"
    if "pinterest.com" in value:
        return "pinterest"
    if "likee.video" in value or "l.likee.video" in value:
        return "likee"
    return ""


def platform_label(platform: str) -> str:
    normalized = canonical_platform_name(platform)
    return PLATFORM_LABELS.get(normalized, normalized)


def instagram_username_from_url(raw: str) -> str:
    value = (raw or "").strip().lstrip("@")
    if not value:
        return ""
    value = value.split("?", 1)[0].split("#", 1)[0]
    if not value.startswith("http"):
        return value.strip("/").split("/", 1)[0].lstrip("@")
    parsed = urlparse(value)
    if "instagram.com" not in parsed.netloc.lower():
        return ""
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        return ""
    # Account feeds can be configured as /user/, /user/reels/, /user/videos/.
    # Do not treat direct post/reel URLs as account usernames.
    if parts[0].lower() in {"p", "reel", "reels", "tv", "stories", "explore"}:
        return ""
    return parts[0].lstrip("@")


def instagram_reels_feed_url(raw: str) -> str:
    username = instagram_username_from_url(raw)
    return f"https://www.instagram.com/{username}/reels/" if username else ""


def normalize_account_url(raw: str, platform: str | None = None) -> str:
    value = (raw or "").strip()
    platform = canonical_platform_name(platform or detect_platform(value))
    if not value:
        return ""
    if platform == "instagram":
        username = instagram_username_from_url(value)
        return f"https://www.instagram.com/{username}/" if username else ""
    if platform == "youtube":
        return value.rstrip("/")
    if platform == "tiktok":
        value = value.split("?", 1)[0].rstrip("/")
        value = re.sub(r"^https?://m\.tiktok\.com/", "https://www.tiktok.com/", value, flags=re.I)
        return value
    if platform == "facebook":
        parsed = urlparse(value)
        host = re.sub(r"^(m|mbasic)\.", "www.", parsed.netloc.lower())
        path = parsed.path.rstrip("/")
        query = f"?{parsed.query}" if parsed.query and path == "/profile.php" else ""
        return f"https://{host}{path}{query}" if host else value
    if platform == "vk":
        parsed = urlparse(value)
        host = "vk.com"
        path = parsed.path.rstrip("/")
        return f"https://{host}{path}" if path else value.rstrip("/")
    if platform == "rutube":
        parsed = urlparse(value)
        return f"https://rutube.ru{parsed.path.rstrip('/')}" if parsed.path else value.rstrip("/")
    if platform in {"dzen", "ok", "pinterest", "likee"}:
        parsed = urlparse(value)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
    return value


def normalize_video_url(raw: str, platform: str | None = None) -> str:
    value = (raw or "").strip()
    platform = canonical_platform_name(platform or detect_platform(value) or "")
    if not value:
        return ""
    if platform in ("youtube", "ютуб"):
        parsed = urlparse(value)
        host = parsed.netloc.lower()
        if "youtu.be" in host:
            video_id = parsed.path.strip("/")
            return f"https://www.youtube.com/watch?v={video_id}" if video_id else value
        if "youtube.com" in host:
            if parsed.path.startswith("/watch"):
                qs = parse_qs(parsed.query)
                video_id = (qs.get("v") or [""])[0]
                return f"https://www.youtube.com/watch?v={video_id}" if video_id else value
            short_match = re.search(r"/shorts/([^/?#]+)", parsed.path)
            if short_match:
                return f"https://www.youtube.com/watch?v={short_match.group(1)}"
        return value
    if platform in ("instagram", "инстаграм"):
        value = value.split("?", 1)[0].rstrip("/")
        parsed = urlparse(value if value.startswith("http") else f"https://www.instagram.com/{value.lstrip('/')}")
        match = re.search(r"/(?:reel|p)/([^/?#]+)/?", parsed.path, re.I)
        if match:
            return f"https://www.instagram.com/p/{match.group(1)}/"
        return value + "/"
    if platform == "tiktok":
        parsed = urlparse(value)
        path = parsed.path.rstrip("/")
        match = re.search(r"/@([^/]+)/video/(\d+)", path, re.I)
        if match:
            return f"https://www.tiktok.com/@{match.group(1)}/video/{match.group(2)}"
        return f"https://www.tiktok.com{path}" if path else value.split("?", 1)[0]
    if platform == "facebook":
        parsed = urlparse(value)
        host = "www.facebook.com"
        path = parsed.path.rstrip("/")
        query = f"?{parsed.query}" if parsed.query and path == "/watch" else ""
        return f"https://{host}{path}{query}" if path else value.split("?", 1)[0]
    if platform == "vk":
        if re.fullmatch(r"video-?\d+_\d+", value, re.I):
            return f"https://vk.com/{value.lower()}"
        parsed = urlparse(value)
        path = parsed.path.rstrip("/")
        match = re.search(r"(video-?\d+_\d+)", path, re.I)
        if match:
            return f"https://vk.com/{match.group(1).lower()}"
        return f"https://vk.com{path}" if path else value.split("?", 1)[0]
    if platform == "rutube":
        parsed = urlparse(value)
        path = parsed.path.rstrip("/")
        match = re.search(r"/video/([0-9a-f-]+)", path, re.I)
        if match:
            return f"https://rutube.ru/video/{match.group(1)}/"
        return f"https://rutube.ru{path}" if path else value.split("?", 1)[0]
    if platform in {"dzen", "ok", "pinterest", "likee"}:
        parsed = urlparse(value)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
    return value


def _sheet_call(func, *args, **kwargs):
    for attempt in range(5):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            text = str(e)
            if "429" in text or "Quota exceeded" in text:
                if attempt == 4:
                    raise
                sleep_for = 10 * (attempt + 1)
                log.warning("Sheets quota hit, sleeping %ss before retry", sleep_for)
                time.sleep(sleep_for)
                continue
            raise


def ensure_sheet(sh, title: str, header_rows: list[list[str]], rows: int = 400, cols: int = 12):
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
        log.info("Created worksheet %s", title)

    max_cols = max(len(r) for r in header_rows)
    if ws.col_count < max_cols:
        ws.add_cols(max_cols - ws.col_count)
    if ws.row_count < rows:
        ws.add_rows(rows - ws.row_count)

    from gspread.utils import rowcol_to_a1

    ws.update(f"A1:{rowcol_to_a1(len(header_rows), max_cols)}", header_rows, value_input_option="USER_ENTERED")
    try:
        ws.freeze(rows=len(header_rows))
    except Exception:
        pass
    return ws


async def _apify_run(actor_id: str, payload: dict) -> list[dict]:
    result = await _apify_run_with_meta(actor_id, payload)
    return result.get("items") or []


async def _apify_run_with_meta(actor_id: str, payload: dict) -> dict[str, Any]:
    _load_extra_env()
    if not APIFY_TOKEN:
        raise RuntimeError("APIFY_API_TOKEN is not configured")

    base = f"https://api.apify.com/v2/acts/{actor_id}"
    timeout = aiohttp.ClientTimeout(total=RUN_TIMEOUT_SECONDS)
    headers = {
        "Authorization": f"Bearer {APIFY_TOKEN}",
        "Content-Type": "application/json",
        # aiohttp on this host intermittently fails on Brotli responses from Apify
        # ("Can not decode content-encoding: br"). Do not advertise br support.
        "Accept-Encoding": "gzip, deflate",
    }
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                resp = await session.post(
                    f"{base}/runs",
                    json=payload,
                )
                data = await resp.json()
                run = (data or {}).get("data", {})
                run_id = run.get("id")
                dataset_id = run.get("defaultDatasetId")
                if not run_id:
                    raise RuntimeError(f"Apify run start failed: {data}")

                final_run_details = run
                for _ in range(max(10, RUN_TIMEOUT_SECONDS // 3)):
                    await asyncio.sleep(3)
                    sr = await session.get(f"{base}/runs/{run_id}", params={"token": APIFY_TOKEN})
                    status_data = await sr.json()
                    final_run_details = ((status_data or {}).get("data", {}) or {})
                    status = final_run_details.get("status")
                    if status == "SUCCEEDED":
                        break
                    if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                        raise RuntimeError(f"Apify run failed: {status}")
                else:
                    raise RuntimeError("Apify run timeout")

                items_resp = await session.get(
                    f"https://api.apify.com/v2/datasets/{dataset_id}/items",
                    params={"token": APIFY_TOKEN, "clean": "true"},
                )
                items = await items_resp.json()
                if not isinstance(items, list):
                    raise RuntimeError(f"Unexpected Apify dataset payload: {items}")
                return {
                    "items": items,
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "run_details": final_run_details or {},
                    "request_payload": payload,
                    "actor_id": actor_id,
                }
        except Exception as exc:
            last_error = exc
            if attempt >= 2:
                break
            await asyncio.sleep(5 * (attempt + 1))
            continue
    raise last_error or RuntimeError("Apify run failed")


async def fetch_instagram_account(
    account_url: str,
    results_limit: int | None = None,
    *,
    extra_direct_urls: list[str] | None = None,
    client_config: dict | None = None,
    purpose: str = "instagram_profile_check",
    operation: str = "instagram_profile_fetch",
    local_run_id: int | None = None,
    local_query_id: int | None = None,
    metadata_json: dict | None = None,
) -> dict[str, Any]:
    limit = max(1, int(results_limit if results_limit is not None else INSTAGRAM_RESULTS_LIMIT))
    normalized_account_url = normalize_account_url(account_url, "instagram")
    normalized_extra_urls: list[str] = []
    for raw in extra_direct_urls or []:
        normalized = normalize_video_url(raw, "instagram")
        if normalized and normalized != normalized_account_url and normalized not in normalized_extra_urls:
            normalized_extra_urls.append(normalized)
    # IMPORTANT: account-level Instagram parsing must use the dedicated Reel Scraper.
    # The generic Instagram Scraper charges for mixed post/feed results even when `/reels/`
    # is passed, so it can bill us for Image/Sidecar items. The Reel Scraper is priced per
    # reel written to dataset and does not scrape generic photo posts.
    reel_actor_inputs = [normalized_account_url, *normalized_extra_urls]
    payload = {
        "username": reel_actor_inputs,
        "resultsLimit": limit,
        "skipPinnedPosts": True,
        "includeSharesCount": False,
        "includeTranscript": False,
        "includeDownloadedVideo": False,
    }
    started_at = datetime.utcnow()
    try:
        apify_result = await _apify_run_with_meta(INSTAGRAM_REEL_ACTOR_ID, payload)
    except Exception as exc:
        try:
            scout_db.create_api_cost_event(
                client_config=client_config,
                provider='apify',
                service='instagram-reel-scraper',
                operation=operation,
                actor_or_model=INSTAGRAM_REEL_ACTOR_ID,
                purpose=purpose,
                status='error',
                local_run_id=local_run_id,
                local_query_id=local_query_id,
                request_count=1,
                result_count=0,
                usage_usd=0.0,
                usage_units=None,
                unit_type='apify_usage',
                error_text=str(exc),
                request_json=payload,
                response_json={'error': str(exc)},
                metadata_json={
                    'account_url': normalized_account_url,
                    'results_limit': limit,
                    'extra_direct_urls': normalized_extra_urls,
                    **(metadata_json or {}),
                },
                started_at=started_at,
                finished_at=datetime.utcnow(),
            )
        except Exception as cost_err:
            log.warning("Apify api_cost_events error log failed for %s: %s", account_url, cost_err)
        raise

    items = apify_result.get("items") or []
    items = sorted(
        items,
        key=lambda item: _parse_dt(item.get("timestamp") or item.get("takenAtTimestamp") or item.get("publishedAt")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    run_details = apify_result.get("run_details") or {}
    try:
        scout_db.create_api_cost_event(
            client_config=client_config,
            provider='apify',
            service='instagram-reel-scraper',
            operation=operation,
            actor_or_model=INSTAGRAM_REEL_ACTOR_ID,
            purpose=purpose,
            status='ok',
            external_run_id=apify_result.get('run_id') or None,
            local_run_id=local_run_id,
            local_query_id=local_query_id,
            request_count=1,
            result_count=int(((run_details.get('chargedEventCounts') or {}).get('result')) or len(items) or 0),
            usage_usd=float(run_details.get('usageTotalUsd') or 0),
            usage_units=float((run_details.get('usage') or {}).get('ACTOR_COMPUTE_UNITS') or 0) if run_details.get('usage') else None,
            unit_type='apify_usage',
            request_json=payload,
            response_json={
                'apify_run_id': apify_result.get('run_id'),
                'items_count': len(items),
                'chargedEventCounts': run_details.get('chargedEventCounts'),
                'pricingInfo': run_details.get('pricingInfo'),
            },
            metadata_json={
                'account_url': normalized_account_url,
                'results_limit': limit,
                'extra_direct_urls': normalized_extra_urls,
                **(metadata_json or {}),
            },
            started_at=run_details.get('startedAt') or started_at,
            finished_at=run_details.get('finishedAt') or datetime.utcnow(),
        )
    except Exception as cost_err:
        log.warning("Apify api_cost_events success log failed for %s: %s", account_url, cost_err)

    first = items[0] if items else {}
    followers_value = max((_parse_int(item.get("followersCount")) for item in items), default=0)
    total_videos_value = max((_parse_int(item.get("postsCount")) for item in items), default=0)
    return {
        "followers": str(followers_value or _parse_int(first.get("followersCount"))),
        "total_videos": str(total_videos_value or _parse_int(first.get("postsCount"))),
        "items": items,
    }


def build_instagram_apify_batches(accounts: list[tuple[str, int]], max_runs: int | None = None) -> list[list[tuple[str, int]]]:
    normalized: list[tuple[str, int]] = []
    seen: set[str] = set()
    for account_url, limit in accounts:
        normalized_url = normalize_account_url(account_url, "instagram")
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        normalized.append((normalized_url, max(1, int(limit or INSTAGRAM_RESULTS_LIMIT))))

    if not normalized:
        return []

    runs = max(1, int(max_runs or INSTAGRAM_MAX_APIFY_RUNS_PER_CYCLE or 1))
    runs = min(runs, len(normalized))
    base = len(normalized) // runs
    extra = len(normalized) % runs
    batches: list[list[tuple[str, int]]] = []
    cursor = 0
    for idx in range(runs):
        size = base + (1 if idx < extra else 0)
        batch = normalized[cursor:cursor + size]
        if batch:
            batches.append(batch)
        cursor += size
    return batches


async def fetch_instagram_accounts_batch(accounts: list[tuple[str, int]]) -> dict[str, dict[str, Any]]:
    if not accounts:
        return {}

    unique_accounts: list[tuple[str, int]] = []
    seen: set[str] = set()
    for account_url, limit in accounts:
        normalized_url = normalize_account_url(account_url, "instagram")
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        unique_accounts.append((normalized_url, max(1, int(limit or INSTAGRAM_RESULTS_LIMIT))))

    batch_limit = max(limit for _, limit in unique_accounts)
    payload = {
        "username": [account_url for account_url, _ in unique_accounts],
        "resultsLimit": batch_limit,
        "skipPinnedPosts": True,
        "includeSharesCount": False,
        "includeTranscript": False,
        "includeDownloadedVideo": False,
    }
    apify_result = await _apify_run_with_meta(INSTAGRAM_REEL_ACTOR_ID, payload)
    grouped_items: dict[str, list[dict[str, Any]]] = {account_url: [] for account_url, _ in unique_accounts}
    for item in apify_result.get("items") or []:
        input_url = normalize_account_url(str(item.get("inputUrl") or item.get("ownerProfilePicUrl") or "").strip(), "instagram") if str(item.get("inputUrl") or "").strip() else ""
        if input_url and input_url in grouped_items:
            grouped_items[input_url].append(item)
            continue
        owner_username = str(item.get("ownerUsername") or item.get("username") or "").strip().lstrip("@")
        if owner_username:
            fallback_url = normalize_account_url(owner_username, "instagram")
            if fallback_url in grouped_items:
                grouped_items[fallback_url].append(item)

    result: dict[str, dict[str, Any]] = {}
    for account_url, limit in unique_accounts:
        items = sorted(
            grouped_items.get(account_url) or [],
            key=lambda item: _parse_dt(item.get("timestamp") or item.get("takenAtTimestamp") or item.get("publishedAt")) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        trimmed = items[:limit]
        first = trimmed[0] if trimmed else {}
        result[account_url] = {
            "followers": str(_parse_int(first.get("followersCount"))),
            "total_videos": str(_parse_int(first.get("postsCount"))),
            "items": trimmed,
        }
    return result


def build_tiktok_apify_batches(accounts: list[tuple[str, int]], max_runs: int | None = None) -> list[list[tuple[str, int]]]:
    normalized: list[tuple[str, int]] = []
    seen: set[str] = set()
    for account_url, limit in accounts:
        normalized_url = normalize_account_url(account_url, "tiktok")
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        normalized.append((normalized_url, max(1, int(limit or TIKTOK_RESULTS_LIMIT))))

    if not normalized:
        return []

    runs = max(1, int(max_runs or TIKTOK_MAX_APIFY_RUNS_PER_CYCLE or 1))
    runs = min(runs, len(normalized))
    base = len(normalized) // runs
    extra = len(normalized) % runs
    batches: list[list[tuple[str, int]]] = []
    cursor = 0
    for idx in range(runs):
        size = base + (1 if idx < extra else 0)
        batch = normalized[cursor:cursor + size]
        if batch:
            batches.append(batch)
        cursor += size
    return batches


async def fetch_tiktok_accounts_batch(accounts: list[tuple[str, int]]) -> dict[str, dict[str, Any]]:
    if not accounts:
        return {}

    unique_accounts: list[tuple[str, int]] = []
    seen: set[str] = set()
    for account_url, limit in accounts:
        normalized_url = normalize_account_url(account_url, "tiktok")
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        unique_accounts.append((normalized_url, max(1, int(limit or TIKTOK_RESULTS_LIMIT))))

    batch_limit = max(limit for _, limit in unique_accounts)
    usernames = [_tiktok_profile_name(url) for url, _ in unique_accounts]
    usernames = [u for u in usernames if u]
    if not usernames:
        return {}

    apify_result = await _apify_run_with_meta(
        TIKTOK_APIFY_ACTOR_ID,
        {
            "profiles": usernames,
            "resultsPerPage": batch_limit,
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
            "shouldDownloadSubtitles": False,
            "shouldDownloadSlideshowImages": False,
        },
    )

    grouped_items: dict[str, list[dict[str, Any]]] = {url: [] for url, _ in unique_accounts}
    for item in apify_result.get("items") or []:
        author = item.get("authorMeta") or {}
        author_name = (author.get("name") or author.get("uniqueId") or "").strip().lstrip("@").lower()
        if author_name:
            for account_url, _ in unique_accounts:
                if (_tiktok_profile_name(account_url) or "").lower() == author_name:
                    grouped_items[account_url].append(item)
                    break

    result: dict[str, dict[str, Any]] = {}
    for account_url, limit in unique_accounts:
        items = sorted(
            grouped_items.get(account_url) or [],
            key=lambda item: _parse_dt(item.get("createTimeISO") or item.get("createTime") or item.get("timestamp")) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        trimmed = items[:limit]
        first = trimmed[0] if trimmed else {}
        author = first.get("authorMeta") or {}
        followers = author.get("fans") or author.get("followers") or first.get("followersCount") or ""
        result[account_url] = {
            "followers": str(_parse_int(followers)),
            "total_videos": str(len(trimmed)),
            "items": trimmed,
        }
    return result


def build_facebook_apify_batches(accounts: list[tuple[str, int]], max_runs: int | None = None) -> list[list[tuple[str, int]]]:
    normalized: list[tuple[str, int]] = []
    seen: set[str] = set()
    for account_url, limit in accounts:
        normalized_url = normalize_account_url(account_url, "facebook")
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        normalized.append((normalized_url, max(1, int(limit or FACEBOOK_RESULTS_LIMIT))))

    if not normalized:
        return []

    runs = max(1, int(max_runs or FACEBOOK_MAX_APIFY_RUNS_PER_CYCLE or 1))
    runs = min(runs, len(normalized))
    base = len(normalized) // runs
    extra = len(normalized) % runs
    batches: list[list[tuple[str, int]]] = []
    cursor = 0
    for idx in range(runs):
        size = base + (1 if idx < extra else 0)
        batch = normalized[cursor:cursor + size]
        if batch:
            batches.append(batch)
        cursor += size
    return batches


async def fetch_facebook_accounts_batch(accounts: list[tuple[str, int]]) -> dict[str, dict[str, Any]]:
    if not accounts:
        return {}

    unique_accounts: list[tuple[str, int]] = []
    seen: set[str] = set()
    for account_url, limit in accounts:
        normalized_url = normalize_account_url(account_url, "facebook")
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        unique_accounts.append((normalized_url, max(1, int(limit or FACEBOOK_RESULTS_LIMIT))))

    batch_limit = max(limit for _, limit in unique_accounts)

    # Single Apify run for all Facebook accounts — Posts scraper only
    post_items = await _apify_run(
        FACEBOOK_POSTS_APIFY_ACTOR_ID,
        {
            "startUrls": [{"url": url} for url, _ in unique_accounts],
            "resultsLimit": batch_limit * len(unique_accounts),
        },
    )

    grouped_posts: dict[str, list[dict]] = {url: [] for url, _ in unique_accounts}
    for item in post_items:
        if not bool(item.get("isVideo")):
            continue
        item_url = (item.get("pageUrl") or item.get("pageProfileUrl") or item.get("url") or "").rstrip("/")
        for acc_url, _ in unique_accounts:
            if acc_url.rstrip("/") == item_url or acc_url.rstrip("/") in item_url:
                grouped_posts[acc_url].append(item)
                break

    result: dict[str, dict[str, Any]] = {}
    for account_url, limit in unique_accounts:
        items = sorted(
            grouped_posts.get(account_url) or [],
            key=lambda item: _parse_dt(item.get("time") or item.get("timestamp")) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        trimmed = items[:limit]
        first = (post_items[0] if post_items else {})
        followers = _parse_int(first.get("pageFollowers") or first.get("pageLikes") or first.get("likes") or "")
        result[account_url] = {
            "followers": str(followers),
            "total_videos": str(len(trimmed)),
            "items": trimmed,
        }
    return result


def _load_extra_env() -> None:
    global APIFY_TOKEN
    env_path = EXTRA_ENV_FILE
    if not env_path or not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as fh:
            for line in fh:
                raw = line.strip()
                if not raw or raw.startswith("#") or "=" not in raw:
                    continue
                key, value = raw.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and value and key not in os.environ:
                    os.environ[key] = value
    except Exception as exc:
        log.warning("Could not load extra env file %s: %s", env_path, exc)
    APIFY_TOKEN = os.getenv("APIFY_API_TOKEN") or APIFY_TOKEN


def _youtube_api_key() -> str:
    key = (os.getenv("YOUTUBE_API_KEY") or "").strip()
    if key:
        return key
    _load_extra_env()
    return (os.getenv("YOUTUBE_API_KEY") or "").strip()


def _youtube_extract_channel_locator(account_url: str) -> tuple[str, str] | None:
    parsed = urlparse(normalize_account_url(account_url, "youtube"))
    path = (parsed.path or "").strip("/")
    if not path:
        return None
    segments = [segment for segment in path.split("/") if segment]
    if not segments:
        return None
    first = segments[0]
    if first.startswith("@"):
        return ("forHandle", first[1:])
    if first == "channel" and len(segments) >= 2:
        return ("id", segments[1])
    if first == "user" and len(segments) >= 2:
        return ("forUsername", segments[1])
    return ("customName", segments[-1])


def _youtube_description_links(description: str) -> list[dict[str, str]]:
    if not description:
        return []
    links: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in re.finditer(r"https?://[^\s<>\"']+", description):
        url = match.group(0).rstrip("),.;]")
        if url and url not in seen:
            seen.add(url)
            links.append({"url": url})
    return links


async def _youtube_api_get(session: aiohttp.ClientSession, resource: str, params: dict[str, Any]) -> dict[str, Any]:
    api_key = _youtube_api_key()
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY is not configured")

    async with session.get(f"{YOUTUBE_API_BASE_URL}/{resource}", params={**params, "key": api_key}) as resp:
        data = await resp.json()
        if resp.status >= 400:
            message = ((data.get("error") or {}).get("message") if isinstance(data, dict) else "") or resp.reason
            raise RuntimeError(f"YouTube API {resource} failed: {message}")
        if isinstance(data, dict) and data.get("error"):
            message = (data["error"] or {}).get("message") or "unknown error"
            raise RuntimeError(f"YouTube API {resource} failed: {message}")
        return data if isinstance(data, dict) else {}


async def _youtube_fetch_channel_details(session: aiohttp.ClientSession, account_url: str) -> dict[str, Any]:
    locator = _youtube_extract_channel_locator(account_url)
    if not locator:
        raise RuntimeError(f"Unsupported YouTube account URL: {account_url}")

    locator_name, locator_value = locator
    if locator_name == "customName":
        search_data = await _youtube_api_get(
            session,
            "search",
            {
                "part": "snippet",
                "type": "channel",
                "q": locator_value,
                "maxResults": 1,
            },
        )
        channel_id = (((search_data.get("items") or [{}])[0].get("snippet") or {}).get("channelId") or "")
        if not channel_id:
            raise RuntimeError(f"YouTube channel not found: {account_url}")
        locator_name = "id"
        locator_value = channel_id

    channel_data = await _youtube_api_get(
        session,
        "channels",
        {
            "part": "snippet,statistics,contentDetails",
            locator_name: locator_value,
            "maxResults": 1,
        },
    )
    items = channel_data.get("items") or []
    if not items:
        raise RuntimeError(f"YouTube channel not found: {account_url}")
    return items[0]


async def _youtube_fetch_upload_items(
    session: aiohttp.ClientSession,
    uploads_playlist_id: str,
    results_limit: int,
) -> list[dict[str, Any]]:
    playlist_items: list[dict[str, Any]] = []
    page_token = ""
    remaining = max(0, int(results_limit))
    while remaining > 0:
        batch_size = min(50, remaining)
        params: dict[str, Any] = {
            "part": "snippet,contentDetails",
            "playlistId": uploads_playlist_id,
            "maxResults": batch_size,
        }
        if page_token:
            params["pageToken"] = page_token
        data = await _youtube_api_get(session, "playlistItems", params)
        items = data.get("items") or []
        if not items:
            break
        playlist_items.extend(items)
        remaining -= len(items)
        page_token = data.get("nextPageToken") or ""
        if not page_token:
            break
    return playlist_items


async def _youtube_fetch_videos(session: aiohttp.ClientSession, video_ids: list[str]) -> dict[str, dict[str, Any]]:
    videos_by_id: dict[str, dict[str, Any]] = {}
    for index in range(0, len(video_ids), 50):
        chunk = video_ids[index:index + 50]
        if not chunk:
            continue
        data = await _youtube_api_get(
            session,
            "videos",
            {
                "part": "snippet,contentDetails,statistics",
                "id": ",".join(chunk),
                "maxResults": len(chunk),
            },
        )
        for item in data.get("items") or []:
            video_id = item.get("id")
            if video_id:
                videos_by_id[video_id] = item
    return videos_by_id


def _youtube_playlist_item_to_record(playlist_item: dict[str, Any], video_item: dict[str, Any]) -> dict[str, Any] | None:
    video_id = (
        ((playlist_item.get("contentDetails") or {}).get("videoId"))
        or video_item.get("id")
        or ""
    )
    if not video_id:
        return None

    snippet = (video_item.get("snippet") or {}).copy()
    playlist_snippet = playlist_item.get("snippet") or {}
    if not snippet:
        snippet = playlist_snippet.copy()

    statistics = video_item.get("statistics") or {}
    content_details = video_item.get("contentDetails") or {}
    description = snippet.get("description") or ""
    title = snippet.get("title") or ""
    published_at = (
        snippet.get("publishedAt")
        or ((playlist_item.get("contentDetails") or {}).get("videoPublishedAt"))
        or playlist_snippet.get("publishedAt")
        or ""
    )
    tags = snippet.get("tags") or []

    return {
        "id": video_id,
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "publishedAt": published_at,
        "date": published_at,
        "viewCount": statistics.get("viewCount") or "",
        "views": statistics.get("viewCount") or "",
        "likeCount": statistics.get("likeCount") or "",
        "likes": statistics.get("likeCount") or "",
        "commentCount": statistics.get("commentCount") or "",
        "commentsCount": statistics.get("commentCount") or "",
        "duration": content_details.get("duration") or "",
        "title": title,
        "text": title,
        "description": description,
        "tags": tags,
        "hashtags": tags,
        "descriptionLinks": _youtube_description_links(description),
    }


async def fetch_youtube_account(account_url: str, results_limit: int | None = None) -> dict[str, Any]:
    normalized_url = normalize_account_url(account_url, "youtube")
    limit = max(0, int(results_limit if results_limit is not None else YOUTUBE_RESULTS_LIMIT))
    timeout = aiohttp.ClientTimeout(total=RUN_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        channel = await _youtube_fetch_channel_details(session, normalized_url)
        statistics = channel.get("statistics") or {}
        uploads_playlist_id = (((channel.get("contentDetails") or {}).get("relatedPlaylists") or {}).get("uploads") or "")
        playlist_items = await _youtube_fetch_upload_items(session, uploads_playlist_id, limit) if uploads_playlist_id and limit else []

        video_ids: list[str] = []
        seen_ids: set[str] = set()
        for playlist_item in playlist_items:
            video_id = ((playlist_item.get("contentDetails") or {}).get("videoId") or "").strip()
            if video_id and video_id not in seen_ids:
                seen_ids.add(video_id)
                video_ids.append(video_id)

        videos_by_id = await _youtube_fetch_videos(session, video_ids) if video_ids else {}
        items: list[dict[str, Any]] = []
        for playlist_item in playlist_items:
            video_id = ((playlist_item.get("contentDetails") or {}).get("videoId") or "").strip()
            video_item = videos_by_id.get(video_id)
            if not video_item:
                continue
            record = _youtube_playlist_item_to_record(playlist_item, video_item)
            if record:
                items.append(record)

    items = sorted(
        items,
        key=lambda item: _parse_dt(item.get("date") or item.get("publishedAt")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return {
        "followers": str(_parse_int(statistics.get("subscriberCount") or "")),
        "total_videos": str(_parse_int(statistics.get("videoCount") or "")),
        "items": items,
    }


def _tiktok_profile_name(url: str) -> str:
    match = re.search(r"tiktok\.com/@([^/?#]+)", url or "", re.I)
    return match.group(1) if match else ""


async def fetch_tiktok_account(account_url: str, results_limit: int | None = None) -> dict[str, Any]:
    _load_extra_env()
    username = _tiktok_profile_name(account_url)
    if not username:
        raise RuntimeError(f"TikTok username missing in URL: {account_url}")
    limit = max(1, int(results_limit if results_limit is not None else TIKTOK_RESULTS_LIMIT))
    items = await _apify_run(
        TIKTOK_APIFY_ACTOR_ID,
        {
            "profiles": [username],
            "resultsPerPage": limit,
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
            "shouldDownloadSubtitles": False,
            "shouldDownloadSlideshowImages": False,
        },
    )
    items = sorted(
        items,
        key=lambda item: _parse_dt(item.get("createTimeISO") or item.get("createTime") or item.get("timestamp")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    first = items[0] if items else {}
    author = first.get("authorMeta") or {}
    followers = author.get("fans") or author.get("followers") or first.get("followersCount") or ""
    return {
        "followers": str(_parse_int(followers)),
        "total_videos": str(len(items)),
        "items": items,
    }


async def fetch_facebook_account(account_url: str, results_limit: int | None = None) -> dict[str, Any]:
    _load_extra_env()
    limit = max(1, min(int(results_limit if results_limit is not None else FACEBOOK_RESULTS_LIMIT), 500))
    post_items = await _apify_run(
        FACEBOOK_POSTS_APIFY_ACTOR_ID,
        {
            "startUrls": [{"url": account_url}],
            "resultsLimit": limit,
        },
    )
    items = [item for item in post_items if bool(item.get("isVideo"))]
    items = sorted(
        items,
        key=lambda item: _parse_dt(item.get("time") or item.get("timestamp")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    first = post_items[0] if post_items else {}
    followers = _parse_int(first.get("pageFollowers") or first.get("pageLikes") or first.get("likes") or "")
    return {
        "followers": str(followers),
        "total_videos": str(len(items)),
        "items": items,
    }


async def _vk_api_get(session: aiohttp.ClientSession, method: str, params: dict[str, Any]) -> dict[str, Any]:
    _load_extra_env()
    token = (os.getenv("VK_API_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("VK_API_TOKEN is not configured")
    all_params = {**params, "access_token": token, "v": "5.131"}
    async with session.get(f"{VK_API_BASE_URL}/{method}", params=all_params) as resp:
        data = await resp.json()
        if resp.status >= 400:
            raise RuntimeError(f"VK API {method} failed: {resp.reason}")
        if isinstance(data, dict) and data.get("error"):
            error = data["error"] or {}
            message = error.get("error_msg") or error.get("error_text") or str(error)
            raise RuntimeError(f"VK API {method} failed: {message}")
        response = (data or {}).get("response")
        return response if isinstance(response, dict) else {"items": response} if isinstance(response, list) else {}


async def fetch_vk_account(account_url: str, results_limit: int | None = None) -> dict[str, Any]:
    normalized_url = normalize_account_url(account_url, "vk")
    screen_name = (urlparse(normalized_url).path or "").strip("/")
    if not screen_name:
        raise RuntimeError(f"VK screen_name missing in URL: {account_url}")
    limit = max(1, int(results_limit if results_limit is not None else VK_RESULTS_LIMIT))
    timeout = aiohttp.ClientTimeout(total=RUN_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        resolved = await _vk_api_get(session, "utils.resolveScreenName", {"screen_name": screen_name})
        object_id = int(resolved.get("object_id") or 0)
        object_type = str(resolved.get("type") or "")
        if not object_id or object_type not in {"group", "user"}:
            raise RuntimeError(f"VK screen_name unresolved or unsupported: {account_url}")

        followers = ""
        owner_id = object_id
        if object_type == "group":
            owner_id = -object_id
            group_data = await _vk_api_get(session, "groups.getById", {"group_id": object_id, "fields": "members_count"})
            group_items = group_data.get("items") or []
            first_group = group_items[0] if group_items else {}
            followers = str(_parse_int(first_group.get("members_count") or ""))
        else:
            user_data = await _vk_api_get(session, "users.get", {"user_ids": object_id, "fields": "followers_count"})
            user_items = user_data.get("items") or []
            first_user = user_items[0] if user_items else {}
            followers = str(_parse_int(first_user.get("followers_count") or ""))

        items: list[dict[str, Any]] = []
        offset = 0
        while len(items) < limit:
            batch_count = min(100, max(100, limit - len(items)))
            wall_data = await _vk_api_get(
                session,
                "wall.get",
                {"owner_id": owner_id, "count": batch_count, "offset": offset},
            )
            wall_items = wall_data.get("items") or []
            if not wall_items:
                break
            for post in wall_items:
                for att in (post.get("attachments") or []):
                    if (att or {}).get("type") != "video":
                        continue
                    video = (att or {}).get("video") or {}
                    if not video:
                        continue
                    if post.get("id"):
                        video.setdefault("post_url", f"https://vk.com/wall{owner_id}_{post.get('id')}")
                    items.append(video)
                    if len(items) >= limit:
                        break
                if len(items) >= limit:
                    break
            offset += len(wall_items)
            if len(wall_items) < batch_count:
                break
        return {
            "followers": followers,
            "total_videos": str(len(items)),
            "items": items,
        }


async def _rutube_api_get(session: aiohttp.ClientSession, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    del session
    def _call() -> dict[str, Any]:
        response = requests.get(
            f"{RUTUBE_API_BASE_URL}{path}",
            params=params,
            headers={"User-Agent": "Mozilla/5.0", "Accept-Encoding": "identity"},
            timeout=RUN_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, dict) else {}

    return await asyncio.to_thread(_call)


async def fetch_rutube_account(account_url: str, results_limit: int | None = None) -> dict[str, Any]:
    normalized_url = normalize_account_url(account_url, "rutube")
    match = re.search(r"/channel/(\d+)", normalized_url)
    if not match:
        raise RuntimeError(f"RuTube channel id missing in URL: {account_url}")
    channel_id = match.group(1)
    limit = max(1, int(results_limit if results_limit is not None else RUTUBE_RESULTS_LIMIT))
    timeout = aiohttp.ClientTimeout(total=RUN_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        items: list[dict[str, Any]] = []
        page = 1
        total = ""
        while len(items) < limit:
            batch_limit = min(100, limit - len(items))
            payload = await _rutube_api_get(
                session,
                f"/video/person/{channel_id}/",
                {"page": page, "limit": batch_limit, "ordering": "-created_ts"},
            )
            if total == "":
                total = str(_parse_int(payload.get("count") or ""))
            results = payload.get("results") or []
            if not results:
                break
            items.extend(results)
            if not payload.get("has_next"):
                break
            page += 1
    resolved_total = str(_parse_int(total or ""))
    if not resolved_total or resolved_total == "0":
        resolved_total = str(len(items))
    return {
        "followers": "",
        "total_videos": resolved_total,
        "items": items,
    }


def _duration_to_seconds(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, (int, float)):
        return str(int(float(value)))
    s = str(value).strip()
    if not s:
        return ""
    if s.startswith("PT"):
        h = re.search(r"(\d+)H", s)
        m = re.search(r"(\d+)M", s)
        sec = re.search(r"(\d+)S", s)
        total = (int(h.group(1)) if h else 0) * 3600 + (int(m.group(1)) if m else 0) * 60 + (int(sec.group(1)) if sec else 0)
        return str(total)
    parts = s.split(":")
    try:
        nums = [int(x) for x in parts]
    except Exception:
        return s
    if len(nums) == 3:
        return str(nums[0] * 3600 + nums[1] * 60 + nums[2])
    if len(nums) == 2:
        return str(nums[0] * 60 + nums[1])
    return str(nums[0])


def _youtube_mentions(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                text = item.get("url") or item.get("text") or item.get("title") or ""
            else:
                text = str(item)
            text = (text or "").strip()
            if text:
                parts.append(text)
        return " | ".join(parts)[:3000]
    return str(value)[:3000]


def _age_snapshots(published_dt: datetime | None, views: int, likes: int, comments: int) -> tuple[str, str]:
    day1 = ""
    month1 = ""
    if published_dt:
        age = _now_msk() - published_dt.astimezone(MSK)
        if age >= timedelta(hours=20):
            day1 = _metrics_text(views, likes, comments)
        if age >= timedelta(days=28):
            month1 = _metrics_text(views, likes, comments)
    return day1, month1


def _extract_hashtags_from_text(text: str) -> str:
    if not text:
        return ""
    seen: list[str] = []
    for match in re.finditer(r"(?<!\w)(#[\wа-яА-ЯёЁ]+)", text):
        tag = match.group(1)
        if tag not in seen:
            seen.append(tag)
    return " | ".join(seen)[:2000]


def _extract_mentions_from_text(text: str) -> str:
    if not text:
        return ""
    seen: list[str] = []
    for match in re.finditer(r"(?<!\w)(@[\w\.\-]+)", text):
        mention = match.group(1)
        if mention not in seen:
            seen.append(mention)
    return " | ".join(seen)[:2000]


def _join_dict_texts(value: Any, keys: tuple[str, ...], limit: int = 5000) -> str:
    if not value:
        return ""
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, dict):
                for key in keys:
                    text = (item.get(key) or "").strip() if isinstance(item.get(key), str) else item.get(key)
                    if text:
                        parts.append(str(text).strip())
                        break
            elif str(item).strip():
                parts.append(str(item).strip())
        return " | ".join(parts)[:limit]
    return str(value)[:limit]


def map_instagram_item(item: dict, account_url: str, imported_at: str) -> ProjectVideoRecord | None:
    video_url = item.get("url") or item.get("inputUrl") or item.get("postUrl")
    if not video_url:
        shortcode = item.get("shortCode") or item.get("shortcode")
        if shortcode:
            video_url = f"https://www.instagram.com/reel/{shortcode}/"
    if not video_url:
        return None

    published_dt = _parse_dt(item.get("timestamp") or item.get("takenAtTimestamp") or item.get("publishedAt"))
    play_views = _parse_int(item.get("videoPlayCount") or item.get("videoViewCount") or item.get("playsCount") or item.get("viewsCount"))
    raw_views = _parse_int(item.get("videoViewCount") or item.get("videoPlayCount") or item.get("viewsCount") or item.get("playsCount"))
    likes = _parse_int(item.get("likesCount") or item.get("likes"))
    comments = _parse_int(item.get("commentsCount") or item.get("comments"))
    caption = _sanitize_caption(item.get("caption") or item.get("text") or "")
    first_comment = _sanitize_caption(item.get("firstComment") or "", 3000)
    latest_comments = _join_text_list(item.get("latestComments"), 5000)
    hashtags = _join_text_list(item.get("hashtags"), 2000)
    mentions = _join_text_list(item.get("mentions"), 2000)
    child_posts = _join_text_list(item.get("childPosts"), 5000)
    duration_seconds = round(float(item.get("videoDuration") or 0), 2)

    day1, month1 = _age_snapshots(published_dt, play_views, likes, comments)

    return ProjectVideoRecord(
        published_at=_format_dt_msk(published_dt),
        account_url=normalize_account_url(account_url, "instagram"),
        platform_label="Инстаграм",
        video_url=video_url,
        caption=caption,
        first_comment=first_comment,
        latest_comments=latest_comments,
        hashtags=hashtags,
        mentions=mentions,
        child_posts=child_posts,
        comments=comments,
        likes=likes,
        play_views=play_views,
        raw_views=raw_views,
        duration_seconds=duration_seconds,
        recheck_enabled=not (day1 and month1),
        day1_check=day1,
        month1_check=month1,
        imported_at=imported_at,
    )


def map_youtube_item(item: dict, account_url: str, imported_at: str) -> ProjectVideoRecord | None:
    video_url = item.get("url") or ""
    if not video_url and item.get("id"):
        video_url = f"https://www.youtube.com/watch?v={item['id']}"
    if not video_url:
        return None

    published_dt = _parse_dt(item.get("date") or item.get("publishedAt"))
    views = _parse_int(item.get("viewCount") or item.get("views") or item.get("videoViewCount"))
    likes = _parse_int(item.get("likes") or item.get("likeCount"))
    comments = _parse_int(item.get("commentsCount") or item.get("commentCount"))
    caption = _sanitize_caption(item.get("text") or item.get("title") or item.get("description") or "")
    hashtags = _join_text_list(item.get("hashtags") or item.get("tags"), 2000)
    mentions = _youtube_mentions(item.get("descriptionLinks"))
    duration_seconds = _duration_to_seconds(item.get("duration"))

    day1, month1 = _age_snapshots(published_dt, views, likes, comments)

    published_text = _format_dt_msk(published_dt) or str(item.get("date") or "")
    return ProjectVideoRecord(
        published_at=published_text,
        account_url=normalize_account_url(account_url, "youtube"),
        platform_label="Ютуб",
        video_url=video_url,
        caption=caption,
        first_comment="",
        latest_comments="",
        hashtags=hashtags,
        mentions=mentions,
        child_posts="",
        comments=comments,
        likes=likes,
        play_views=views,
        raw_views=views,
        duration_seconds=duration_seconds,
        recheck_enabled=not (day1 and month1),
        day1_check=day1,
        month1_check=month1,
        imported_at=imported_at,
    )


def map_tiktok_item(item: dict, account_url: str, imported_at: str) -> ProjectVideoRecord | None:
    video_id = str(item.get("id") or item.get("awemeId") or "").strip()
    author = item.get("authorMeta") or {}
    author_name = str(author.get("name") or author.get("nickName") or _tiktok_profile_name(account_url)).strip("@")
    video_url = item.get("webVideoUrl") or item.get("url") or ""
    if not video_url and video_id and author_name:
        video_url = f"https://www.tiktok.com/@{author_name}/video/{video_id}"
    if not video_url:
        return None

    published_dt = _parse_dt(item.get("createTimeISO") or item.get("createTime") or item.get("timestamp"))
    caption = _sanitize_caption(item.get("text") or item.get("desc") or "", 5000)
    hashtags = _join_dict_texts(item.get("hashtags") or item.get("challenges"), ("name", "title"), 2000) or _extract_hashtags_from_text(caption)
    mentions = _join_dict_texts(item.get("mentions"), ("name", "title"), 2000) or _extract_mentions_from_text(caption)
    comments = _parse_int(item.get("commentCount") or item.get("commentsCount"))
    likes = _parse_int(item.get("diggCount") or item.get("likes") or item.get("likeCount"))
    views = _parse_int(item.get("playCount") or item.get("videoPlayCount") or item.get("views"))
    duration_seconds = _duration_to_seconds((item.get("videoMeta") or {}).get("duration") or item.get("videoDuration"))
    day1, month1 = _age_snapshots(published_dt, views, likes, comments)

    return ProjectVideoRecord(
        published_at=_format_dt_msk(published_dt),
        account_url=normalize_account_url(account_url, "tiktok"),
        platform_label=platform_label("tiktok"),
        video_url=video_url,
        caption=caption,
        first_comment="",
        latest_comments="",
        hashtags=hashtags,
        mentions=mentions,
        child_posts="",
        comments=comments,
        likes=likes,
        play_views=views,
        raw_views=views,
        duration_seconds=duration_seconds,
        recheck_enabled=not (day1 and month1),
        day1_check=day1,
        month1_check=month1,
        imported_at=imported_at,
    )


def map_facebook_item(item: dict, account_url: str, imported_at: str) -> ProjectVideoRecord | None:
    video_url = item.get("url") or item.get("postUrl") or item.get("videoUrl") or ""
    if not video_url or not bool(item.get("isVideo")):
        return None

    published_dt = _parse_dt(item.get("time") or item.get("timestamp"))
    caption = _sanitize_caption(item.get("text") or item.get("description") or item.get("title") or "", 5000)
    comments = _parse_int(item.get("comments") or item.get("commentCount"))
    likes = _parse_int(item.get("likes") or item.get("reactions") or item.get("reactionCount"))
    views = _parse_int(item.get("views") or item.get("viewCount") or item.get("videoViewCount"))
    day1, month1 = _age_snapshots(published_dt, views, likes, comments)

    return ProjectVideoRecord(
        published_at=_format_dt_msk(published_dt),
        account_url=normalize_account_url(account_url, "facebook"),
        platform_label=platform_label("facebook"),
        video_url=video_url,
        caption=caption,
        first_comment="",
        latest_comments="",
        hashtags=_extract_hashtags_from_text(caption),
        mentions=_extract_mentions_from_text(caption),
        child_posts="",
        comments=comments,
        likes=likes,
        play_views=views,
        raw_views=views,
        duration_seconds=_duration_to_seconds(item.get("duration")),
        recheck_enabled=not (day1 and month1),
        day1_check=day1,
        month1_check=month1,
        imported_at=imported_at,
    )


def map_vk_item(item: dict, account_url: str, imported_at: str) -> ProjectVideoRecord | None:
    owner_id = item.get("owner_id")
    video_id = item.get("id")
    if owner_id in (None, "") or video_id in (None, ""):
        return None
    video_url = item.get("player") or f"https://vk.com/video{owner_id}_{video_id}"
    published_dt = _parse_dt(item.get("date") or item.get("adding_date"))
    source_text = " ".join(part for part in [item.get("title") or "", item.get("description") or ""] if part).strip()
    caption = _sanitize_caption(source_text, 5000)
    comments = _parse_int(item.get("comments") or item.get("comments_count"))
    likes = _parse_int(((item.get("likes") or {}).get("count")) if isinstance(item.get("likes"), dict) else item.get("likes_count"))
    views = _parse_int(((item.get("views") or {}).get("count")) if isinstance(item.get("views"), dict) else item.get("views"))
    duration_seconds = _duration_to_seconds(item.get("duration"))
    day1, month1 = _age_snapshots(published_dt, views, likes, comments)

    return ProjectVideoRecord(
        published_at=_format_dt_msk(published_dt),
        account_url=normalize_account_url(account_url, "vk"),
        platform_label=platform_label("vk"),
        video_url=video_url,
        caption=caption,
        first_comment="",
        latest_comments="",
        hashtags=_extract_hashtags_from_text(source_text),
        mentions=_extract_mentions_from_text(source_text),
        child_posts="",
        comments=comments,
        likes=likes,
        play_views=views,
        raw_views=views,
        duration_seconds=duration_seconds,
        recheck_enabled=not (day1 and month1),
        day1_check=day1,
        month1_check=month1,
        imported_at=imported_at,
    )


def map_rutube_item(item: dict, account_url: str, imported_at: str) -> ProjectVideoRecord | None:
    video_url = item.get("video_url") or ""
    if not video_url:
        uuid = item.get("id") or item.get("uuid")
        if uuid:
            video_url = f"https://rutube.ru/video/{uuid}/"
    if not video_url:
        return None

    published_dt = _parse_dt(item.get("created_ts") or item.get("created") or item.get("publication_ts"))
    source_text = " ".join(part for part in [item.get("title") or "", item.get("description") or ""] if part).strip()
    caption = _sanitize_caption(source_text, 5000)
    comments = _parse_int(item.get("comments_count"))
    likes = _parse_int(item.get("likes") or item.get("likes_count"))
    views = _parse_int(item.get("hits") or item.get("views"))
    duration_seconds = _duration_to_seconds(item.get("duration"))
    day1, month1 = _age_snapshots(published_dt, views, likes, comments)

    return ProjectVideoRecord(
        published_at=_format_dt_msk(published_dt),
        account_url=normalize_account_url(account_url, "rutube"),
        platform_label=platform_label("rutube"),
        video_url=video_url,
        caption=caption,
        first_comment="",
        latest_comments="",
        hashtags=_extract_hashtags_from_text(source_text),
        mentions=_extract_mentions_from_text(source_text),
        child_posts="",
        comments=comments,
        likes=likes,
        play_views=views,
        raw_views=views,
        duration_seconds=duration_seconds,
        recheck_enabled=not (day1 and month1),
        day1_check=day1,
        month1_check=month1,
        imported_at=imported_at,
    )


def get_project_sheets(client_config: dict):
    gc = get_client()
    sh = gc.open_by_key(client_config["spreadsheet_id"])
    admin_ws = sh.worksheet(client_config.get("project_admin_sheet", DEFAULT_PROJECT_ADMIN_SHEET))
    videos_ws = sh.worksheet(client_config.get("project_videos_sheet", DEFAULT_PROJECT_VIDEOS_SHEET))
    history_ws = ensure_sheet(sh, client_config.get("project_history_sheet", DEFAULT_PROJECT_HISTORY_SHEET), HISTORY_HEADERS, cols=10)
    return sh, admin_ws, videos_ws, history_ws


def _empty_daily_snapshot() -> ProjectDailySnapshot:
    return ProjectDailySnapshot(date=_today_full_date())


def _incremental_since_msk() -> datetime:
    yesterday = (_now_msk() - timedelta(days=1)).date()
    return datetime.combine(yesterday, datetime.min.time(), tzinfo=MSK)


def _parse_record_published_at(value: str) -> datetime | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%d.%m.%Y %H:%M").replace(tzinfo=MSK)
    except Exception:
        return _parse_dt(text)


def _needs_full_backfill(account: ProjectAccountRow) -> bool:
    return not bool((account.last_full_import_at or "").strip())


def _fetch_limit(account: ProjectAccountRow) -> int:
    if _needs_full_backfill(account):
        return FULL_BACKFILL_RESULTS_LIMIT
    if account.platform == "instagram":
        return INSTAGRAM_RESULTS_LIMIT
    if account.platform == "youtube":
        return YOUTUBE_RESULTS_LIMIT
    if account.platform == "tiktok":
        return TIKTOK_RESULTS_LIMIT
    if account.platform == "facebook":
        return FACEBOOK_RESULTS_LIMIT
    if account.platform == "vk":
        return VK_RESULTS_LIMIT
    if account.platform == "rutube":
        return RUTUBE_RESULTS_LIMIT
    return FULL_BACKFILL_RESULTS_LIMIT


def upsert_daily_snapshot(history_ws, snapshot: ProjectDailySnapshot):
    rows = _sheet_call(history_ws.get, f"A{HISTORY_DATA_START_ROW}:I{history_ws.row_count}")
    target_row_idx = None
    existing_row: list[str] | None = None
    for offset, row in enumerate(rows, start=HISTORY_DATA_START_ROW):
        row_date = (row[0] if len(row) > 0 else "").strip()
        if row_date == snapshot.date:
            target_row_idx = offset
            existing_row = row
            break

    merged_row = snapshot.as_row(existing_row)
    if target_row_idx is None:
        _sheet_call(history_ws.append_row, merged_row, value_input_option="USER_ENTERED")
        return

    _sheet_call(
        history_ws.update,
        f"A{target_row_idx}:I{target_row_idx}",
        [merged_row],
        value_input_option="USER_ENTERED",
    )


def read_admin_rows(admin_ws) -> tuple[bool, list[ProjectAccountRow]]:
    values = _sheet_call(admin_ws.get, "A1:N200")
    master_enabled = False
    if len(values) >= 2 and len(values[1]) >= 2:
        master_enabled = is_triggered(values[1][1])

    accounts: list[ProjectAccountRow] = []
    for row_idx, row in enumerate(values[1:], start=2):
        url = (row[0] if len(row) > 0 else "").strip()
        if not url:
            continue
        platform = canonical_platform_name(detect_platform(url))
        if not platform or platform in DISABLED_PROJECT_PLATFORMS:
            continue
        accounts.append(
            ProjectAccountRow(
                row_idx=row_idx,
                account_url=normalize_account_url(url, platform),
                platform=platform,
                followers=(row[2] if len(row) > 2 else "").strip(),
                total_videos=(row[3] if len(row) > 3 else "").strip(),
                last_checked_at=(row[4] if len(row) > 4 else "").strip(),
                last_full_import_at=(row[5] if len(row) > 5 else "").strip(),
                new_videos_count=(row[6] if len(row) > 6 else "").strip(),
                status=(row[7] if len(row) > 7 else "").strip(),
            )
        )
    return master_enabled, accounts


def read_existing_video_keys(videos_ws) -> set[tuple[str, str]]:
    rows = _sheet_call(videos_ws.get, f"C{VIDEO_DATA_START_ROW}:D{videos_ws.row_count}")
    existing: set[tuple[str, str]] = set()
    for row in rows:
        platform = (row[0] if len(row) > 0 else "").strip().lower()
        video_url = (row[1] if len(row) > 1 else "").strip()
        if not platform or not video_url:
            continue
        canonical_platform = canonical_platform_name(platform)
        existing.add((canonical_platform, normalize_video_url(video_url, canonical_platform)))
    return existing


def _find_next_project_video_row(videos_ws) -> int:
    last_row = max(int(getattr(videos_ws, "row_count", DEFAULT_ROWS) or DEFAULT_ROWS), VIDEO_DATA_START_ROW)
    rows = _sheet_call(videos_ws.get, f"A{VIDEO_DATA_START_ROW}:A{last_row}")
    for idx in range(len(rows) - 1, -1, -1):
        row = rows[idx] or []
        if row and str(row[0] or "").strip():
            return VIDEO_DATA_START_ROW + idx + 1
    return VIDEO_DATA_START_ROW


def append_video_rows(videos_ws, records: list[ProjectVideoRecord]) -> int:
    if not records:
        return 0
    values = []
    for rec in records:
        values.append([
            rec.published_at,
            rec.account_url,
            rec.platform_label,
            rec.video_url,
            rec.caption,
            rec.first_comment,
            rec.latest_comments,
            rec.hashtags,
            rec.mentions,
            rec.child_posts,
            rec.comments,
            rec.likes,
            rec.play_views,
            rec.raw_views,
            rec.duration_seconds,
            "TRUE" if rec.recheck_enabled else "FALSE",
            rec.day1_check,
            rec.month1_check,
            rec.imported_at,
        ])

    start_row = _find_next_project_video_row(videos_ws)
    end_row = start_row + len(values) - 1
    required_rows = end_row - int(getattr(videos_ws, "row_count", 0) or 0)
    if required_rows > 0:
        _sheet_call(videos_ws.add_rows, required_rows)
    _sheet_call(
        videos_ws.update,
        f"A{start_row}:S{end_row}",
        values,
        value_input_option="USER_ENTERED",
    )
    return len(values)


def format_project_video_summary(row: list[str]) -> str:
    parts: list[str] = []
    for idx, label in PROJECT_VIDEO_SUMMARY_FIELDS:
        value = (row[idx] if idx < len(row) else "") or ""
        value = str(value).strip()
        if not value:
            continue
        parts.append(f"{label}: {value}")
    return "\n\n".join(parts)


def build_project_video_summary_map(rows: list[list[str]]) -> dict[str, str]:
    summary_map: dict[str, str] = {}
    for row in rows:
        platform_value = (row[2] if len(row) > 2 else "") or ""
        video_url = (row[3] if len(row) > 3 else "") or ""
        platform = canonical_platform_name(platform_value or detect_platform(video_url))
        if platform != "instagram":
            continue
        key = normalize_video_url(video_url, platform)
        if not key:
            continue
        summary = format_project_video_summary(row)
        if not summary:
            continue
        summary_map[key] = summary
    return summary_map


def extract_missing_main_sheet_instagram_urls(rows: list[list[str]], existing_keys: set[tuple[str, str]]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for row in rows:
        reel_url = (row[0] if len(row) > 0 else "") or ""
        if not reel_url:
            continue
        platform = canonical_platform_name(detect_platform(reel_url))
        if platform != "instagram":
            continue
        normalized_url = normalize_video_url(reel_url, platform)
        if not normalized_url:
            continue
        if ("instagram", normalized_url) in existing_keys:
            continue
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        urls.append(normalized_url)
        if len(urls) >= INSTAGRAM_MAIN_SHEET_EXTRA_URLS_LIMIT:
            break
    return urls


def read_missing_main_sheet_instagram_urls(sh, client_config: dict, existing_keys: set[tuple[str, str]]) -> list[str]:
    main_sheet_name = client_config.get("sheet_name")
    if not main_sheet_name:
        return []
    try:
        main_ws = sh.worksheet(main_sheet_name)
    except Exception as exc:
        log.warning("[%s] main sheet '%s' unavailable for BL read: %s", client_config.get("name"), main_sheet_name, exc)
        return []
    if int(getattr(main_ws, "col_count", 0) or 0) < 64:
        return []
    rows = _sheet_call(main_ws.get, f"BL{MAIN_SHEET_DATA_START_ROW}:BL{main_ws.row_count}")
    return extract_missing_main_sheet_instagram_urls(rows, existing_keys)


def prepare_main_sheet_bm_updates(rows: list[list[str]], summary_map: dict[str, str], start_row: int = MAIN_SHEET_DATA_START_ROW) -> list[dict[str, list[list[str]]]]:
    updates: list[dict[str, list[list[str]]]] = []
    for offset, row in enumerate(rows, start=start_row):
        reel_url = (row[0] if len(row) > 0 else "") or ""
        current_summary = (row[1] if len(row) > 1 else "") or ""
        if not reel_url:
            continue
        platform = canonical_platform_name(detect_platform(reel_url))
        if platform != "instagram":
            continue
        normalized_url = normalize_video_url(reel_url, platform)
        if not normalized_url:
            continue
        new_summary = summary_map.get(normalized_url)
        if not new_summary or new_summary == str(current_summary).strip():
            continue
        updates.append({"range": f"BM{offset}:BM{offset}", "values": [[new_summary]]})
    return updates


def sync_main_sheet_publication_metrics(sh, client_config: dict, videos_ws) -> dict[str, int]:
    main_sheet_name = client_config.get("sheet_name")
    if not main_sheet_name:
        return {"matched": 0, "updated": 0, "candidates": 0}

    try:
        main_ws = sh.worksheet(main_sheet_name)
    except Exception as exc:
        log.warning("[%s] main sheet '%s' unavailable for BM sync: %s", client_config.get("name"), main_sheet_name, exc)
        return {"matched": 0, "updated": 0, "candidates": 0}

    if int(getattr(main_ws, "col_count", 0) or 0) < 65:
        return {"matched": 0, "updated": 0, "candidates": 0}

    video_rows = _sheet_call(videos_ws.get, f"A{VIDEO_DATA_START_ROW}:S{videos_ws.row_count}")
    summary_map = build_project_video_summary_map(video_rows)
    if not summary_map:
        return {"matched": 0, "updated": 0, "candidates": 0}

    main_rows = _sheet_call(main_ws.get, f"BL{MAIN_SHEET_DATA_START_ROW}:BM{main_ws.row_count}")
    updates = prepare_main_sheet_bm_updates(main_rows, summary_map, start_row=MAIN_SHEET_DATA_START_ROW)
    for i in range(0, len(updates), 200):
        chunk = updates[i:i + 200]
        _sheet_call(main_ws.batch_update, chunk, value_input_option="USER_ENTERED")

    candidates = sum(1 for row in main_rows if row and (row[0] if len(row) > 0 else "").strip())
    matched = 0
    for row in main_rows:
        reel_url = (row[0] if len(row) > 0 else "") or ""
        if not reel_url:
            continue
        platform = canonical_platform_name(detect_platform(reel_url))
        if platform != "instagram":
            continue
        normalized_url = normalize_video_url(reel_url, platform)
        if normalized_url in summary_map:
            matched += 1

    return {"matched": matched, "updated": len(updates), "candidates": candidates}


def update_admin_row(
    admin_ws,
    account: ProjectAccountRow,
    followers: str,
    total_videos: str,
    new_count: int,
    status: str,
    *,
    full_import_at: str | None = None,
):
    now_str = _now_str()
    today_label = _today_label()
    full_import = full_import_at if full_import_at is not None else (account.last_full_import_at or "")
    main_values = [[followers, total_videos, now_str, full_import, str(new_count), status]]
    if account.platform == "instagram":
        split_values = [[followers, total_videos, "", ""]]
    elif account.platform == "youtube":
        split_values = [["", "", followers, total_videos]]
    else:
        split_values = [["", "", "", ""]]

    _sheet_call(
        admin_ws.batch_update,
        [
            {"range": f"C{account.row_idx}:H{account.row_idx}", "values": main_values},
            {"range": f"I{account.row_idx}:I{account.row_idx}", "values": [[today_label]]},
            {"range": f"K{account.row_idx}:N{account.row_idx}", "values": split_values},
        ],
        value_input_option="USER_ENTERED",
    )


async def sync_project_account(
    admin_ws,
    videos_ws,
    account: ProjectAccountRow,
    existing_keys: set[tuple[str, str]],
    *,
    forced_video_urls: list[str] | None = None,
    prefetched_instagram_payload: dict[str, Any] | None = None,
    prefetched_tiktok_payload: dict[str, Any] | None = None,
    prefetched_facebook_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    imported_at = _now_str()
    sync_mode = "full_backfill" if _needs_full_backfill(account) else "incremental"
    if account.platform in UNSUPPORTED_PLATFORM_REASONS:
        reason = UNSUPPORTED_PLATFORM_REASONS[account.platform]
        status = f"SKIPPED: {reason[:100]}"
        update_admin_row(admin_ws, account, account.followers, account.total_videos, 0, status)
        return {
            "platform": account.platform,
            "new": 0,
            "followers": account.followers,
            "total_videos": account.total_videos,
            "status": status,
            "sync_mode": sync_mode,
            "skipped_reason": reason,
            "incremental_supported": False,
        }

    limit = _fetch_limit(account)
    forced_video_keys = {
        ("instagram", normalize_video_url(url, "instagram"))
        for url in (forced_video_urls or [])
        if normalize_video_url(url, "instagram")
    }
    if account.platform == "instagram":
        payload = prefetched_instagram_payload if prefetched_instagram_payload is not None else await fetch_instagram_account(
            account.account_url,
            results_limit=limit,
            extra_direct_urls=forced_video_urls,
        )
        mapper = map_instagram_item
    elif account.platform == "youtube":
        payload = await fetch_youtube_account(account.account_url, results_limit=limit)
        mapper = map_youtube_item
    elif account.platform == "tiktok":
        payload = prefetched_tiktok_payload if prefetched_tiktok_payload is not None else await fetch_tiktok_account(account.account_url, results_limit=limit)
        mapper = map_tiktok_item
    elif account.platform == "facebook":
        payload = prefetched_facebook_payload if prefetched_facebook_payload is not None else await fetch_facebook_account(account.account_url, results_limit=limit)
        mapper = map_facebook_item
    elif account.platform == "vk":
        payload = await fetch_vk_account(account.account_url, results_limit=limit)
        mapper = map_vk_item
    elif account.platform == "rutube":
        payload = await fetch_rutube_account(account.account_url, results_limit=limit)
        mapper = map_rutube_item
    else:
        status = "UNSUPPORTED"
        update_admin_row(admin_ws, account, account.followers, account.total_videos, 0, status)
        return {
            "platform": account.platform,
            "new": 0,
            "followers": account.followers,
            "total_videos": account.total_videos,
            "status": status,
            "sync_mode": sync_mode,
            "incremental_supported": False,
        }

    followers = payload.get("followers", "")
    total_videos = payload.get("total_videos", "")
    incremental_floor = _incremental_since_msk() if sync_mode == "incremental" else None
    new_records: list[ProjectVideoRecord] = []
    for item in payload.get("items", []):
        rec = mapper(item, account.account_url, imported_at)
        if not rec:
            continue
        key = rec.unique_key()
        published_dt = _parse_record_published_at(rec.published_at)
        if incremental_floor and key not in forced_video_keys and published_dt and published_dt.astimezone(MSK) < incremental_floor:
            continue
        if key in existing_keys:
            continue
        existing_keys.add(key)
        new_records.append(rec)

    added = append_video_rows(videos_ws, new_records)
    full_import_at = imported_at if sync_mode == "full_backfill" else None
    if sync_mode == "full_backfill":
        status = "FULL_BACKFILL_DONE" if added else "FULL_BACKFILL_NO_NEW"
    else:
        status = "INCREMENTAL_SYNC" if added else "NO_CHANGES"
    update_admin_row(admin_ws, account, followers, total_videos, added, status, full_import_at=full_import_at)
    return {
        "platform": account.platform,
        "new": added,
        "followers": followers,
        "total_videos": total_videos,
        "status": status,
        "checked_at": imported_at,
        "sync_mode": sync_mode,
        "skipped_reason": "",
        "incremental_supported": True,
    }


async def process_client_project_content(client_config: dict) -> dict[str, Any]:
    sh, admin_ws, videos_ws, history_ws = get_project_sheets(client_config)
    master_enabled, accounts = read_admin_rows(admin_ws)
    if not master_enabled:
        log.info("[%s] project content sync skipped: %s is off", client_config.get("name"), MASTER_TRIGGER_CELL)
        return {
            "client": client_config.get("name"),
            "enabled": False,
            "accounts": 0,
            "new_videos": 0,
            "statuses": [],
        }

    existing_keys = read_existing_video_keys(videos_ws)
    missing_main_sheet_instagram_urls = read_missing_main_sheet_instagram_urls(sh, client_config, existing_keys)
    rows_before = len(existing_keys)
    statuses = []
    new_videos = 0
    inserted_by_platform: dict[str, int] = {}
    skipped_platforms: dict[str, str] = {}
    incremental_support: dict[str, bool] = {}
    snapshot = _empty_daily_snapshot()
    for account in accounts:
        try:
            result = await sync_project_account(
                admin_ws,
                videos_ws,
                account,
                existing_keys,
                forced_video_urls=missing_main_sheet_instagram_urls if account.platform == "instagram" else None,
            )
            statuses.append(f"{result['platform']}:{result['status']}")
            new_videos += int(result.get("new") or 0)
            inserted_by_platform[result["platform"]] = inserted_by_platform.get(result["platform"], 0) + int(result.get("new") or 0)
            incremental_support[result["platform"]] = bool(result.get("incremental_supported"))
            if result.get("skipped_reason"):
                skipped_platforms[result["platform"]] = str(result["skipped_reason"])
            if result["platform"] == "instagram":
                snapshot.instagram_followers = str(result.get("followers") or "")
                snapshot.instagram_videos = str(result.get("total_videos") or "")
                snapshot.instagram_checked_at = str(result.get("checked_at") or "")
                snapshot.instagram_status = str(result.get("status") or "")
            elif result["platform"] == "youtube":
                snapshot.youtube_followers = str(result.get("followers") or "")
                snapshot.youtube_videos = str(result.get("total_videos") or "")
                snapshot.youtube_checked_at = str(result.get("checked_at") or "")
                snapshot.youtube_status = str(result.get("status") or "")
        except Exception as e:
            status = f"ERROR: {str(e)[:120]}"
            update_admin_row(admin_ws, account, account.followers, account.total_videos, 0, status)
            statuses.append(f"{account.platform}:ERROR")
            skipped_platforms[account.platform] = str(e)[:200]
            incremental_support[account.platform] = False
            if account.platform == "instagram":
                snapshot.instagram_status = status
            elif account.platform == "youtube":
                snapshot.youtube_status = status
            log.exception("[%s] project sync failed for row %s: %s", client_config.get("name"), account.row_idx, e)

    upsert_daily_snapshot(history_ws, snapshot)
    bm_sync = sync_main_sheet_publication_metrics(sh, client_config, videos_ws)

    return {
        "client": client_config.get("name"),
        "enabled": True,
        "accounts": len(accounts),
        "new_videos": new_videos,
        "statuses": statuses,
        "inserted_by_platform": inserted_by_platform,
        "skipped_platforms": skipped_platforms,
        "incremental_support": incremental_support,
        "rows_before": rows_before,
        "rows_after": rows_before + new_videos,
        "main_sheet_bm_candidates": bm_sync.get("candidates", 0),
        "main_sheet_bm_matched": bm_sync.get("matched", 0),
        "main_sheet_bm_updated": bm_sync.get("updated", 0),
    }


async def process_project_content_clients(client_key: str | None = None) -> list[dict[str, Any]]:
    results = []
    for client in load_clients_config():
        if client_key and client.get("_key") != client_key:
            continue
        try:
            result = await process_client_project_content(client)
            results.append(result)
        except Exception as e:
            if DEFAULT_PROJECT_ADMIN_SHEET in str(e) or DEFAULT_PROJECT_VIDEOS_SHEET in str(e):
                log.info("[%s] project content sheets not configured, skip", client.get("name"))
                continue
            raise
    return results


def fetch_youtube_video_snapshot(video_url: str) -> dict[str, Any] | None:
    try:
        res = subprocess.run(
            ["yt-dlp", "-J", "--no-playlist", normalize_video_url(video_url, "youtube")],
            capture_output=True,
            text=True,
            timeout=180,
        )
        if res.returncode != 0:
            return None
        import json
        return json.loads(res.stdout)
    except Exception:
        return None
