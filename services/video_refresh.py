"""
Per-video metric refresh fetchers.
Each function returns {"views": int, "likes": int, "comments": int} or None on failure.

Platform routing:
  youtube   — YouTube Data API (free, batches up to 50 IDs)
  vk        — VK API video.get
  rutube    — public rutube.ru/api/video/{uuid}/
  ok        — HTML scrape ok.ru/video/{id}
  pinterest — JSON-LD scrape pinterest.com/pin/{id}
  instagram / tiktok / facebook — Apify (batched, requires working token)
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Any
from urllib.parse import urlparse, parse_qs

import aiohttp
import requests

from services.competitor_pipeline import _parse_int
from services.project_content_pipeline import (
    _load_extra_env,
    _apify_run,
    INSTAGRAM_ACTOR_ID,
    TIKTOK_POST_APIFY_ACTOR_ID,
    FACEBOOK_POSTS_APIFY_ACTOR_ID,
    RUN_TIMEOUT_SECONDS,
    YOUTUBE_API_BASE_URL,
    VK_API_BASE_URL,
    RUTUBE_API_BASE_URL,
)

log = logging.getLogger("scout.video_refresh")

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _youtube_api_key() -> str:
    _load_extra_env()
    return (os.getenv("YOUTUBE_API_KEY") or "").strip()


def _vk_token() -> str:
    _load_extra_env()
    return (os.getenv("VK_API_TOKEN") or "").strip()


def _extract_youtube_id(url: str) -> str | None:
    parsed = urlparse(url or "")
    host = parsed.netloc.lower()
    if "youtu.be" in host:
        return parsed.path.strip("/") or None
    if "youtube.com" in host:
        qs = parse_qs(parsed.query)
        vid = (qs.get("v") or [""])[0]
        if vid:
            return vid
        m = re.search(r"/shorts/([^/?#]+)", parsed.path)
        if m:
            return m.group(1)
    return None


def _extract_vk_video_id(url: str) -> str | None:
    """Return 'owner_id_video_id' string for VK API, e.g. '-123456_789'."""
    path = urlparse(url or "").path.rstrip("/")
    m = re.search(r"(video-?\d+_\d+)", path, re.I)
    return m.group(1).lower() if m else None


def _extract_rutube_uuid(url: str) -> str | None:
    m = re.search(r"/video/([0-9a-f\-]{32,36})", url or "", re.I)
    return m.group(1) if m else None


def _extract_ok_video_id(url: str) -> str | None:
    m = re.search(r"ok\.ru/video/(\d+)", url or "", re.I)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# YouTube
# ---------------------------------------------------------------------------

async def refresh_youtube_videos(urls: list[str]) -> dict[str, dict]:
    """Batch-refresh up to 50 YouTube video URLs. Returns {url: metrics}."""
    api_key = _youtube_api_key()
    if not api_key:
        log.warning("YOUTUBE_API_KEY not set, skipping YouTube refresh")
        return {}

    id_to_url: dict[str, str] = {}
    for url in urls:
        vid = _extract_youtube_id(url)
        if vid:
            id_to_url[vid] = url

    if not id_to_url:
        return {}

    timeout = aiohttp.ClientTimeout(total=30)
    result: dict[str, dict] = {}
    video_ids = list(id_to_url.keys())

    async with aiohttp.ClientSession(timeout=timeout) as session:
        for i in range(0, len(video_ids), 50):
            chunk = video_ids[i:i + 50]
            params = {
                "part": "statistics",
                "id": ",".join(chunk),
                "key": api_key,
            }
            try:
                async with session.get(f"{YOUTUBE_API_BASE_URL}/videos", params=params) as resp:
                    data = await resp.json()
                for item in (data.get("items") or []):
                    vid = item.get("id")
                    stats = item.get("statistics") or {}
                    if vid and vid in id_to_url:
                        result[id_to_url[vid]] = {
                            "views": _parse_int(stats.get("viewCount")),
                            "likes": _parse_int(stats.get("likeCount")),
                            "comments": _parse_int(stats.get("commentCount")),
                        }
            except Exception as exc:
                log.warning("YouTube batch refresh failed for chunk %s: %s", chunk[:3], exc)

    return result


# ---------------------------------------------------------------------------
# VK
# ---------------------------------------------------------------------------

async def refresh_vk_videos(urls: list[str]) -> dict[str, dict]:
    """Batch-refresh VK video URLs using video.get (up to 200 per call)."""
    token = _vk_token()
    if not token:
        log.warning("VK_API_TOKEN not set, skipping VK refresh")
        return {}

    id_to_url: dict[str, str] = {}
    for url in urls:
        vid = _extract_vk_video_id(url)
        if vid:
            id_to_url[vid] = url

    if not id_to_url:
        return {}

    result: dict[str, dict] = {}
    video_ids = list(id_to_url.keys())
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        for i in range(0, len(video_ids), 200):
            chunk = video_ids[i:i + 200]
            params = {
                "videos": ",".join(chunk),
                "access_token": token,
                "v": "5.131",
            }
            try:
                async with session.get(f"{VK_API_BASE_URL}/video.get", params=params) as resp:
                    data = await resp.json()
                items = ((data or {}).get("response") or {}).get("items") or []
                for item in items:
                    owner_id = item.get("owner_id")
                    video_id = item.get("id")
                    if owner_id is None or video_id is None:
                        continue
                    key = f"video{owner_id}_{video_id}"
                    if key not in id_to_url:
                        key = f"video-{abs(int(owner_id))}_{video_id}"
                    if key in id_to_url:
                        views_raw = item.get("views") or {}
                        likes_raw = item.get("likes") or {}
                        comments_raw = item.get("comments") or {}
                        result[id_to_url[key]] = {
                            "views": _parse_int(views_raw.get("count") if isinstance(views_raw, dict) else views_raw),
                            "likes": _parse_int(likes_raw.get("count") if isinstance(likes_raw, dict) else likes_raw),
                            "comments": _parse_int(comments_raw.get("count") if isinstance(comments_raw, dict) else comments_raw),
                        }
            except Exception as exc:
                log.warning("VK batch refresh failed: %s", exc)

    return result


# ---------------------------------------------------------------------------
# Rutube
# ---------------------------------------------------------------------------

async def refresh_rutube_videos(urls: list[str]) -> dict[str, dict]:
    """Refresh Rutube videos one by one (no batch API)."""
    result: dict[str, dict] = {}
    for url in urls:
        uuid = _extract_rutube_uuid(url)
        if not uuid:
            continue
        try:
            def _fetch():
                resp = requests.get(
                    f"{RUTUBE_API_BASE_URL}/video/{uuid}/",
                    headers={"User-Agent": _BROWSER_UA, "Accept-Encoding": "identity"},
                    timeout=20,
                )
                resp.raise_for_status()
                return resp.json()

            data = await asyncio.to_thread(_fetch)
            result[url] = {
                "views": _parse_int(data.get("hits") or data.get("views")),
                "likes": _parse_int(data.get("likes") or data.get("likes_count")),
                "comments": _parse_int(data.get("comments_count")),
            }
        except Exception as exc:
            log.warning("Rutube refresh failed for %s: %s", url, exc)

    return result


# ---------------------------------------------------------------------------
# OK.ru
# ---------------------------------------------------------------------------

async def refresh_ok_videos(urls: list[str]) -> dict[str, dict]:
    """Refresh OK.ru video pages via HTML scraping."""
    result: dict[str, dict] = {}
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": _BROWSER_UA}) as session:
        for url in urls:
            try:
                async with session.get(url, allow_redirects=True) as resp:
                    html = await resp.text(errors="replace")

                views = 0
                likes = 0
                comments = 0

                # Try og: meta tags
                m = re.search(r'<meta[^>]+property="og:video:views_count"[^>]+content="(\d+)"', html, re.I)
                if m:
                    views = int(m.group(1))

                # Try JSON blobs in script tags (OK often puts __DATA__ or __serializeData__)
                for pattern in (r'__DATA__\s*=\s*(\{.*?\});', r'__serializeData__\s*=\s*(\{.*?\})'):
                    json_m = re.search(pattern, html, re.S)
                    if json_m:
                        try:
                            import json
                            blob = json.loads(json_m.group(1))
                            # Try to navigate to video stats
                            def _deep_get(obj, *keys):
                                for k in keys:
                                    if not isinstance(obj, dict):
                                        return None
                                    obj = obj.get(k)
                                return obj
                            views = views or _parse_int(_deep_get(blob, "videoInfo", "viewsCount") or
                                                        _deep_get(blob, "video", "viewsCount") or 0)
                            likes = _parse_int(_deep_get(blob, "videoInfo", "likeCount") or
                                               _deep_get(blob, "video", "likeCount") or 0)
                            comments = _parse_int(_deep_get(blob, "videoInfo", "discussionSummary", "totalCount") or 0)
                        except Exception:
                            pass
                        break

                if views or likes or comments:
                    result[url] = {"views": views, "likes": likes, "comments": comments}
                else:
                    log.debug("OK: no metrics extracted for %s", url)

            except Exception as exc:
                log.warning("OK refresh failed for %s: %s", url, exc)

    return result


# ---------------------------------------------------------------------------
# Pinterest
# ---------------------------------------------------------------------------

async def refresh_pinterest_videos(urls: list[str]) -> dict[str, dict]:
    """Refresh Pinterest pins via JSON-LD / __PWS_INITIAL_PROPS__ scraping."""
    result: dict[str, dict] = {}
    timeout = aiohttp.ClientTimeout(total=30)
    headers = {"User-Agent": _BROWSER_UA, "Accept-Language": "en-US,en;q=0.9"}

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        for url in urls:
            try:
                async with session.get(url, allow_redirects=True) as resp:
                    html = await resp.text(errors="replace")

                saves = 0

                # Try JSON-LD
                ld_m = re.search(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S | re.I)
                if ld_m:
                    try:
                        import json
                        ld = json.loads(ld_m.group(1))
                        stats = ld.get("interactionStatistic") or []
                        if isinstance(stats, dict):
                            stats = [stats]
                        for stat in stats:
                            if "Save" in str(stat.get("interactionType", {})) or "save" in str(stat.get("@type", "")).lower():
                                saves = _parse_int(stat.get("userInteractionCount", 0))
                    except Exception:
                        pass

                # Try __PWS_INITIAL_PROPS__
                if not saves:
                    pws_m = re.search(r'id="__PWS_INITIAL_PROPS__"[^>]*>(.*?)</script>', html, re.S)
                    if pws_m:
                        try:
                            import json
                            blob = json.loads(pws_m.group(1))
                            pin = ((blob.get("initialReduxState") or {}).get("pins") or {})
                            if pin:
                                first_pin = next(iter(pin.values()), {})
                                saves = _parse_int(first_pin.get("aggregated_pin_data", {}).get("saves", 0))
                        except Exception:
                            pass

                result[url] = {"views": 0, "likes": saves, "comments": 0}

            except Exception as exc:
                log.warning("Pinterest refresh failed for %s: %s", url, exc)

    return result


# ---------------------------------------------------------------------------
# Apify-based (Instagram, TikTok, Facebook)
# ---------------------------------------------------------------------------

async def refresh_instagram_videos(urls: list[str]) -> dict[str, dict]:
    """Refresh Instagram reels/posts via Apify (batch, charges per result)."""
    _load_extra_env()
    if not urls:
        return {}
    try:
        items = await _apify_run(
            INSTAGRAM_ACTOR_ID,
            {
                "directUrls": urls,
                "resultsType": "posts",
                "resultsLimit": len(urls),
            },
        )
    except Exception as exc:
        log.warning("Instagram Apify refresh failed: %s", exc)
        return {}

    result: dict[str, dict] = {}
    for item in items:
        raw_url = item.get("url") or item.get("postUrl") or ""
        shortcode = item.get("shortCode") or item.get("shortcode") or ""
        if not raw_url and shortcode:
            raw_url = f"https://www.instagram.com/p/{shortcode}/"
        # Normalize to match canonical form
        from services.project_content_pipeline import normalize_video_url
        norm = normalize_video_url(raw_url, "instagram")
        if not norm:
            continue
        # Match against input urls (also normalized)
        for in_url in urls:
            if normalize_video_url(in_url, "instagram") == norm:
                result[in_url] = {
                    "views": _parse_int(item.get("videoPlayCount") or item.get("videoViewCount")),
                    "likes": _parse_int(item.get("likesCount")),
                    "comments": _parse_int(item.get("commentsCount")),
                }
                break
    return result


async def refresh_tiktok_videos(urls: list[str]) -> dict[str, dict]:
    """Refresh TikTok videos via Apify — single batch run for all URLs."""
    _load_extra_env()
    if not urls:
        return {}
    try:
        items = await _apify_run(
            TIKTOK_POST_APIFY_ACTOR_ID,
            {"postURLs": urls, "resultsPerPage": len(urls)},
        )
    except Exception as exc:
        log.warning("TikTok Apify refresh failed: %s", exc)
        return {}

    result: dict[str, dict] = {}
    for item in items:
        url = item.get("webVideoUrl") or item.get("url") or ""
        matched = url if url in urls else next((u for u in urls if u.rstrip("/") == url.rstrip("/")), None)
        if matched:
            result[matched] = {
                "views": _parse_int(item.get("playCount") or item.get("videoPlayCount")),
                "likes": _parse_int(item.get("diggCount") or item.get("likeCount")),
                "comments": _parse_int(item.get("commentCount")),
            }
    return result


async def refresh_facebook_videos(urls: list[str]) -> dict[str, dict]:
    """Refresh Facebook videos via Apify — single batch run for all URLs."""
    _load_extra_env()
    if not urls:
        return {}
    try:
        items = await _apify_run(
            FACEBOOK_POSTS_APIFY_ACTOR_ID,
            {"startUrls": [{"url": u} for u in urls], "resultsLimit": len(urls)},
        )
    except Exception as exc:
        log.warning("Facebook Apify refresh failed: %s", exc)
        return {}

    result: dict[str, dict] = {}
    for item in items:
        item_url = (item.get("url") or item.get("postUrl") or "").rstrip("/")
        matched = next(
            (u for u in urls if u.rstrip("/") == item_url),
            None,
        )
        if matched:
            result[matched] = {
                "views": _parse_int(item.get("views") or item.get("viewCount")),
                "likes": _parse_int(item.get("likes") or item.get("reactions")),
                "comments": _parse_int(item.get("comments") or item.get("commentCount")),
            }
    return result


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

PLATFORM_BATCH_FETCHER = {
    "youtube": refresh_youtube_videos,
    "vk": refresh_vk_videos,
    "rutube": refresh_rutube_videos,
    "ok": refresh_ok_videos,
    "pinterest": refresh_pinterest_videos,
    "instagram": refresh_instagram_videos,
    "tiktok": refresh_tiktok_videos,
    "facebook": refresh_facebook_videos,
}


async def refresh_platform_batch(platform: str, urls: list[str]) -> dict[str, dict]:
    """Dispatch to the correct fetcher for the given platform."""
    fetcher = PLATFORM_BATCH_FETCHER.get(platform)
    if fetcher is None:
        log.debug("No refresh fetcher for platform: %s", platform)
        return {}
    return await fetcher(urls)
