#!/usr/bin/env python3
"""
Daily project-owned content sync with batched Instagram Apify runs.

Behavior:
- reads enabled clients from `clients.yaml`
- respects master trigger B2 in `Админка проекта`
- batches all Instagram account fetches into at most N Apify runs per cycle
- keeps YouTube on official API per account
- appends only unseen videos to `База Данных видео по проекту`
- updates `История проекта по дням`

Recommended cron:
- daily at 06:00 MSK (03:00 UTC)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent.parent))

from services.project_content_pipeline import (
    INSTAGRAM_MAX_APIFY_RUNS_PER_CYCLE,
    TIKTOK_MAX_APIFY_RUNS_PER_CYCLE,
    FACEBOOK_MAX_APIFY_RUNS_PER_CYCLE,
    build_instagram_apify_batches,
    fetch_instagram_accounts_batch,
    build_tiktok_apify_batches,
    fetch_tiktok_accounts_batch,
    build_facebook_apify_batches,
    fetch_facebook_accounts_batch,
    get_project_sheets,
    read_admin_rows,
    read_existing_video_keys,
    read_missing_main_sheet_instagram_urls,
    sync_project_account,
    sync_main_sheet_publication_metrics,
    update_admin_row,
    upsert_daily_snapshot,
    _empty_daily_snapshot,
    _fetch_limit,
)
from workers.common import load_clients_config

MSK = ZoneInfo("Europe/Moscow")


class MoscowFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, MSK)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S")


_handler = logging.StreamHandler()
_handler.setFormatter(MoscowFormatter("%(asctime)s [%(levelname)s] %(message)s"))
root_logger = logging.getLogger()
root_logger.handlers.clear()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(_handler)

log = logging.getLogger("scout.project_content.daily")


@dataclass
class ClientContext:
    client_config: dict
    sh: object
    admin_ws: object
    videos_ws: object
    history_ws: object
    accounts: list


async def _load_contexts(client_key: str | None = None) -> list[ClientContext]:
    contexts: list[ClientContext] = []
    for client in load_clients_config():
        if client_key and client.get("_key") != client_key:
            continue
        try:
            sh, admin_ws, videos_ws, history_ws = get_project_sheets(client)
            master_enabled, accounts = read_admin_rows(admin_ws)
            if not master_enabled:
                log.info("[%s] skipped: B2 is off", client.get("name"))
                continue
            contexts.append(ClientContext(client, sh, admin_ws, videos_ws, history_ws, accounts))
        except Exception as exc:
            if "Админка проекта" in str(exc) or "База Данных видео по проекту" in str(exc):
                log.info("[%s] project content sheets not configured, skip", client.get("name"))
                continue
            raise
    return contexts


async def run_once(client_key: str | None = None, max_apify_runs: int = INSTAGRAM_MAX_APIFY_RUNS_PER_CYCLE, platform_filter: str | None = None) -> list[dict]:
    contexts = await _load_contexts(client_key)

    instagram_accounts: list[tuple[str, int]] = []
    tiktok_accounts: list[tuple[str, int]] = []
    facebook_accounts: list[tuple[str, int]] = []
    for ctx in contexts:
        for account in ctx.accounts:
            if platform_filter and account.platform != platform_filter:
                continue
            if account.platform == "instagram":
                instagram_accounts.append((account.account_url, _fetch_limit(account)))
            elif account.platform == "tiktok":
                tiktok_accounts.append((account.account_url, _fetch_limit(account)))
            elif account.platform == "facebook":
                facebook_accounts.append((account.account_url, _fetch_limit(account)))

    prefetched_instagram: dict[str, dict] = {}
    batches = build_instagram_apify_batches(instagram_accounts, max_runs=max_apify_runs)
    for idx, batch in enumerate(batches, start=1):
        log.info("Instagram batch %s/%s: %s accounts", idx, len(batches), len(batch))
        try:
            batch_payload = await fetch_instagram_accounts_batch(batch)
            prefetched_instagram.update(batch_payload)
        except Exception as batch_exc:
            log.warning("Instagram batch %s/%s failed (will retry per-account): %s", idx, len(batches), batch_exc)
            for account_url, _ in batch:
                prefetched_instagram[account_url] = None

    prefetched_tiktok: dict[str, dict] = {}
    tiktok_batches = build_tiktok_apify_batches(tiktok_accounts, max_runs=TIKTOK_MAX_APIFY_RUNS_PER_CYCLE)
    for idx, batch in enumerate(tiktok_batches, start=1):
        log.info("TikTok batch %s/%s: %s accounts", idx, len(tiktok_batches), len(batch))
        try:
            batch_payload = await fetch_tiktok_accounts_batch(batch)
            prefetched_tiktok.update(batch_payload)
        except Exception as batch_exc:
            log.warning("TikTok batch %s/%s failed (will retry per-account): %s", idx, len(tiktok_batches), batch_exc)
            for account_url, _ in batch:
                prefetched_tiktok[account_url] = None

    prefetched_facebook: dict[str, dict] = {}
    facebook_batches = build_facebook_apify_batches(facebook_accounts, max_runs=FACEBOOK_MAX_APIFY_RUNS_PER_CYCLE)
    for idx, batch in enumerate(facebook_batches, start=1):
        log.info("Facebook batch %s/%s: %s accounts", idx, len(facebook_batches), len(batch))
        try:
            batch_payload = await fetch_facebook_accounts_batch(batch)
            prefetched_facebook.update(batch_payload)
        except Exception as batch_exc:
            log.warning("Facebook batch %s/%s failed (will retry per-account): %s", idx, len(facebook_batches), batch_exc)
            for account_url, _ in batch:
                prefetched_facebook[account_url] = None

    results: list[dict] = []
    for ctx in contexts:
        existing_keys = read_existing_video_keys(ctx.videos_ws)
        missing_main_sheet_instagram_urls = read_missing_main_sheet_instagram_urls(ctx.sh, ctx.client_config, existing_keys)
        rows_before = len(existing_keys)
        statuses: list[str] = []
        new_videos = 0
        inserted_by_platform: dict[str, int] = {}
        skipped_platforms: dict[str, str] = {}
        incremental_support: dict[str, bool] = {}
        snapshot = _empty_daily_snapshot()

        for account in ctx.accounts:
            if platform_filter and account.platform != platform_filter:
                continue
            try:
                result = await sync_project_account(
                    ctx.admin_ws,
                    ctx.videos_ws,
                    account,
                    existing_keys,
                    forced_video_urls=missing_main_sheet_instagram_urls if account.platform == "instagram" else None,
                    prefetched_instagram_payload=prefetched_instagram.get(account.account_url) if account.platform == "instagram" else None,
                    prefetched_tiktok_payload=prefetched_tiktok.get(account.account_url) if account.platform == "tiktok" else None,
                    prefetched_facebook_payload=prefetched_facebook.get(account.account_url) if account.platform == "facebook" else None,
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
            except Exception as exc:
                status = f"ERROR: {str(exc)[:120]}"
                update_admin_row(ctx.admin_ws, account, account.followers, account.total_videos, 0, status)
                statuses.append(f"{account.platform}:ERROR")
                skipped_platforms[account.platform] = str(exc)[:200]
                incremental_support[account.platform] = False
                log.exception("[%s] project sync failed for row %s: %s", ctx.client_config.get("name"), account.row_idx, exc)

        upsert_daily_snapshot(ctx.history_ws, snapshot)
        bm_sync = sync_main_sheet_publication_metrics(ctx.sh, ctx.client_config, ctx.videos_ws)
        summary = {
            "client": ctx.client_config.get("name"),
            "enabled": True,
            "accounts": len(ctx.accounts),
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
        log.info(
            "[%s] accounts=%s new_videos=%s statuses=%s",
            summary["client"],
            summary["accounts"],
            summary["new_videos"],
            ",".join(summary["statuses"]),
        )
        results.append(summary)
        # Brief pause between clients to stay within Sheets API quota (300 req/min)
        if len(contexts) > 1:
            await asyncio.sleep(int(os.getenv("SCOUT_INTER_CLIENT_DELAY_SEC", "3")))

    return results


def main():
    parser = argparse.ArgumentParser(description="Daily project content sync worker with batched Instagram Apify runs")
    parser.add_argument("--client", help="Only run for one client key from clients.yaml")
    parser.add_argument("--once", action="store_true", help="Run one sync cycle and exit")
    parser.add_argument("--max-apify-runs", type=int, default=INSTAGRAM_MAX_APIFY_RUNS_PER_CYCLE, help="Max Instagram Apify runs per cycle")
    parser.add_argument("--test-limit", type=int, default=0, help="Limit videos fetched per account (for testing, e.g. --test-limit 3)")
    parser.add_argument("--platform", help="Only sync this platform (e.g. tiktok, instagram, youtube)")
    args = parser.parse_args()

    if args.test_limit > 0:
        import os
        limit_str = str(args.test_limit)
        for var in (
            "SCOUT_PROJECT_INSTAGRAM_RESULTS_LIMIT",
            "SCOUT_PROJECT_YOUTUBE_RESULTS_LIMIT",
            "SCOUT_PROJECT_TIKTOK_RESULTS_LIMIT",
            "SCOUT_PROJECT_FACEBOOK_RESULTS_LIMIT",
            "SCOUT_PROJECT_VK_RESULTS_LIMIT",
            "SCOUT_PROJECT_RUTUBE_RESULTS_LIMIT",
            "SCOUT_PROJECT_FULL_BACKFILL_RESULTS_LIMIT",
        ):
            os.environ[var] = limit_str
        log.info("Test mode: fetch limit = %s per account", args.test_limit)

    if args.once:
        asyncio.run(run_once(args.client, args.max_apify_runs, args.platform))
        return
    asyncio.run(run_once(args.client, args.max_apify_runs, args.platform))


if __name__ == "__main__":
    main()
