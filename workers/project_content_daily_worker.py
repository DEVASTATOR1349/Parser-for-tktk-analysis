#!/usr/bin/env python3
"""
Daily project-owned content sync.

Behavior:
- reads enabled clients from `clients.yaml`
- respects master trigger B2 in `Админка проекта`
- batches all Instagram/Facebook account fetches into 1 Apify run per platform
- keeps YouTube/VK/Rutube on official APIs (per account)
- processes each client with 1 batchGet + 1 batchUpdate to Google Sheets
- TikTok is disabled (DISABLED_PROJECT_PLATFORMS includes "tiktok")

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
    fetch_instagram_accounts_batch,
    fetch_facebook_accounts_batch,
    get_project_sheets,
    read_admin_rows,
    batched_sync_client,
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


async def run_once(client_key: str | None = None, max_apify_runs: int = 1, platform_filter: str | None = None) -> list[dict]:
    contexts = await _load_contexts(client_key)

    # Collect accounts per platform across all clients.
    # TikTok is skipped via DISABLED_PROJECT_PLATFORMS in the pipeline.
    instagram_accounts: list[tuple[str, int]] = []
    facebook_accounts: list[tuple[str, int]] = []
    for ctx in contexts:
        for account in ctx.accounts:
            if platform_filter and account.platform != platform_filter:
                continue
            if account.platform == "instagram":
                instagram_accounts.append((account.account_url, _fetch_limit(account)))
            elif account.platform == "facebook":
                facebook_accounts.append((account.account_url, _fetch_limit(account)))

    # ONE Apify run per platform — all accounts together, single request
    prefetched_instagram: dict[str, dict] = {}
    if instagram_accounts:
        log.info("Instagram: single batch with %s accounts", len(instagram_accounts))
        try:
            prefetched_instagram = await fetch_instagram_accounts_batch(instagram_accounts)
        except Exception as exc:
            log.warning("Instagram single batch failed: %s", exc)
            for account_url, _ in instagram_accounts:
                prefetched_instagram[account_url] = None

    prefetched_facebook: dict[str, dict] = {}
    if facebook_accounts:
        log.info("Facebook: single batch with %s accounts", len(facebook_accounts))
        try:
            prefetched_facebook = await fetch_facebook_accounts_batch(facebook_accounts)
        except Exception as exc:
            log.warning("Facebook single batch failed: %s", exc)
            for account_url, _ in facebook_accounts:
                prefetched_facebook[account_url] = None

    # Process each client with batched_sync_client (1 batchGet + 1 batchUpdate each)
    results: list[dict] = []
    for ctx in contexts:
        try:
            result = await batched_sync_client(
                ctx.client_config,
                ctx.sh,
                prefetched_instagram=prefetched_instagram,
                prefetched_facebook=prefetched_facebook,
            )
        except Exception as exc:
            log.exception("[%s] batched_sync_client failed", ctx.client_config.get("name"))
            result = {
                "client": ctx.client_config.get("name"),
                "error": str(exc)[:200],
            }

        log.info(
            "[%s] accounts=%s new_videos=%s statuses=%s",
            result.get("client", "?"),
            result.get("accounts", 0),
            result.get("new_videos", 0),
            ",".join(result.get("statuses", [])),
        )
        results.append(result)

        # Brief pause between clients to stay within Sheets API quota (300 req/min)
        if len(contexts) > 1:
            await asyncio.sleep(int(os.getenv("SCOUT_INTER_CLIENT_DELAY_SEC", "3")))

    return results


def main():
    parser = argparse.ArgumentParser(description="Daily project content sync worker with batched Google Sheets writes")
    parser.add_argument("--client", help="Only run for one client key from clients.yaml")
    parser.add_argument("--once", action="store_true", help="Run one sync cycle and exit")
    parser.add_argument("--max-apify-runs", type=int, default=1, help="Max Apify runs per cycle (legacy, kept for compat)")
    parser.add_argument("--test-limit", type=int, default=0, help="Limit videos fetched per account (for testing, e.g. --test-limit 3)")
    parser.add_argument("--platform", help="Only sync this platform (e.g. tiktok, instagram, youtube)")
    args = parser.parse_args()

    if args.test_limit > 0:
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
        log.info("Test mode: fetch limit = %s per account (os.environ updated for lazy reads)", args.test_limit)

    if args.once:
        asyncio.run(run_once(args.client, args.max_apify_runs, args.platform))
        return
    asyncio.run(run_once(args.client, args.max_apify_runs, args.platform))


if __name__ == "__main__":
    main()
