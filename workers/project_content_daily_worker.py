#!/usr/bin/env python3
"""
Daily project-owned content sync.

Behavior (v2):
- reads enabled clients from clients.yaml
- respects master trigger B2 in `Админка проекта`
- splits Instagram/Facebook accounts into batches (max `--max-apify-runs 5`)
  to limit Apify credit consumption per cycle
- keeps YouTube/VK/Rutube on official APIs (per account)
- processes each client with 1 batchGet + 1 batchUpdate to Google Sheets
- TikTok is disabled (DISABLED_PROJECT_PLATFORMS includes "tiktok")

Key difference from v1:
- v1 used 1 single Apify run per platform for all accounts; fast but costly
  if many accounts (everything runs in 1 shot)
- v2 splits accounts into up to `--max-apify-runs` batches; slightly slower
  but much cheaper — you control max spend per cycle

Recommended cron:
- daily at 06:00 MSK (03:00 UTC)
"""

from __future__ import annotations

import argparse
import asyncio
import gspread
import os
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent.parent))

from services.project_content_pipeline import (
    _sheet_call,
    build_instagram_apify_batches,
    build_facebook_apify_batches,
    fetch_instagram_accounts_batch,
    fetch_facebook_accounts_batch,
    get_project_sheets,
    read_admin_rows,
    batched_sync_client,
    _fetch_limit,
)
from workers.common import get_client, load_clients_config

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
    gc = get_client()
    clients = load_clients_config()
    for idx, client in enumerate(clients):
        if client_key and client.get("_key") != client_key:
            continue
        try:
            # Open spreadsheet with retry (quota saver)
            for attempt in range(3):
                try:
                    sh = gc.open_by_key(client["spreadsheet_id"])
                    break
                except gspread.exceptions.APIError as e:
                    if "429" in str(e) or "Quota exceeded" in str(e):
                        sleep_for = 10 * (attempt + 1)
                        log.warning("Quota on open_by_key, sleeping %ss", sleep_for)
                        time.sleep(sleep_for)
                        continue
                    raise

            admin_ws = sh.worksheet("Админка проекта")
            videos_ws = sh.worksheet("База Данных видео по проекту")
            history_ws = sh.worksheet("История проекта по дням")
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

        # Stagger opens to avoid Sheets API quota bursts
        if idx < len(clients) - 1:
            time.sleep(0.5)

    return contexts


async def run_once(client_key: str | None = None, max_apify_runs: int = 5, platform_filter: str | None = None) -> list[dict]:
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

    # v2: Split accounts into batches and run up to `max_apify_runs` batches.
    # Each batch = one Apify run with a subset of accounts.
    # This caps the maximum Apify spend per cycle regardless of total accounts.
    prefetched_instagram: dict[str, dict] = {}
    if instagram_accounts:
        insta_batches = build_instagram_apify_batches(instagram_accounts, max_runs=max_apify_runs)
        log.info(
            "Instagram: %s account(s) split into %s batch(es), max_runs=%s",
            len(instagram_accounts), len(insta_batches), max_apify_runs,
        )
        for batch_idx, batch in enumerate(insta_batches):
            log.info("  Batch %s/%s: %s account(s)", batch_idx + 1, len(insta_batches), len(batch))
            try:
                batch_result = await fetch_instagram_accounts_batch(batch)
                prefetched_instagram.update(batch_result)
            except Exception as exc:
                log.warning("Instagram batch %s failed: %s", batch_idx + 1, exc)
                for account_url, _ in batch:
                    prefetched_instagram.setdefault(account_url, None)
    else:
        log.info("Instagram: no accounts")

    prefetched_facebook: dict[str, dict] = {}
    if facebook_accounts:
        fb_batches = build_facebook_apify_batches(facebook_accounts, max_runs=max_apify_runs)
        log.info(
            "Facebook: %s account(s) split into %s batch(es), max_runs=%s",
            len(facebook_accounts), len(fb_batches), max_apify_runs,
        )
        for batch_idx, batch in enumerate(fb_batches):
            log.info("  Batch %s/%s: %s account(s)", batch_idx + 1, len(fb_batches), len(batch))
            try:
                batch_result = await fetch_facebook_accounts_batch(batch)
                prefetched_facebook.update(batch_result)
            except Exception as exc:
                log.warning("Facebook batch %s failed: %s", batch_idx + 1, exc)
                for account_url, _ in batch:
                    prefetched_facebook.setdefault(account_url, None)
    else:
        log.info("Facebook: no accounts")

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
    parser = argparse.ArgumentParser(description="Daily project content sync worker v2 — batched Apify runs + batched Google Sheets writes")
    parser.add_argument("--client", help="Only run for one client key from clients.yaml")
    parser.add_argument("--once", action="store_true", help="Run one sync cycle and exit")
    parser.add_argument("--max-apify-runs", type=int, default=5, help="Max Apify runs per platform per cycle (default: 5)")
    parser.add_argument("--test-limit", type=int, default=0, help="Limit videos fetched per account (for testing, e.g. --test-limit 3)")
    parser.add_argument("--platform", help="Only sync this platform (e.g. instagram, youtube)")
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
