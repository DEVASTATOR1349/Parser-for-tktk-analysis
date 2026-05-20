#!/usr/bin/env python3
"""
Per-video metric refresh worker.

Reads existing rows from 'База Данных видео по проекту', finds rows whose
next_refresh_at has passed (or is not set), fetches fresh metrics from the
platform API, updates the same row, and appends a snapshot to 'История видео'.

Refresh schedule (days between successive refreshes):
  0→1: +3d  |  1→2: +7d  |  2→3: +14d  |  3→4: +21d  |  4+→*: +31d

New columns added to 'База Данных видео по проекту':
  AO: Количество сборов    (parse_count)
  AP: Последний рефреш     (last_refreshed_at, dd.mm.yyyy HH:MM МСК)
  AQ: Следующий рефреш     (next_refresh_at,   dd.mm.yyyy HH:MM МСК)
  AR: Статус рефреша       (OK / ERROR / text)

Run:
  python workers/video_refresh_worker.py --once
  python workers/video_refresh_worker.py --once --max-rows 2   # test mode
  python workers/video_refresh_worker.py --once --client test_client
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent.parent))

from workers.common import load_clients_config, get_client
from services.project_content_pipeline import (
    _sheet_call,
    ensure_sheet,
    canonical_platform_name,
)
from services.video_refresh_scheduler import (
    compute_next_refresh,
    needs_refresh,
    format_dt,
    parse_dt,
    MSK,
)
from services.video_refresh import refresh_platform_batch

# ── logging ──────────────────────────────────────────────────────────────────
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                         datefmt="%Y-%m-%d %H:%M:%S"))
logging.getLogger().handlers = [_handler]
logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger("scout.video_refresh")

# ── constants ─────────────────────────────────────────────────────────────────
MSK = ZoneInfo("Europe/Moscow")
VIDEO_DATA_START_ROW = 5
DEFAULT_MAX_ROWS = 50

# Columns in the video sheet (1-indexed)
COL_PUBLISHED_AT  = 1   # A
COL_PLATFORM      = 3   # C
COL_VIDEO_URL     = 4   # D
COL_COMMENTS      = 11  # K
COL_LIKES         = 12  # L
COL_PLAY_VIEWS    = 13  # M
COL_RAW_VIEWS     = 14  # N
COL_PARSE_COUNT   = 41  # AO
COL_LAST_REFRESH  = 42  # AP
COL_NEXT_REFRESH  = 43  # AQ
COL_STATUS        = 44  # AR

REFRESH_HEADER_ROW = 4
REFRESH_HEADERS = ["Количество сборов", "Последний рефреш", "Следующий рефреш", "Статус рефреша"]

HISTORY_SHEET_NAME = "История видео"
HISTORY_HEADERS = [[
    "Дата рефреша",
    "Платформа",
    "Ссылка на ролик",
    "Просмотры",
    "Лайки",
    "Комментарии",
    "Сборов всего",
]]


def _col_letter(n: int) -> str:
    """Convert 1-indexed column number to letter(s): 1→A, 27→AA, 41→AO."""
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(ord("A") + rem) + result
    return result


def _ensure_refresh_columns(videos_ws) -> None:
    """Add AO:AR headers in row 4 if they are missing."""
    try:
        existing = _sheet_call(videos_ws.get, f"AO{REFRESH_HEADER_ROW}:AR{REFRESH_HEADER_ROW}")
        if existing and any(c.strip() for c in (existing[0] if existing else [])):
            return  # headers already written
    except Exception:
        pass

    # Ensure sheet has enough columns
    if int(getattr(videos_ws, "col_count", 0) or 0) < COL_STATUS:
        _sheet_call(videos_ws.add_cols, COL_STATUS - int(getattr(videos_ws, "col_count", 0) or 0))

    _sheet_call(
        videos_ws.update,
        f"AO{REFRESH_HEADER_ROW}:AR{REFRESH_HEADER_ROW}",
        [REFRESH_HEADERS],
        value_input_option="USER_ENTERED",
    )
    log.info("Wrote refresh column headers to AO:AR row %s", REFRESH_HEADER_ROW)


def _read_candidates(videos_ws, now: datetime, max_rows: int) -> list[dict]:
    """
    Return up to max_rows rows that need refreshing.
    Reads two narrow ranges to avoid pulling all 40 columns.
    """
    last_row = max(int(getattr(videos_ws, "row_count", 5000) or 5000), VIDEO_DATA_START_ROW + 1)
    start = VIDEO_DATA_START_ROW

    core = _sheet_call(videos_ws.get, f"A{start}:D{last_row}")   # published_at, _, platform, video_url
    meta = _sheet_call(videos_ws.get, f"AO{start}:AR{last_row}")  # parse_count, last, next, status

    candidates: list[dict] = []
    for offset, core_row in enumerate(core):
        published_at_str = (core_row[0] if len(core_row) > 0 else "").strip()
        platform_raw     = (core_row[2] if len(core_row) > 2 else "").strip()
        video_url        = (core_row[3] if len(core_row) > 3 else "").strip()

        if not platform_raw or not video_url:
            continue

        platform = canonical_platform_name(platform_raw)

        meta_row        = meta[offset] if offset < len(meta) else []
        parse_count_str = (meta_row[0] if len(meta_row) > 0 else "").strip()
        last_str        = (meta_row[1] if len(meta_row) > 1 else "").strip()
        next_str        = (meta_row[2] if len(meta_row) > 2 else "").strip()
        status          = (meta_row[3] if len(meta_row) > 3 else "").strip()

        if status == "STOPPED":
            continue

        next_dt = parse_dt(next_str)
        if not needs_refresh(next_dt, now):
            continue

        candidates.append({
            "row_idx":      VIDEO_DATA_START_ROW + offset,
            "platform":     platform,
            "video_url":    video_url,
            "published_at": published_at_str,
            "parse_count":  int(parse_count_str) if parse_count_str.isdigit() else 0,
            "last_refresh": last_str,
            "next_refresh": next_str,
        })

        if len(candidates) >= max_rows:
            break

    return candidates


def _write_row_updates(videos_ws, updates: list[dict]) -> None:
    """Batch-update metrics + refresh-tracking columns for each row."""
    if not updates:
        return

    batch: list[dict] = []
    for u in updates:
        row = u["row_idx"]
        v, l, c = u["views"], u["likes"], u["comments"]
        pc   = u["parse_count"]
        last = u["last_refresh_str"]
        nxt  = u["next_refresh_str"]
        stat = u["status"]

        batch.extend([
            {"range": f"K{row}:N{row}", "values": [[c, l, v, v]]},
            {"range": f"AO{row}:AR{row}", "values": [[pc, last, nxt, stat]]},
        ])

    _sheet_call(videos_ws.batch_update, batch, value_input_option="USER_ENTERED")


def _append_history(history_ws, rows: list[list]) -> None:
    if not rows:
        return
    _sheet_call(history_ws.append_rows, rows, value_input_option="USER_ENTERED")


async def refresh_client(client_config: dict, max_rows: int = DEFAULT_MAX_ROWS) -> dict:
    gc = get_client()
    sh = gc.open_by_key(client_config["spreadsheet_id"])

    videos_ws = sh.worksheet(client_config.get("project_videos_sheet", "База Данных видео по проекту"))
    history_ws = ensure_sheet(sh, HISTORY_SHEET_NAME, HISTORY_HEADERS, rows=10000, cols=8)

    _ensure_refresh_columns(videos_ws)

    now = datetime.now(MSK)
    candidates = _read_candidates(videos_ws, now, max_rows)

    if not candidates:
        log.info("[%s] no rows need refreshing", client_config.get("name"))
        return {"client": client_config.get("name"), "refreshed": 0, "errors": 0}

    log.info("[%s] %s rows to refresh", client_config.get("name"), len(candidates))

    # Group by platform for batched API calls
    by_platform: dict[str, list[dict]] = defaultdict(list)
    for row in candidates:
        by_platform[row["platform"]].append(row)

    row_updates: list[dict] = []
    history_rows: list[list] = []
    errors = 0

    for platform, rows in by_platform.items():
        urls = [r["video_url"] for r in rows]
        log.info("[%s] refreshing %s %s videos", client_config.get("name"), len(urls), platform)

        metrics_map = await refresh_platform_batch(platform, urls)

        for row_data in rows:
            url = row_data["video_url"]
            metrics = metrics_map.get(url)

            if metrics is None:
                errors += 1
                # Still update tracking so we don't retry immediately
                new_parse_count = row_data["parse_count"]
                last_dt = parse_dt(row_data["last_refresh"]) or now
                next_dt = compute_next_refresh(now, new_parse_count)
                row_updates.append({
                    "row_idx":       row_data["row_idx"],
                    "views": 0, "likes": 0, "comments": 0,
                    "parse_count":   new_parse_count,
                    "last_refresh_str": format_dt(last_dt),
                    "next_refresh_str": format_dt(next_dt),
                    "status": "ERROR: no data",
                })
                continue

            new_parse_count = row_data["parse_count"] + 1
            next_dt = compute_next_refresh(now, new_parse_count)

            row_updates.append({
                "row_idx":         row_data["row_idx"],
                "views":           metrics.get("views", 0),
                "likes":           metrics.get("likes", 0),
                "comments":        metrics.get("comments", 0),
                "parse_count":     new_parse_count,
                "last_refresh_str": format_dt(now),
                "next_refresh_str": format_dt(next_dt),
                "status":          "OK",
            })

            history_rows.append([
                format_dt(now),
                platform,
                url,
                metrics.get("views", 0),
                metrics.get("likes", 0),
                metrics.get("comments", 0),
                new_parse_count,
            ])

    _write_row_updates(videos_ws, row_updates)
    _append_history(history_ws, history_rows)

    ok_count = len(row_updates) - errors
    log.info("[%s] done: refreshed=%s errors=%s", client_config.get("name"), ok_count, errors)
    return {
        "client":    client_config.get("name"),
        "refreshed": ok_count,
        "errors":    errors,
    }


async def run_once(client_key: str | None = None, max_rows: int = DEFAULT_MAX_ROWS) -> list[dict]:
    results = []
    for client in load_clients_config():
        if client_key and client.get("_key") != client_key:
            continue
        try:
            result = await refresh_client(client, max_rows=max_rows)
            results.append(result)
        except Exception as exc:
            log.exception("[%s] refresh failed: %s", client.get("name"), exc)
    return results


def main():
    parser = argparse.ArgumentParser(description="Per-video metric refresh worker")
    parser.add_argument("--once", action="store_true", help="Run one refresh cycle and exit")
    parser.add_argument("--client", help="Limit to one client key from clients.yaml")
    parser.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS,
                        help=f"Max rows to refresh per run (default {DEFAULT_MAX_ROWS}, use 2 for testing)")
    args = parser.parse_args()

    if args.once:
        results = asyncio.run(run_once(args.client, args.max_rows))
        for r in results:
            print(f"[{r['client']}] refreshed={r['refreshed']} errors={r['errors']}")
        return

    asyncio.run(run_once(args.client, args.max_rows))


if __name__ == "__main__":
    main()
