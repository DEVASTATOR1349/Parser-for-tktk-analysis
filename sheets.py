"""Работа с Google Sheets:
- Чтение «ВсеИсточники» через Google Visualization API (без авторизации)
- Запись результатов напрямую через Google Sheets API (сервисный аккаунт)
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import google.oauth2.service_account
import googleapiclient.discovery
import requests
from loguru import logger

from config import SOURCE_SHEET_ID, SHEET_LINKS_GID, SOURCE_NAME_MAP

# ID целевой таблицы (куда пишем результаты) и диапазон
RESULTS_SHEET_ID = "10S1xijZ4ZNXVB4JQKyBylFmc7N_jwazHKSTc9pNj-t8"
RESULTS_TAB = "ДанныеПарсинга"
ERRORS_TAB = "Ошибки"

# Путь к JSON-ключу сервисного аккаунта
_KEY_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/app/eva-bot-api-key.json")
_CREDS: google.oauth2.service_account.Credentials | None = None
_SERVICE: Any = None  # googleapiclient.discovery.Resource


def _get_service():
    global _CREDS, _SERVICE
    if _SERVICE is not None:
        return _SERVICE
    try:
        _CREDS = google.oauth2.service_account.Credentials.from_service_account_file(
            _KEY_PATH,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        _SERVICE = googleapiclient.discovery.build("sheets", "v4", credentials=_CREDS)
        logger.info("Google Sheets API: подключён")
        return _SERVICE
    except Exception as e:
        logger.error(f"Google Sheets API: ошибка подключения — {e}")
        return None


# ---------------------------------------------------------------------------
# Чтение через Visualization API (без авторизации)
# ---------------------------------------------------------------------------

def _fetch_viz_data(gid: int = 0) -> dict | None:
    url = (
        f"https://docs.google.com/spreadsheets/d/{SOURCE_SHEET_ID}"
        f"/gviz/tq?tqx=out:json&gid={gid}"
    )
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        content = resp.text
        marker = "google.visualization.Query.setResponse("
        start = content.index(marker) + len(marker)
        end = content.rindex(")")
        inner = content[start:end]
        return json.loads(inner) if inner else None
    except Exception as e:
        logger.warning(f"Visualization API error: {e}")
        return None


def read_links_sheet() -> list[dict[str, Any]]:
    """Читает «ВсеИсточники» (gid=425357122)."""
    raw = _fetch_viz_data(gid=SHEET_LINKS_GID)
    if not raw:
        logger.error("Не удалось получить данные из таблицы (ВсеИсточники)")
        return []

    rows = raw.get("table", {}).get("rows", [])
    if not rows:
        logger.warning("Нет данных в «ВсеИсточники»")
        return []

    projects: dict[str, list[str]] = {}
    skipped_sources: set[str] = set()

    for row in rows[1:]:
        vals = [c.get("v") if c else None for c in row["c"]]
        client = (vals[0] or "").strip() if len(vals) > 0 else ""
        source = (vals[1] or "").strip() if len(vals) > 1 else ""
        url = (vals[2] or "").strip() if len(vals) > 2 else ""

        if not client or not url:
            continue
        if not url.startswith(("http://", "https://")):
            continue

        platform_key = SOURCE_NAME_MAP.get(source)
        if not platform_key:
            skipped_sources.add(source)
            continue

        projects.setdefault(client, []).append(url)

    if skipped_sources:
        logger.debug(f"Пропущенные источники: {', '.join(sorted(skipped_sources))}")

    result = [{"name": k, "links": v} for k, v in projects.items()]
    logger.info(
        f"«ВсеИсточники»: {len(result)} проектов, "
        f"всего {sum(len(p['links']) for p in result)} ссылок"
    )
    return result


# ---------------------------------------------------------------------------
# Запись через Google Sheets API (сервисный аккаунт)
# ---------------------------------------------------------------------------

def _append_rows(tab_name: str, rows: list[list[str]], header: list[str] | None = None) -> bool:
    """Дописывает строки в конец листа. Если листа нет — создаёт."""
    svc = _get_service()
    if not svc:
        return False

    try:
        # Проверяем существование листа
        meta = svc.spreadsheets().get(spreadsheetId=RESULTS_SHEET_ID).execute()
        sheet_names = [s["properties"]["title"] for s in meta.get("sheets", [])]

        if tab_name not in sheet_names:
            # Создаём лист
            svc.spreadsheets().batchUpdate(
                spreadsheetId=RESULTS_SHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
            ).execute()
            if header:
                svc.spreadsheets().values().append(
                    spreadsheetId=RESULTS_SHEET_ID,
                    range=f"{tab_name}!A1",
                    valueInputOption="RAW",
                    body={"values": [header]},
                ).execute()
            logger.info(f"Создан лист «{tab_name}»")

        # Дописываем данные
        svc.spreadsheets().values().append(
            spreadsheetId=RESULTS_SHEET_ID,
            range=f"{tab_name}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows},
        ).execute()
        return True
    except Exception as e:
        logger.error(f"Google Sheets API: ошибка записи в «{tab_name}» — {e}")
        return False


def write_results(results: list[dict[str, Any]]):
    """Пишет результаты в лист «ДанныеПарсинга»."""
    if not results:
        logger.info("Нет данных для записи")
        return

    rows = [
        [
            r.get("date", ""),
            r.get("client", ""),
            r.get("platform", ""),
            str(r.get("followers", "")),
        ]
        for r in results
    ]
    ok = _append_rows(RESULTS_TAB, rows, header=["Дата", "Клиент", "Площадка", "Подписчиков"])
    if ok:
        logger.info(f"Записано: {len(rows)} строк в «{RESULTS_TAB}»")
    else:
        logger.error(f"Не удалось записать {len(rows)} строк")


def log_errors_batch(errors: list[dict[str, str]]):
    """Пишет ошибки в лист «Ошибки»."""
    if not errors:
        return
    rows = [
        [e["date"], e["client"], e["link"], e["error"]]
        for e in errors
    ]
    ok = _append_rows(ERRORS_TAB, rows, header=["Дата", "Клиент", "Ссылка", "Ошибка"])
    if ok:
        logger.info(f"Записано ошибок: {len(errors)}")
    else:
        logger.warning(f"Не удалось записать {len(errors)} ошибок")


def log_error(client: str, link: str, error: str):
    """Однострочная запись ошибки (совместимость)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_errors_batch([{"date": today, "client": client, "link": link, "error": error[:200]}])
