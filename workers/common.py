"""
Bootstrap module: Google Sheets auth, clients config, trigger helper.
"""
from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any

import gspread
import yaml
from google.oauth2.service_account import Credentials

log = logging.getLogger(__name__)

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_gspread_client: gspread.Client | None = None


def _load_dotenv() -> None:
    """Load .env file from the repo root into os.environ (does not overwrite existing vars)."""
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        return
    try:
        with open(env_path, encoding="utf-8") as fh:
            for line in fh:
                raw = line.strip()
                if not raw or raw.startswith("#") or "=" not in raw:
                    continue
                key, value = raw.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception as exc:
        log.warning("Could not load .env: %s", exc)


# Load on import so every module that imports from here gets the env vars.
_load_dotenv()


def get_client() -> gspread.Client:
    """Return a cached authenticated gspread client."""
    global _gspread_client
    if _gspread_client is not None:
        return _gspread_client

    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

    if creds_path and Path(creds_path).exists():
        creds = Credentials.from_service_account_file(creds_path, scopes=_SCOPES)
        log.info("Google auth: using service account file %s", creds_path)
    elif service_account_json:
        info = json.loads(service_account_json)
        creds = Credentials.from_service_account_info(info, scopes=_SCOPES)
        log.info("Google auth: using GOOGLE_SERVICE_ACCOUNT_JSON env var")
    else:
        raise RuntimeError(
            "Google credentials not found. "
            "Set GOOGLE_APPLICATION_CREDENTIALS to the path of your service account JSON."
        )

    _gspread_client = gspread.authorize(creds)
    return _gspread_client


def is_triggered(value: Any) -> bool:
    """Return True if the cell value represents an enabled trigger."""
    val = (str(value) if value is not None else "").strip().lower()
    return val in ("true", "1", "да", "yes", "✓", "✔", "+", "on", "вкл", "включено")


def load_clients_from_sheet(master_sheet_id: str) -> list[dict]:
    """Load clients from the 'Клиенты' sheet in the master registry spreadsheet."""
    gc = get_client()
    sh = gc.open_by_key(master_sheet_id)
    ws = sh.worksheet("Клиенты")
    rows = ws.get_all_values()

    # Find header row (first row where col A == '#')
    header_idx = next((i for i, r in enumerate(rows) if r and r[0].strip() == "#"), 1)

    clients: list[dict] = []
    for row in rows[header_idx + 1:]:
        if not row or not (row[0].strip().isdigit()):
            continue
        name = row[1].strip() if len(row) > 1 else ""
        url = row[18].strip() if len(row) > 18 else ""
        if not name or not url:
            continue
        m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", url)
        if not m:
            continue
        row_num = row[0].strip()
        key = f"client_{row_num}"
        clients.append({
            "_key": key,
            "name": name,
            "spreadsheet_id": m.group(1),
            "project_admin_sheet": "Админка проекта",
            "project_videos_sheet": "База Данных видео по проекту",
            "project_history_sheet": "История проекта по дням",
        })
    log.info("Loaded %s clients from master sheet %s", len(clients), master_sheet_id)
    return clients


def load_clients_config(path: str | None = None) -> list[dict]:
    """Load clients from master Google Sheet (CLIENTS_SHEET_ID) or fall back to YAML."""
    sheet_id = os.getenv("CLIENTS_SHEET_ID", "").strip()
    if sheet_id:
        return load_clients_from_sheet(sheet_id)

    config_path = path or os.getenv("CLIENTS_CONFIG", "clients.yaml")
    config_file = Path(config_path)
    if not config_file.is_absolute():
        config_file = Path(__file__).parent.parent / config_path
    with open(config_file, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return (data or {}).get("clients", [])
