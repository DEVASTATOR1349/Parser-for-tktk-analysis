"""
Quick sanity check: Google Sheets auth, sheet access, env vars.
Run from repo root: python test_connection.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# workers/common loads .env on import
from workers.common import get_client, load_clients_config, is_triggered

SPREADSHEET_ID = "11kFtVOlWc7z2-klfjgea0FvqiNcXDMEtqAWZ89z7vK0"


def check_env():
    print("\n=== ENV VARS ===")
    keys = [
        "GOOGLE_APPLICATION_CREDENTIALS",
        "APIFY_API_TOKEN",
        "YOUTUBE_API_KEY",
        "VK_API_TOKEN",
    ]
    ok = True
    for k in keys:
        v = os.getenv(k, "")
        status = "OK" if v else "FAIL MISSING"
        masked = v[:8] + "..." if len(v) > 8 else v
        print(f"  {status}  {k} = {masked}")
        if not v:
            ok = False
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    if creds_path and not Path(creds_path).exists():
        print(f"  FAIL  GOOGLE_APPLICATION_CREDENTIALS file not found: {creds_path}")
        ok = False
    return ok


def check_sheets():
    print("\n=== GOOGLE SHEETS ===")
    try:
        gc = get_client()
        sh = gc.open_by_key(SPREADSHEET_ID)
        worksheets = [ws.title for ws in sh.worksheets()]
        print(f"  OK  Opened spreadsheet: {sh.title}")
        print(f"  OK  Worksheets: {worksheets}")
        return True
    except Exception as exc:
        print(f"  FAIL  Failed: {exc}")
        print()
        print("  -&gt; Did you share the sheet with: evabotshhets@eva-bot-api.iam.gserviceaccount.com ?")
        return False


def check_clients_yaml():
    print("\n=== CLIENTS YAML ===")
    try:
        clients = load_clients_config()
        print(f"  OK  Loaded {len(clients)} client(s):")
        for c in clients:
            print(f"       {c.get('_key')} - {c.get('name')} - sheet: {c.get('spreadsheet_id')}")
        return True
    except Exception as exc:
        print(f"  FAIL  Failed: {exc}")
        return False


def check_is_triggered():
    print("\n=== TRIGGER HELPER ===")
    cases = [("ДА", True), ("да", True), ("TRUE", True), ("1", True),
             ("", False), ("нет", False), ("FALSE", False)]
    all_ok = True
    for val, expected in cases:
        result = is_triggered(val)
        ok = result == expected
        all_ok = all_ok and ok
        print(f"  {'OK' if ok else 'FAIL'}  is_triggered({val!r}) = {result} (expected {expected})")
    return all_ok


if __name__ == "__main__":
    results = [
        check_env(),
        check_clients_yaml(),
        check_is_triggered(),
        check_sheets(),
    ]
    print("\n" + ("=" * 40))
    if all(results):
        print("  ALL CHECKS PASSED - ready to run daily worker")
    else:
        print("  SOME CHECKS FAILED - fix the issues above")
    print("=" * 40)
    sys.exit(0 if all(results) else 1)
