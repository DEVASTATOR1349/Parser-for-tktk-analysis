"""
Scheduling logic for per-video metric refresh.

Refresh intervals (days between refreshes, applied to last_refreshed_at):
  parse_count=0 (never refreshed): +3 days from import
  parse_count=1: +7 days from last refresh
  parse_count=2: +14 days
  parse_count=3: +21 days
  parse_count=4+: +31 days (repeats forever)
"""
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")

REFRESH_SCHEDULE_DAYS = [3, 7, 14, 21, 31]
DT_FMT = "%d.%m.%Y %H:%M"


def compute_next_refresh(last_refreshed_at: datetime | None, parse_count: int) -> datetime:
    """Return when the next refresh should happen, in MSK timezone."""
    idx = min(max(int(parse_count), 0), len(REFRESH_SCHEDULE_DAYS) - 1)
    delta = timedelta(days=REFRESH_SCHEDULE_DAYS[idx])
    base = last_refreshed_at or datetime.now(MSK)
    return base.astimezone(MSK) + delta


def needs_refresh(next_refresh_at: datetime | None, now: datetime | None = None) -> bool:
    """True if it's time to refresh (next_refresh_at is None or in the past)."""
    if next_refresh_at is None:
        return True
    now = (now or datetime.now(MSK)).astimezone(MSK)
    return now >= next_refresh_at.astimezone(MSK)


def format_dt(dt: datetime | None) -> str:
    if not dt:
        return ""
    return dt.astimezone(MSK).strftime(DT_FMT)


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    text = (value or "").strip()
    for fmt in (DT_FMT, "%d.%m.%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.replace(tzinfo=MSK)
        except ValueError:
            pass
    try:
        from dateutil import parser as dateutil_parser
        dt = dateutil_parser.parse(text)
        return dt if dt.tzinfo else dt.replace(tzinfo=MSK)
    except Exception:
        return None
