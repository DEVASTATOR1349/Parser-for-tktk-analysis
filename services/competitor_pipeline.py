"""
Utility functions re-implemented to satisfy the import contract from project_content_pipeline.py.
Original lives in the full scout project; this is the minimal standalone version.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from dateutil import parser as dateutil_parser


def _parse_dt(value: Any) -> datetime | None:
    """Parse a datetime from various formats: ISO string, unix timestamp, dd.mm.yyyy, etc."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    # Unix timestamps stored as numeric strings
    try:
        ts = float(text)
        if 1_000_000_000 < ts < 20_000_000_000:
            # Seconds or milliseconds since epoch
            divisor = 1000 if ts > 1e12 else 1
            return datetime.fromtimestamp(ts / divisor, tz=timezone.utc)
    except (ValueError, TypeError):
        pass
    # Russian date format dd.mm.yyyy HH:MM
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    # Fallback: dateutil handles most ISO / RFC formats
    try:
        dt = dateutil_parser.parse(text, dayfirst=False)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _format_dt(dt: datetime | None, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    if not dt:
        return ""
    return dt.strftime(fmt)


def _parse_int(value: Any) -> int:
    """Parse integer from a value that may contain spaces, commas, non-breaking spaces."""
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = (
        str(value)
        .strip()
        .replace("\xa0", "")   # non-breaking space
        .replace(" ", "")  # narrow no-break space
        .replace(" ", "")
        .replace(",", "")
        .replace("_", "")
    )
    match = re.search(r"-?\d+", text)
    if not match:
        return 0
    try:
        return int(match.group(0))
    except (ValueError, TypeError):
        return 0


def _metrics_text(views: int, likes: int, comments: int) -> str:
    """Format engagement metrics as a human-readable string."""
    parts: list[str] = []
    if views:
        parts.append(f"👁 {views:,}".replace(",", " "))
    if likes:
        parts.append(f"❤ {likes:,}".replace(",", " "))
    if comments:
        parts.append(f"💬 {comments:,}".replace(",", " "))
    return " | ".join(parts)


def _sanitize_caption(text: Any, limit: int = 5000) -> str:
    """Remove control characters (except \\n and \\t) and truncate."""
    if not text:
        return ""
    s = str(text)
    # Strip ASCII control chars except LF (0x0A) and TAB (0x09)
    s = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", s)
    return s[:limit].strip()


def _join_text_list(value: Any, limit: int = 5000) -> str:
    """Join a list of strings / dicts to a pipe-separated string."""
    if not value:
        return ""
    if isinstance(value, str):
        return value[:limit]
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, dict):
                text = (
                    item.get("text")
                    or item.get("value")
                    or item.get("name")
                    or item.get("title")
                    or str(item)
                )
            else:
                text = str(item)
            text = (text or "").strip()
            if text:
                parts.append(text)
        return " | ".join(parts)[:limit]
    return str(value)[:limit]
