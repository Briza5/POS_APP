"""Prague timezone helpers (CET/CEST with automatic DST)."""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

PRAGUE_TZ = ZoneInfo("Europe/Prague")


def to_prague(dt: datetime | None) -> datetime | None:
    """Convert a naive UTC datetime to Prague local time."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(PRAGUE_TZ)


def prague_offset_hours() -> int:
    """Return current Prague UTC offset in whole hours (+1 winter, +2 summer)."""
    now = datetime.now(PRAGUE_TZ)
    return int(now.utcoffset().total_seconds() // 3600)


def fmt(dt: datetime | None, fmt: str = "%d.%m %H:%M") -> str:
    """Format a naive UTC datetime in Prague local time."""
    if dt is None:
        return "—"
    local = to_prague(dt)
    return local.strftime(fmt) if local else "—"
