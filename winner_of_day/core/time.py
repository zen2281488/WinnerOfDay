from __future__ import annotations

import datetime

MSK_TZ = datetime.timezone(datetime.timedelta(hours=3))


def current_timestamp() -> int:
    return int(datetime.datetime.now(MSK_TZ).timestamp())


def format_msk_time(timestamp: int) -> str:
    try:
        dt = datetime.datetime.fromtimestamp(int(timestamp), tz=MSK_TZ)
        return dt.strftime("%d.%m %H:%M")
    except Exception:
        return str(timestamp)


def format_build_date(value: str) -> str:
    if not value or value == "unknown":
        return "неизвестно"
    try:
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        dt = datetime.datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        dt = dt.astimezone(MSK_TZ)
        return dt.strftime("%d.%m.%y в %H:%M")
    except Exception:
        return value


def retention_cutoff_ts(days: int, now_ts: int) -> int | None:
    if not days or int(days) <= 0:
        return None
    return int(now_ts) - int(days) * 24 * 3600
