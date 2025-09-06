# app/timezones.py
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

UTC = timezone.utc

def as_aware_utc(dt: datetime) -> datetime:
    """Return an aware UTC datetime from naive/aware dt (treat naive as UTC)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)

def ensure_tz(tz_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo("UTC")

def convert_utc_to_tz(dt_utc: datetime, tz_name: str) -> datetime:
    return as_aware_utc(dt_utc).astimezone(ensure_tz(tz_name))

def fmt_dt(dt: datetime, tz_name: str, fmt: str = "%b %d, %Y %I:%M %p") -> str:
    return convert_utc_to_tz(dt, tz_name).strftime(fmt)
