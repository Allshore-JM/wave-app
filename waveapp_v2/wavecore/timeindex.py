from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc


def hourly_series_from_day_hour(cycle_dt_utc: datetime, day_hour_tuples: list[tuple[int, int]]) -> list[datetime]:
    """
    Return strictly +1h UTC datetimes anchored to cycle month/day/hour rules.
    cycle_dt_utc must be a timezone-aware datetime in UTC.
    day_hour_tuples: list of (day, hour) tuples.
    """
    if cycle_dt_utc.tzinfo != UTC:
        raise ValueError("cycle_dt_utc must be timezone-aware UTC")
    results = []
    last = None
    for day_val, hour_val in day_hour_tuples:
        if last is None:
            try:
                dt = cycle_dt_utc.replace(day=day_val, hour=hour_val, minute=0, second=0, microsecond=0)
            except ValueError:
                continue
            while dt < cycle_dt_utc:
                dt += timedelta(days=1)
        else:
            dt = last + timedelta(hours=1)
        results.append(dt)
        last = dt
    return results


def to_local(dt_utc: datetime, tz_name: str) -> datetime:
    """
    Convert a UTC datetime to a local timezone.
    """
    tz = ZoneInfo(tz_name)
    return dt_utc.astimezone(tz)
