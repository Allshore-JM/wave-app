from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable, Any


_CACHE: dict[str, tuple[datetime, Any]] = {}


def ttl_get(key: str, ttl_seconds: int, fetcher: Callable[[], Any]) -> Any:
    now = datetime.now(timezone.utc)

    if key in _CACHE:
        created, value = _CACHE[key]
        if now - created < timedelta(seconds=ttl_seconds):
            return value

    value = fetcher()
    _CACHE[key] = (now, value)
    return value


def clear_cache() -> None:
    _CACHE.clear()
