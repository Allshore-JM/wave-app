from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from .cache import ttl_get
from .models import NormalizedBuoy
from .ndbc import latest_buoys as ndbc_latest_buoys
from .cdip import latest_buoys as cdip_latest_buoys
from .erddap import ireland_wave_buoys_latest, canada_dfo_buoys_latest


FETCHERS = [
    ndbc_latest_buoys,
    cdip_latest_buoys,
    ireland_wave_buoys_latest,
    canada_dfo_buoys_latest,
]


def _fetch_all_uncached(enabled_sources: Optional[set[str]] = None) -> dict:
    buoys: list[dict] = []
    errors: list[dict] = []

    for fetcher in FETCHERS:
        source_key = getattr(fetcher, "source_key", fetcher.__name__)
        source_name = getattr(fetcher, "source_name", source_key)

        if enabled_sources and source_key not in enabled_sources:
            continue

        try:
            source_buoys = fetcher()
            buoys.extend([b.to_dict() if isinstance(b, NormalizedBuoy) else b for b in source_buoys])
        except Exception as exc:
            errors.append(
                {
                    "source_key": source_key,
                    "source": source_name,
                    "error": str(exc),
                }
            )

    # Deduplicate within same source/station.
    deduped: dict[tuple[str, str], dict] = {}
    for b in buoys:
        key = (str(b.get("source_key", "")), str(b.get("station_id", "")))
        if key not in deduped:
            deduped[key] = b
        else:
            old_age = deduped[key].get("age_minutes")
            new_age = b.get("age_minutes")
            if old_age is None or (new_age is not None and new_age < old_age):
                deduped[key] = b

    final_buoys = list(deduped.values())
    final_buoys.sort(key=lambda x: (x.get("source_key") or "", x.get("station_id") or ""))

    return {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "count": len(final_buoys),
        "buoys": final_buoys,
        "errors": errors,
    }


def get_all_live_buoys(
    enabled_sources: Optional[set[str]] = None,
    max_age_hours: Optional[float] = 96,
) -> dict:
    """
    Fetch normalized latest buoy observations from all enabled sources.
    """
    cache_key = "all_live_buoys:" + ",".join(sorted(enabled_sources or {"all"}))

    result = ttl_get(
        cache_key,
        5 * 60,
        lambda: _fetch_all_uncached(enabled_sources=enabled_sources),
    )

    if max_age_hours is None:
        return result

    max_age_minutes = max_age_hours * 60.0
    filtered = []

    for buoy in result.get("buoys", []):
        age = buoy.get("age_minutes")

        # Keep unknown age for now because some source timestamps are imperfect.
        if age is None or age <= max_age_minutes:
            filtered.append(buoy)

    return {
        **result,
        "count": len(filtered),
        "buoys": filtered,
        "max_age_hours": max_age_hours,
    }


def get_one_live_buoy(source_key: str, station_id: str) -> Optional[dict]:
    source_key = source_key.lower().strip()
    station_id = station_id.lower().strip()

    result = get_all_live_buoys(enabled_sources={source_key}, max_age_hours=None)

    for buoy in result.get("buoys", []):
        if str(buoy.get("station_id", "")).lower() == station_id:
            return buoy

    return None
