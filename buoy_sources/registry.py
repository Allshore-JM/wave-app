from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from .cache import ttl_get
from .models import NormalizedBuoy
from .ndbc import latest_buoys as ndbc_latest_buoys
from .erddap import ireland_wave_buoys_latest, canada_dfo_buoys_latest

# CDIP can be slow on Render because it uses OPeNDAP/xarray.
# Keep it available, but do not include it in the default map load.
try:
    from .cdip import latest_buoys as cdip_latest_buoys
except Exception:
    cdip_latest_buoys = None


STABLE_FETCHERS = [
    ndbc_latest_buoys,
    ireland_wave_buoys_latest,
    canada_dfo_buoys_latest,
]

OPTIONAL_FETCHERS = []
if cdip_latest_buoys is not None:
    OPTIONAL_FETCHERS.append(cdip_latest_buoys)

ALL_FETCHERS = STABLE_FETCHERS + OPTIONAL_FETCHERS


def _fetcher_key(fetcher) -> str:
    return getattr(fetcher, "source_key", fetcher.__name__)


def _fetcher_name(fetcher) -> str:
    return getattr(fetcher, "source_name", _fetcher_key(fetcher))


def _selected_fetchers(enabled_sources: Optional[set[str]] = None):
    """
    Default behavior:
      no sources query param -> stable sources only

    Explicit behavior:
      sources=ndbc,ireland,canada,cdip -> only requested sources
    """
    if not enabled_sources:
        return STABLE_FETCHERS

    selected = []
    for fetcher in ALL_FETCHERS:
        if _fetcher_key(fetcher).lower() in enabled_sources:
            selected.append(fetcher)

    return selected


def _fetch_all_uncached(enabled_sources: Optional[set[str]] = None) -> dict:
    buoys: list[dict] = []
    errors: list[dict] = []

    fetchers = _selected_fetchers(enabled_sources)

    for fetcher in fetchers:
        source_key = _fetcher_key(fetcher)
        source_name = _fetcher_name(fetcher)

        try:
            source_buoys = fetcher()
            buoys.extend(
                [
                    b.to_dict() if isinstance(b, NormalizedBuoy) else b
                    for b in source_buoys
                ]
            )
        except Exception as exc:
            errors.append(
                {
                    "source_key": source_key,
                    "source": source_name,
                    "error": str(exc),
                }
            )

    deduped: dict[tuple[str, str], dict] = {}

    for b in buoys:
        key = (
            str(b.get("source_key", "")),
            str(b.get("station_id", "")),
        )

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
        "generated_at_utc": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "count": len(final_buoys),
        "buoys": final_buoys,
        "errors": errors,
        "sources_requested": sorted(enabled_sources) if enabled_sources else ["stable-default"],
        "sources_loaded": [_fetcher_key(f) for f in fetchers],
    }


def get_all_live_buoys(
    enabled_sources: Optional[set[str]] = None,
    max_age_hours: Optional[float] = 96,
) -> dict:
    source_label = ",".join(sorted(enabled_sources)) if enabled_sources else "stable-default"
    cache_key = f"all_live_buoys:{source_label}"

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
