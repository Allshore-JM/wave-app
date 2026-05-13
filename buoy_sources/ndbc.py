from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
import requests

from .cache import ttl_get
from .models import NormalizedBuoy, WaveObservation, WindObservation, safe_float, safe_int, isoformat_utc


NDBC_LATEST_OBS_URL = "https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt"
NDBC_STATION_TABLE_URL = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"


def _get_text(url: str, timeout: int = 30) -> str:
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "AllshoreSurf/1.0"})
    r.raise_for_status()
    return r.text


def _load_station_names() -> dict[str, str]:
    def fetch() -> dict[str, str]:
        names: dict[str, str] = {}

        try:
            text = _get_text(NDBC_STATION_TABLE_URL)
        except Exception:
            return names

        for line in text.splitlines():
            if not line or line.startswith("#"):
                continue

            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 5:
                continue

            station_id = parts[0].strip()
            name = parts[4].strip() or station_id

            if station_id:
                names[station_id] = name

        return names

    return ttl_get("ndbc_station_names", 6 * 3600, fetch)


def _parse_latest_obs_time(row: dict[str, str]) -> Optional[datetime]:
    try:
        return datetime(
            int(row["YYYY"]),
            int(row["MM"]),
            int(row["DD"]),
            int(row["HH"]),
            int(row["mm"]),
            tzinfo=timezone.utc,
        )
    except Exception:
        return None


def _parse_latest_obs_rows(text: str) -> list[dict[str, str]]:
    headers: list[str] = []
    rows: list[dict[str, str]] = []

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        if line.startswith("#STN"):
            headers = [h.lstrip("#") for h in line.split()]
            continue

        if line.startswith("#"):
            continue

        if not headers:
            continue

        parts = line.split()
        if len(parts) < len(headers):
            continue

        row = dict(zip(headers, parts))
        rows.append(row)

    return rows


def latest_buoys() -> list[NormalizedBuoy]:
    """
    Latest NDBC observations.

    Uses latest_obs.txt because it gives a broad, efficient station snapshot.
    For detailed spectra/components, keep your existing NDBC spectral logic.
    """
    def fetch() -> list[NormalizedBuoy]:
        station_names = _load_station_names()

        try:
            text = _get_text(NDBC_LATEST_OBS_URL)
        except Exception:
            return []

        buoys: list[NormalizedBuoy] = []

        for row in _parse_latest_obs_rows(text):
            station_id = row.get("STN", "").strip()
            lat = safe_float(row.get("LAT"))
            lon = safe_float(row.get("LON"))

            if not station_id or lat is None or lon is None:
                continue

            obs_dt = _parse_latest_obs_time(row)
            obs_iso = isoformat_utc(obs_dt)

            wvht_m = safe_float(row.get("WVHT"))
            dominant_period_s = safe_float(row.get("DPD"))
            mean_period_s = safe_float(row.get("APD"))
            mean_wave_dir = safe_float(row.get("MWD"))

            wind_speed = safe_float(row.get("WSPD"))
            gust = safe_float(row.get("GST"))
            wind_dir = safe_float(row.get("WD"))

            water_temp_c = safe_float(row.get("WTMP"))
            pressure_hpa = safe_float(row.get("PRES"))

            # Keep only stations with useful live met/wave/ocean info.
            if all(v is None for v in [wvht_m, wind_speed, water_temp_c, pressure_hpa]):
                continue

            buoy = NormalizedBuoy(
                source="NOAA NDBC",
                source_key="ndbc",
                station_id=station_id,
                name=station_names.get(station_id, station_id),
                lat=lat,
                lon=lon,
                country="US",
                last_observation_utc=obs_iso,
                wave=WaveObservation(
                    significant_height_m=wvht_m,
                    dominant_period_s=dominant_period_s,
                    mean_period_s=mean_period_s,
                    mean_direction_deg=mean_wave_dir,
                    direction_convention="from",
                ),
                wind=WindObservation(
                    speed_mps=wind_speed,
                    gust_mps=gust,
                    direction_deg=wind_dir,
                ),
                water_temp_c=water_temp_c,
                pressure_hpa=pressure_hpa,
                source_url=f"https://www.ndbc.noaa.gov/station_page.php?station={station_id}",
            ).finalize()

            buoys.append(buoy)

        return buoys

    return ttl_get("ndbc_latest_buoys", 10 * 60, fetch)


latest_buoys.source_key = "ndbc"
latest_buoys.source_name = "NOAA NDBC"
