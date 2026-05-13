from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Any
import math


M_TO_FT = 3.280839895


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, str):
        value = value.strip()
        if value == "" or value.upper() in {"MM", "NAN", "NULL", "NONE"}:
            return None

    try:
        val = float(value)
    except Exception:
        return None

    if not math.isfinite(val):
        return None

    # Common missing-value sentinels used by buoy/ERDDAP/NetCDF datasets.
    if val in {-9999, -9999.0, -999.99, -999.0, 999.0, 9999.0, 99.0}:
        return None

    return val


def safe_int(value: Any) -> Optional[int]:
    val = safe_float(value)
    if val is None:
        return None
    return int(round(val))


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_utc(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    text = str(value).strip()
    if not text:
        return None

    try:
        if text.endswith("Z"):
            return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
        return datetime.fromisoformat(text).astimezone(timezone.utc)
    except Exception:
        return None


def isoformat_utc(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def age_minutes_from_iso(iso_value: Optional[str]) -> Optional[float]:
    dt = parse_iso_utc(iso_value)
    if dt is None:
        return None
    return round((now_utc() - dt).total_seconds() / 60.0, 1)


def status_from_age_minutes(age_minutes: Optional[float]) -> str:
    if age_minutes is None:
        return "unknown"
    if age_minutes <= 180:
        return "active"
    if age_minutes <= 720:
        return "stale"
    return "old"


@dataclass
class WaveObservation:
    significant_height_m: Optional[float] = None
    significant_height_ft: Optional[float] = None
    peak_period_s: Optional[float] = None
    mean_period_s: Optional[float] = None
    dominant_period_s: Optional[float] = None
    peak_direction_deg: Optional[float] = None
    mean_direction_deg: Optional[float] = None

    # Most wave products are "wave from direction".
    # Do not apply a blanket 180-degree correction across all sources.
    direction_convention: str = "from"


@dataclass
class WindObservation:
    speed_mps: Optional[float] = None
    speed_kt: Optional[float] = None
    gust_mps: Optional[float] = None
    gust_kt: Optional[float] = None
    direction_deg: Optional[float] = None


@dataclass
class NormalizedBuoy:
    source: str
    source_key: str
    station_id: str
    name: str
    lat: float
    lon: float

    country: Optional[str] = None
    region: Optional[str] = None

    last_observation_utc: Optional[str] = None
    age_minutes: Optional[float] = None
    status: str = "unknown"

    wave: Optional[WaveObservation] = None
    wind: Optional[WindObservation] = None
    water_temp_c: Optional[float] = None
    water_temp_f: Optional[float] = None
    pressure_hpa: Optional[float] = None

    source_url: Optional[str] = None
    notes: Optional[str] = None

    def finalize(self) -> "NormalizedBuoy":
        if self.wave and self.wave.significant_height_m is not None:
            self.wave.significant_height_ft = round(self.wave.significant_height_m * M_TO_FT, 2)

        if self.wind:
            if self.wind.speed_mps is not None:
                self.wind.speed_kt = round(self.wind.speed_mps * 1.94384449, 1)
            if self.wind.gust_mps is not None:
                self.wind.gust_kt = round(self.wind.gust_mps * 1.94384449, 1)

        if self.water_temp_c is not None:
            self.water_temp_f = round((self.water_temp_c * 9.0 / 5.0) + 32.0, 1)

        self.age_minutes = age_minutes_from_iso(self.last_observation_utc)
        self.status = status_from_age_minutes(self.age_minutes)
        return self

    def to_dict(self) -> dict:
        return asdict(self.finalize())
