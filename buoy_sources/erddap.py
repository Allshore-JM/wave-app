from __future__ import annotations

from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from typing import Any
import requests

from .cache import ttl_get
from .models import NormalizedBuoy, WaveObservation, WindObservation, safe_float, isoformat_utc


def _erddap_json(base_url: str, dataset_id: str, query: str, timeout: int = 45) -> list[dict[str, Any]]:
    """
    Fetch an ERDDAP tabledap .json response and return rows as dictionaries.
    """
    encoded_query = quote(query, safe=',&=():"><-')
    url = f"{base_url.rstrip('/')}/tabledap/{dataset_id}.json?{encoded_query}"

    r = requests.get(url, timeout=timeout, headers={"User-Agent": "AllshoreSurf/1.0"})
    r.raise_for_status()
    payload = r.json()

    table = payload.get("table", {})
    columns = table.get("columnNames", [])
    rows = table.get("rows", [])

    return [dict(zip(columns, row)) for row in rows]


def _latest_by_station_query(columns: list[str], station_col: str, time_col: str, hours_back: int) -> str:
    start = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).replace(microsecond=0)
    start_iso = start.isoformat().replace("+00:00", "Z")

    return (
        f"{','.join(columns)}"
        f"&{time_col}>={start_iso}"
        f"&orderByMax(\"{station_col},{time_col}\")"
    )


def ireland_wave_buoys_latest() -> list[NormalizedBuoy]:
    """
    Marine Institute Ireland Wave Rider Buoy Network.

    SignificantWaveHeight is listed in centimeters in the public metadata,
    so it is converted to meters here.
    """
    def fetch() -> list[NormalizedBuoy]:
        base_url = "https://erddap.marine.ie/erddap"
        dataset_id = "IWaveBNetwork30Min"

        columns = [
            "longitude",
            "latitude",
            "time",
            "station_id",
            "PeakPeriod",
            "PeakDirection",
            "UpcrossPeriod",
            "SignificantWaveHeight",
            "SeaTemperature",
        ]

        query = _latest_by_station_query(
            columns=columns,
            station_col="station_id",
            time_col="time",
            hours_back=96,
        )

        try:
            records = _erddap_json(base_url, dataset_id, query)
        except Exception:
            return []

        buoys: list[NormalizedBuoy] = []

        for rec in records:
            lat = safe_float(rec.get("latitude"))
            lon = safe_float(rec.get("longitude"))
            station_id = str(rec.get("station_id") or "").strip()

            if lat is None or lon is None or not station_id:
                continue

            hs_cm = safe_float(rec.get("SignificantWaveHeight"))
            hs_m = None if hs_cm is None else hs_cm / 100.0

            buoy = NormalizedBuoy(
                source="Marine Institute Ireland",
                source_key="ireland",
                station_id=station_id,
                name=station_id,
                lat=lat,
                lon=lon,
                country="IE",
                last_observation_utc=str(rec.get("time")) if rec.get("time") else None,
                wave=WaveObservation(
                    significant_height_m=hs_m,
                    peak_period_s=safe_float(rec.get("PeakPeriod")),
                    mean_period_s=safe_float(rec.get("UpcrossPeriod")),
                    peak_direction_deg=safe_float(rec.get("PeakDirection")),
                    direction_convention="from",
                ),
                water_temp_c=safe_float(rec.get("SeaTemperature")),
                source_url=f"{base_url}/tabledap/{dataset_id}.html",
            ).finalize()

            buoys.append(buoy)

        return buoys

    return ttl_get("ireland_wave_buoys_latest", 15 * 60, fetch)


def canada_dfo_buoys_latest() -> list[NormalizedBuoy]:
    """
    CIOOS Pacific ERDDAP mirror of DFO MEDS / Environment and Climate Change Canada buoys.

    VCAR = characteristic significant wave height, meters.
    VTPK = wave spectrum peak period, seconds.
    """
    def fetch() -> list[NormalizedBuoy]:
        base_url = "https://data.cioospacific.ca/erddap"
        dataset_id = "DFO_MEDS_BUOYS"

        columns = [
            "STN_ID",
            "time",
            "latitude",
            "longitude",
            "VCAR",
            "VTPK",
            "VWH",
            "VTP",
            "WDIR",
            "WSPD",
            "SSTP",
            "ATMS",
            "Q_FLAG",
        ]

        query = _latest_by_station_query(
            columns=columns,
            station_col="STN_ID",
            time_col="time",
            hours_back=96,
        )

        try:
            records = _erddap_json(base_url, dataset_id, query)
        except Exception:
            return []

        buoys: list[NormalizedBuoy] = []

        for rec in records:
            lat = safe_float(rec.get("latitude"))
            lon = safe_float(rec.get("longitude"))
            station_id = str(rec.get("STN_ID") or "").strip()

            if lat is None or lon is None or not station_id:
                continue

            hs_m = safe_float(rec.get("VCAR"))
            if hs_m is None:
                hs_m = safe_float(rec.get("VWH"))

            peak_period = safe_float(rec.get("VTPK"))
            if peak_period is None:
                peak_period = safe_float(rec.get("VTP"))

            buoy = NormalizedBuoy(
                source="Canada DFO / ECCC",
                source_key="canada",
                station_id=station_id,
                name=station_id,
                lat=lat,
                lon=lon,
                country="CA",
                last_observation_utc=str(rec.get("time")) if rec.get("time") else None,
                wave=WaveObservation(
                    significant_height_m=hs_m,
                    peak_period_s=peak_period,
                    direction_convention="from",
                ),
                wind=WindObservation(
                    speed_mps=safe_float(rec.get("WSPD")),
                    direction_deg=safe_float(rec.get("WDIR")),
                ),
                water_temp_c=safe_float(rec.get("SSTP")),
                pressure_hpa=safe_float(rec.get("ATMS")),
                source_url=f"{base_url}/tabledap/{dataset_id}.html",
                notes=f"Q_FLAG={rec.get('Q_FLAG')}" if rec.get("Q_FLAG") is not None else None,
            ).finalize()

            buoys.append(buoy)

        return buoys

    return ttl_get("canada_dfo_buoys_latest", 15 * 60, fetch)


ireland_wave_buoys_latest.source_key = "ireland"
ireland_wave_buoys_latest.source_name = "Marine Institute Ireland"

canada_dfo_buoys_latest.source_key = "canada"
canada_dfo_buoys_latest.source_name = "Canada DFO / ECCC"
