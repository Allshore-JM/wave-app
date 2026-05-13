from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from .cache import ttl_get
from .models import NormalizedBuoy, WaveObservation, safe_float, isoformat_utc


CDIP_LATEST_3DAY = "https://thredds.cdip.ucsd.edu/thredds/dodsC/cdip/realtime/latest_3day.nc"


def _string_value(value: Any) -> str:
    """
    Robustly convert xarray/numpy string or char-array values to normal strings.
    """
    try:
        import numpy as np
    except Exception:
        np = None

    if value is None:
        return ""

    if hasattr(value, "values"):
        value = value.values

    if np is not None and isinstance(value, np.ndarray):
        if value.ndim == 0:
            value = value.item()
        else:
            chars = []
            for item in value.flatten():
                if isinstance(item, bytes):
                    chars.append(item.decode("utf-8", errors="ignore"))
                else:
                    chars.append(str(item))
            return "".join(chars).strip().strip("_")

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore").strip().strip("_")

    return str(value).strip().strip("_")


def _np_time_to_datetime(value: Any) -> Optional[datetime]:
    try:
        import numpy as np
    except Exception:
        np = None

    if value is None:
        return None

    if np is not None:
        try:
            if isinstance(value, np.datetime64):
                seconds = value.astype("datetime64[s]").astype("int64")
                return datetime.fromtimestamp(int(seconds), tz=timezone.utc)
        except Exception:
            pass

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except Exception:
        pass

    try:
        text = str(value)
        if text.endswith("Z"):
            return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
        return datetime.fromisoformat(text).astimezone(timezone.utc)
    except Exception:
        return None


def latest_buoys() -> list[NormalizedBuoy]:
    """
    CDIP real-time latest_3day feed.

    This adapter is optional. It requires xarray + pydap in requirements.txt.
    If those packages are unavailable or THREDDS is unreachable, it returns [].
    """
    def fetch() -> list[NormalizedBuoy]:
        try:
            import numpy as np
            import xarray as xr
        except Exception:
            return []

        ds = None

        try:
            # pydap is usually easier to install on Render than full netCDF4 OPeNDAP support.
            ds = xr.open_dataset(CDIP_LATEST_3DAY, engine="pydap", decode_times=True)
        except Exception:
            try:
                ds = xr.open_dataset(CDIP_LATEST_3DAY, decode_times=True)
            except Exception:
                return []

        buoys: list[NormalizedBuoy] = []

        try:
            lats = ds["metaLatitude"].values
            lons = ds["metaLongitude"].values
            station_count = len(lats)

            wave_hs = ds["waveHs"].values
            wave_tp = ds["waveTp"].values if "waveTp" in ds else None
            wave_dp = ds["waveDp"].values if "waveDp" in ds else None
            wave_ta = ds["waveTa"].values if "waveTa" in ds else None
            wave_time = ds["waveTime"].values

            wave_time_offset = ds["waveTimeOffset"].values if "waveTimeOffset" in ds else None

            for i in range(station_count):
                lat = safe_float(lats[i])
                lon = safe_float(lons[i])

                if lat is None or lon is None:
                    continue

                hs_col = wave_hs[:, i]
                valid = np.isfinite(hs_col) & (hs_col > -900) & (hs_col >= 0)

                if not valid.any():
                    continue

                idx = int(np.where(valid)[0][-1])

                base_dt = _np_time_to_datetime(wave_time[idx])
                if base_dt is not None and wave_time_offset is not None:
                    offset = safe_float(wave_time_offset[idx, i])
                    if offset is not None and abs(offset) < 86400:
                        base_dt = base_dt + timedelta(seconds=offset)

                site_label = _string_value(ds["metaSiteLabel"][i]) if "metaSiteLabel" in ds else ""
                deploy_label = _string_value(ds["metaDeployLabel"][i]) if "metaDeployLabel" in ds else ""
                station_name = _string_value(ds["metaStationName"][i]) if "metaStationName" in ds else ""
                wmo_id = _string_value(ds["metaWMOid"][i]) if "metaWMOid" in ds else ""

                station_id = site_label or deploy_label or wmo_id or f"cdip_{i}"
                name = station_name or station_id

                buoy = NormalizedBuoy(
                    source="CDIP",
                    source_key="cdip",
                    station_id=station_id,
                    name=name,
                    lat=lat,
                    lon=lon,
                    country="US",
                    last_observation_utc=isoformat_utc(base_dt),
                    wave=WaveObservation(
                        significant_height_m=safe_float(wave_hs[idx, i]),
                        peak_period_s=safe_float(wave_tp[idx, i]) if wave_tp is not None else None,
                        mean_period_s=safe_float(wave_ta[idx, i]) if wave_ta is not None else None,
                        peak_direction_deg=safe_float(wave_dp[idx, i]) if wave_dp is not None else None,
                        direction_convention="from",
                    ),
                    source_url=f"https://cdip.ucsd.edu/m/stn_table/?stn={station_id}",
                ).finalize()

                buoys.append(buoy)

        except Exception:
            return []
        finally:
            try:
                ds.close()
            except Exception:
                pass

        return buoys

    return ttl_get("cdip_latest_buoys", 15 * 60, fetch)


latest_buoys.source_key = "cdip"
latest_buoys.source_name = "CDIP"
