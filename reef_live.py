"""GET /reef/<spot>/live -> today's condition bin, or an honest degraded state.

Nowcast SELECTOR, never a warning. CDIP 106 posts 35-70 min after the sample and waves reach
the reef 6-8 min after the buoy, so a live reading has already broken -- this picks the bin, it
never predicts a set.

THE INVARIANT (enforced here): `bin` is non-null <=> state == "ok". The client cannot draw a
stale / ambiguous / out-of-window zone because it never receives a bin for one. The payload
carries NO geometry, distance, bearing or danger_side -- all geometry lives in zones.json -- so
the live panel is structurally incapable of wiring a shoreward warning to a seaward contour.

Caching: the OBSERVATION is cached; age / stale / state / bin are RECOMPUTED every response from
observed_utc. A cached payload with stale=false served six minutes later would be a lie. Tide
predictions are deterministic -> cache the hi/lo window and evaluate the cosine at request time.

--workers 1 --threads 2: dedicated Session (Retry total=1), an outbound Semaphore(1), a per-spot
single-flight lock, a hard time budget, no-store. REEF_LIVE=0 is an instant kill switch.

Vendored VERBATIM into the wave-app repo root. Imports its sibling reef_swell / reef_sources /
reef_bins (all vendored there too).
"""
from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import reef_bins
import reef_sources
import reef_swell

_TTL_S = 420          # 7 min: buoy cadence is 30 min and it posts 35-70 min late
_NEG_TTL_S = 90       # recover fast after a failed fetch, bound the cost
_TIDE_TTL_S = 6 * 3600
_AGING_S = 3600       # watch age climb before it trips STALE
_OUTBOUND_WAIT_S = 2.0
_TIMEOUT = (3.05, 5.0)   # a live nowcast fails fast and serves the last good obs (no retry)

_LOCK = threading.Lock()
_CACHE: dict = {}        # spot -> {"fetched_ts", "obs"|None, "ttl"}
_LAST_GOOD: dict = {}    # spot -> last successful obs (any age; serve-stale-with-flag source)
_TIDE_CACHE: dict = {}   # station -> {"fetched_ts", "hilo"}
_INFLIGHT: dict = {}     # spot -> threading.Lock
_OUTBOUND = threading.Semaphore(1)


def _make_session() -> requests.Session:
    s = requests.Session()
    # total=0: no retries. A failed live poll must NOT hold a gunicorn thread for retries*backoff
    # (only 2 threads, shared with the whole wave-app); serve-stale covers the miss.
    retry = Retry(total=0, connect=0, read=0, status=0)
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


_SESSION = _make_session()


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(int(epoch), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _cfg(meta: dict) -> dict:
    live = meta.get("live", {})
    return {"band_hz": tuple(live.get("swell_band_hz", (0.035, 0.125))),
            "staleness_max_s": int(live.get("staleness_max_s", 5400)),
            "tide_station": live.get("tide_station", "1612668"),
            "bin_axes": meta["bin_axes"]}


# ---- fetch (cached observation; tide evaluated live) ------------------------

def _fetch_waves(cfg: dict) -> dict | None:
    """CDIP 106 primary. (NDBC 51201 realtime is a documented fast-follow transport fallback;
    until then a CDIP outage degrades honestly to state no_data with manual selectors intact.)"""
    try:
        return reef_swell.fetch_cdip(_SESSION, cfg["band_hz"], timeout=_TIMEOUT)
    except Exception:  # noqa: BLE001
        return None


def _get_tide(cfg: dict, now_epoch: int) -> dict | None:
    station = cfg["tide_station"]
    with _LOCK:
        ent = _TIDE_CACHE.get(station)
        hilo = ent["hilo"] if ent and now_epoch - ent["fetched_ts"] < _TIDE_TTL_S else None
    if hilo is None:
        try:
            hilo = reef_sources.fetch_tide_hilo(_SESSION, station, now_epoch, _TIMEOUT)
        except Exception:  # noqa: BLE001
            return None
        with _LOCK:
            _TIDE_CACHE[station] = {"fetched_ts": now_epoch, "hilo": hilo}
    tide_m, h_mllw = reef_sources.tide_lmsl_at(hilo, now_epoch)
    if tide_m is None:
        return None
    return {"tide_m": tide_m, "h_mllw_m": h_mllw, "datum": "LMSL",
            "uncertainty_m": reef_sources.TIDE_UNCERTAINTY_M, "station": station,
            "reference_station": reef_sources.REF_STATION,
            "msl_minus_mllw_m": reef_sources.MSL_MINUS_MLLW_M}


def _cached_obs(spot: str, cfg: dict, now_epoch: int):
    """Return (obs|None, served_from). Single-flight so 2 threads share one upstream fetch. On a
    fetch failure, SERVE THE LAST GOOD observation (serve-stale-with-flag): build_payload derives
    its age -> a still-fresh reading renders ok/aging, an old one renders stale. A transient CDIP
    blip must not blank a usable 40-min reading. served_from in {cache, fetch, stale}."""
    with _LOCK:
        ent = _CACHE.get(spot)
        if ent and now_epoch - ent["fetched_ts"] < ent["ttl"]:
            return (ent["obs"] if ent["obs"] is not None else _LAST_GOOD.get(spot)), "cache"
        lk = _INFLIGHT.setdefault(spot, threading.Lock())
    with lk:
        with _LOCK:
            ent = _CACHE.get(spot)
            if ent and now_epoch - ent["fetched_ts"] < ent["ttl"]:
                return (ent["obs"] if ent["obs"] is not None else _LAST_GOOD.get(spot)), "cache"
        obs = None
        if _OUTBOUND.acquire(timeout=_OUTBOUND_WAIT_S):
            try:
                obs = _fetch_waves(cfg)
            finally:
                _OUTBOUND.release()
        with _LOCK:
            if obs is not None:
                _LAST_GOOD[spot] = obs
                _CACHE[spot] = {"fetched_ts": now_epoch, "obs": obs, "ttl": _TTL_S}
                return obs, "fetch"
            _CACHE[spot] = {"fetched_ts": now_epoch, "obs": None, "ttl": _NEG_TTL_S}
            last = _LAST_GOOD.get(spot)
        return last, ("stale" if last is not None else "fetch")


# ---- payload assembly (state derived fresh every call) ---------------------

def _near_bin_edge(conditions: dict, axes: dict) -> list:
    """Flag axis values within their uncertainty of a bin edge (dir + tide, the first-order
    axes). A live reading that sits on a boundary must announce it, not silently pick a side."""
    out = []
    d, du = conditions.get("dir_deg"), conditions.get("dir_uncertainty_deg", 8.0)
    if d is not None:
        v = d % 360
        for e in axes["dir_deg"]["edges"]:
            for cand in (e % 360, (e % 360) + 360, (e % 360) - 360):
                if abs(v - cand) <= du:
                    out.append({"axis": "dir_deg", "value": round(d, 1), "edge": e,
                                "margin_deg": round(abs(v - cand), 1)})
    t, tu = conditions.get("tide_m"), conditions.get("tide_uncertainty_m", 0.10)
    if t is not None:
        for e in axes["tide_m"]["edges"]:
            if abs(t - e) <= tu:
                out.append({"axis": "tide_m", "value": t, "edge": e, "margin_m": round(abs(t - e), 3)})
    return out


_NOTES_NOWCAST = ("Nowcast selector, not a warning. CDIP 106 posts 35-70 min after the sample; "
                  "waves reach the reef 6-8 min after the buoy. Never a set-by-set warning.")


def live_response(spot_id: str, meta: dict, now_epoch: int | None = None) -> dict:
    """The /live payload. Pure w.r.t. `now_epoch` given the cached observation."""
    now = int(now_epoch if now_epoch is not None else time.time())
    base = {"schema_version": 1, "spot_id": spot_id, "server_utc": _iso(now), "bin": None,
            "bin_key": None, "conditions": None, "notes": [_NOTES_NOWCAST]}
    if os.environ.get("REEF_LIVE", "1") == "0":
        return {**base, "state": "disabled"}
    cfg = _cfg(meta)
    obs, served_from = _cached_obs(spot_id, cfg, now)
    tide = _get_tide(cfg, now)
    return build_payload(spot_id, cfg, obs, tide, now, served_from)


def build_payload(spot_id: str, cfg: dict, obs: dict | None, tide: dict | None,
                  now: int, served_from: str) -> dict:
    """Derive state / age / bin from a (possibly cached) observation. Separated from
    live_response so it is unit-testable with an injected obs + a fake clock."""
    base = {"schema_version": 1, "spot_id": spot_id, "server_utc": _iso(now), "bin": None,
            "bin_key": None, "conditions": None, "staleness_max_s": cfg["staleness_max_s"],
            "notes": [_NOTES_NOWCAST], "cache": {"served_from": served_from}}
    if obs is None:
        return {**base, "state": "no_data",
                "notes": base["notes"] + ["no wave data from CDIP 106 -- trust your eyes"]}
    if tide is None:
        return {**base, "state": "no_data",
                "notes": base["notes"] + ["tide unavailable; tide is a bin axis, so the bin "
                                          "cannot be resolved (never assumed 0.0)"]}

    age_s = now - int(obs["observed_utc_epoch"])
    observed_utc = _iso(obs["observed_utc_epoch"])
    cf = obs.get("check_factor_weighted")
    r1 = obs.get("r1_bar", 0.0)
    conditions = {
        "hs_swell_ft": obs.get("hs_ft"), "hs_swell_m": obs.get("hs_m"),
        "tp_s": obs.get("tp_s"), "tp_kind": "band_peak",
        "dir_deg": obs.get("dir_deg"), "dir_kind": "from_true",
        "dir_spread_deg": obs.get("spread_deg"), "dir_uncertainty_deg": 8.0, "r1_bar": r1,
        "bimodal": obs.get("bimodal", False), "band_hz": list(cfg["band_hz"]),
        "band_m0_frac": obs.get("band_m0_frac"),
        "tide_m": tide["tide_m"], "tide_uncertainty_m": tide["uncertainty_m"], "tide_datum": "LMSL",
    }
    quality = {"check_factor_weighted": cf, "check_factor_max": obs.get("check_factor_max"),
               "cf_saturated_m0_frac": obs.get("cf_saturated_m0_frac"),
               "bands_used": obs.get("bands_used"), "bands_masked": obs.get("bands_masked"),
               "bands_masked_hz": obs.get("bands_masked_hz"), "record_flag": obs.get("record_flag")}
    sources = [{"role": "waves", "id": obs.get("source"), "fallback": obs.get("fallback", False),
                "transport": obs.get("transport"), "observed_utc": observed_utc, "age_s": age_s,
                "station": "CDIP 106 / NDBC 51201 -- Waimea Bay"},
               {"role": "tide", "id": f"coops_{tide['station']}", "fallback": False,
                "station": f"Haleiwa {tide['station']} (subordinate of {tide['reference_station']})",
                "datum_tie_m": tide["msl_minus_mllw_m"]}]
    common = {**base, "conditions": conditions, "observed_utc": observed_utc, "age_s": age_s,
              "aging": age_s > _AGING_S, "quality": quality, "sources": sources}

    # ---- state dispatch (bin filled ONLY on "ok") --------------------------
    if age_s > cfg["staleness_max_s"]:
        return {**common, "state": "stale", "stale": True,
                "notes": base["notes"] + [f"last observation {age_s // 60} min ago -- no live bin"]}
    if obs.get("tp_s") is None or r1 < 0.55 or obs.get("bimodal"):
        why = ("two swells in the band -- the mean direction is not physical"
               if obs.get("bimodal") else f"direction poorly defined (r1={r1:.2f} < 0.55)")
        return {**common, "state": "ambiguous", "notes": base["notes"] + [why + "; select manually"]}

    det = reef_bins.bin_of_detail(obs["hs_ft"], obs["tp_s"], obs["dir_deg"], tide["tide_m"],
                                  cfg["bin_axes"])
    if det["index"] is None:
        note = ("swell direction outside the modeled arc" if det["reason"] == "outside_window"
                else "below the breaking threshold")
        return {**common, "state": det["reason"], "notes": base["notes"] + [note]}

    idx = det["index"]
    degraded = bool((cf and cf > 2.0) or obs.get("fallback") or det["extrapolated"])
    quality["degraded"] = degraded
    near = _near_bin_edge(conditions, cfg["bin_axes"])
    notes = list(base["notes"])
    if obs.get("bands_masked"):
        notes.append(f"{obs['bands_masked']} band(s) masked by CDIP QC")
    if degraded:
        notes.append("degraded: check-factor high / extrapolated / fallback -- chips shown")
    return {**common, "state": "ok", "stale": False, "bin": list(idx),
            "bin_key": reef_bins.bin_key(idx, cfg["bin_axes"]),
            "extrapolated": det["extrapolated"], "near_bin_edge": near, "notes": notes}
