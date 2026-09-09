"""Deterministic fake live-buoy providers shared by the golden capture and the memo tests.

The fixture exercises every dedup/priority path of buoy_sources.merge_stations:
  * NDBC + CDIP co-located (<1 km)            -> CDIP hidden as dup_of the NDBC marker
  * MI-IE "M6" + CEFAS "M6 Buoy" 3.6 km apart -> name-key merge inside name_radius_km
  * AODN spectra buoy vs AusWaves bulk        -> spectra wins (priority 90 vs 70)
  * a stale station (latest_time older than stale_after_sec)
  * an EMPTY provider (QLD) and a RAISING provider (SMHI) -> both yield [] (fail-soft)
Frozen clock: NOW_EPOCH. Every _fetch_stations returns fresh dicts, like a real fetch.
"""
import copy
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import buoy_sources  # noqa: E402

NOW_EPOCH = 1_788_951_600.0        # 2026-09-09T11:00:00Z (FRESH is 1 h old, OLD is 49 h old)
FRESH = "2026-09-09T10:00:00Z"
OLD = "2026-09-07T10:00:00Z"         # 48 h old -> is_stale for every provider


class _Fake(buoy_sources.BuoyProvider):
    rows = []                         # per-class fixture rows
    fetch_calls = 0

    def _fetch_stations(self):
        type(self).fetch_calls += 1
        return copy.deepcopy(self.rows)

    def latest(self, local_id):
        return {"time_utc": FRESH, "hs_m": 1.5, "tp_s": 12.0, "dir_deg": 315}


class FakeNDBC(_Fake):
    source = "NDBC"; source_name = "NOAA NDBC"; source_url = "https://www.ndbc.noaa.gov"
    license_label = "Public domain (US Govt)"; attribution_text = "Source: NOAA National Data Buoy Center"
    stale_after_sec = 6 * 3600; list_ttl_sec = 1800
    capabilities = buoy_sources._caps(bulk=True, recent_history=True, directional=True, spectra=True, partitions=True)
    rows = [{"local_id": "51201", "name": "Waimea Bay, HI", "lat": 21.671, "lon": -158.117},
            {"local_id": "51001", "name": "NW Hawaii", "lat": 24.4, "lon": -162.0},
            {"local_id": "46001", "name": "Gulf of Alaska", "lat": 56.3, "lon": -148.0}]


class FakeCDIP(_Fake):
    source = "CDIP"; source_name = "CDIP"; source_url = "https://cdip.ucsd.edu"
    license_label = "Open"; attribution_text = "Source: CDIP / Scripps"
    stale_after_sec = 6 * 3600; list_ttl_sec = 3600
    capabilities = buoy_sources._caps(bulk=True, recent_history=True)
    rows = [{"local_id": "106", "name": "Waimea Bay", "lat": 21.672, "lon": -158.118, "latest_time": FRESH},
            {"local_id": "201", "name": "Scripps Nearshore", "lat": 32.87, "lon": -117.27, "latest_time": FRESH}]


class FakeQLD(_Fake):                 # successful EMPTY list
    source = "QLD"; source_name = "Queensland"; source_url = "https://qld"; license_label = "CC-BY"
    attribution_text = "Source: QLD"; list_ttl_sec = 21600
    capabilities = buoy_sources._caps(bulk=True, recent_history=True)
    rows = []


class FakeAODN(_Fake):
    source = "AODN"; source_name = "AODN"; source_url = "https://aodn"; license_label = "CC-BY 4.0"
    attribution_text = "Source: IMOS/AODN NRT wave buoys (CC-BY 4.0)"
    stale_after_sec = 15 * 3600; list_ttl_sec = 1800
    capabilities = buoy_sources._caps(bulk=True, recent_history=True)
    rows = [{"local_id": "SYD", "name": "Sydney", "lat": -33.77, "lon": 151.41, "latest_time": FRESH,
             "capabilities": buoy_sources._caps(bulk=True, recent_history=True, directional=True, spectra=True, partitions=True)},
            {"local_id": "PER", "name": "Perth", "lat": -31.97, "lon": 115.73, "latest_time": OLD}]


class FakeAusWaves(_Fake):
    source = "AusWaves"; source_name = "AusWaves"; source_url = "https://auswaves"; license_label = "Open"
    attribution_text = "Source: AusWaves"; stale_after_sec = 12 * 3600; list_ttl_sec = 1800
    capabilities = buoy_sources._caps(bulk=True, recent_history=True)
    rows = [{"local_id": "sydney", "name": "Sydney", "lat": -33.771, "lon": 151.412, "latest_time": FRESH},
            {"local_id": "brisbane", "name": "Brisbane", "lat": -27.49, "lon": 153.63, "latest_time": FRESH}]


class FakeMIIE(_Fake):
    source = "MI-IE"; source_name = "Marine Institute"; source_url = "https://marine.ie"; license_label = "CC-BY"
    attribution_text = "Source: Marine Institute (Ireland)"; stale_after_sec = 6 * 3600; list_ttl_sec = 1800
    capabilities = buoy_sources._caps(bulk=True, recent_history=True)
    rows = [{"local_id": "M6", "name": "M6", "lat": 53.07, "lon": -15.88, "latest_time": FRESH}]


class FakeCEFAS(_Fake):
    source = "CEFAS"; source_name = "CEFAS WaveNet"; source_url = "https://wavenet"; license_label = "OGL"
    attribution_text = "Source: CEFAS WaveNet"; stale_after_sec = 6 * 3600; list_ttl_sec = 1800
    capabilities = buoy_sources._caps(bulk=True)
    rows = [{"local_id": "M6B", "name": "M6 Buoy", "lat": 53.10, "lon": -15.90, "latest_time": FRESH},
            {"local_id": "PERR", "name": "Perranporth", "lat": 50.35, "lon": -5.17, "latest_time": FRESH}]


class FakeSMHI(_Fake):                # RAISING provider
    source = "SMHI"; source_name = "SMHI"; source_url = "https://smhi"; license_label = "CC-BY"
    attribution_text = "Source: SMHI"; list_ttl_sec = 3600
    capabilities = buoy_sources._caps(bulk=True, recent_history=True)

    def _fetch_stations(self):
        type(self).fetch_calls += 1
        raise RuntimeError("SMHI down")


class FakeRWS(_Fake):
    source = "RWS"; source_name = "Rijkswaterstaat"; source_url = "https://rws"; license_label = "Open"
    attribution_text = "Source: RWS"; list_ttl_sec = 21600
    capabilities = buoy_sources._caps(bulk=True, recent_history=True)
    rows = [{"local_id": "K13", "name": "K13", "lat": 53.22, "lon": 3.22, "latest_time": FRESH}]


class FakeCMEMS(_Fake):
    source = "CMEMS"; source_name = "Copernicus Marine"; source_url = "https://marine.copernicus.eu"
    license_label = "CMEMS"; attribution_text = "Source: Copernicus Marine Service in situ NRT"
    stale_after_sec = 36 * 3600; list_ttl_sec = 10800
    capabilities = buoy_sources._caps(bulk=True)
    rows = [{"local_id": "6200001", "name": "Biscay", "lat": 45.2, "lon": -5.0, "latest_time": FRESH},
            {"local_id": "K13X", "name": "K13", "lat": 53.2205, "lon": 3.2205, "latest_time": FRESH}]  # <1 km of RWS K13


FAKE_CLASSES = [FakeNDBC, FakeCDIP, FakeQLD, FakeAODN, FakeAusWaves, FakeMIIE, FakeCEFAS, FakeSMHI, FakeRWS, FakeCMEMS]


def make_providers():
    for c in FAKE_CLASSES:
        c.fetch_calls = 0
    return [c(http=None) for c in FAKE_CLASSES]


def fake_tz(lat, lon):
    """Deterministic stand-in for app._buoy_tz_cached (timezonefinder is not under test)."""
    if lat is None or lon is None:
        return "UTC"
    return "Etc/GMT%+d" % (-int(round(float(lon) / 15.0)))


class FrozenTime:
    """Drop-in for the `time` module inside buoy_sources (only .time() is used there)."""
    def __init__(self, now=NOW_EPOCH):
        self.now = now

    def time(self):
        return self.now

    def __getattr__(self, name):          # sleep/monotonic etc. -> real module
        import time as _t
        return getattr(_t, name)
