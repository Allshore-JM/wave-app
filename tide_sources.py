"""NOAA CO-OPS tide predictions for the map's tide stations (plan section 38).

Source: the CO-OPS data API (public domain; NOS asks for attribution), product=predictions, datum MLLW, metres, GMT.
Two kinds of station (tide_stations.json, built by tools/tides/fetch_stations.py):

- HARMONIC ("R", 1,260): NOAA serves a curve at any interval; we take 30 minutes, plus the exact highs and lows.
- SUBORDINATE ("S", 2,242): NOAA serves ONLY the highs and lows, each one its reference station's extreme moved by a
  published time offset and height ratio (checked at 25 of 25 stations, to the minute). The curve between them is the
  REFERENCE station's own NOAA curve, re-timed and re-scaled between each pair of extremes ("reference" method), so the
  stands and asymmetric rises of mixed and shallow-water tides survive. Measured on harmonic pairs where the truth is
  known: San Diego 3 cm worst (a plain cosine between the extremes: 31 cm), Seattle 4 (24), Galveston 8 (23),
  Nawiliwili 9 (17). Where an extreme has no partner, that stretch falls back to a cosine between the extremes.

A forecast covers 18 days from 00:00 UTC yesterday (every zone's "today from midnight" plus 16 days lies inside it),
the curve 12 h more on each side; it never changes, so it is kept per station per UTC day. NOAA is asked at most twice per station per day (curve + extremes)
and a subordinate station shares its reference's cached curve. No retries here: a failure is answered at once (the page
retries) and remembered for a minute so a dead upstream is not asked by every visitor.

Nothing here is a live buoy provider: it is not in the live-buoy list, its scheduler or /healthz."""
import json
import math
import os
import re
import threading
import time
import weakref
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import sky

API = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
APPLICATION = "allshoresurf.com"          # CO-OPS asks every client to name itself
ATTRIBUTION = "NOAA CO-OPS tide predictions (tidesandcurrents.noaa.gov)"
STEP_S = 1800                             # the curve's step: 30 minutes
SPAN_DAYS = 18
SPAN_S = SPAN_DAYS * 86400
LEAD_S = 12 * 3600                        # the curve runs this long before and after the window (a subordinate is shaped
                                          # on its reference's curve to the window's ends); the extremes twice as long
CURVE_HOURS = SPAN_DAYS * 24 + 2 * 12
HILO_HOURS = SPAN_DAYS * 24 + 4 * 12
POINTS = CURVE_HOURS * 2 + 1              # both ends included (NOAA answers 913 values for range=456)
MAX_GAP_S = 20 * 3600                     # longest stretch between a high and a low (diurnal Gulf tides: 16.6 h)
PAIR_TOL_S = 3600                         # a subordinate extreme's partner: its reference extreme within an hour of t - offset
MAX_BODY = 512 * 1024
ID_RE = re.compile(r"[A-Za-z0-9]{3,10}")

NO_PREDICTIONS = "NOAA publishes no tide predictions for this station"
UNAVAILABLE = "NOAA's tide service could not be reached; try again in a moment"
BUSY = "The server is busy with other tide stations; try again in a moment"


class TideError(Exception):
    """A failure that may pass (network, a malformed answer): never cached for long."""


# ---------------------------------------------------------------------------------------------- NOAA's answers

def noaa_time(text):
    """'2026-10-08 02:38' (GMT) -> epoch seconds."""
    return int(datetime.strptime(text, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp())


def noaa_error(doc):
    """The message of NOAA's error answer ({"error": {"message": ...}}, sent with HTTP 200), else None."""
    if isinstance(doc, dict) and "error" in doc:
        err = doc["error"]
        msg = err.get("message") if isinstance(err, dict) else err
        return str(msg or "error").strip()
    return None


def _rows(doc, key):
    rows = doc.get(key) if isinstance(doc, dict) else None
    if not isinstance(rows, list):
        raise TideError("unexpected answer from NOAA")
    return rows


def parse_predictions(doc):
    """[(epoch, metres)] in time order; rows without a time or a number are skipped."""
    out = []
    for r in _rows(doc, "predictions"):
        try:
            out.append((noaa_time(r["t"]), float(r["v"])))
        except (KeyError, TypeError, ValueError):
            continue
    out.sort()
    return out


def parse_hilo(doc):
    """[(epoch, metres, 'H'|'L')] in time order. NOAA may write HH / LH (higher / lower high) and HL / LL: the last
    letter says which."""
    out = []
    for r in _rows(doc, "predictions"):
        try:
            kind = str(r["type"]).strip().upper()[-1:]
            if kind not in ("H", "L"):
                continue
            out.append((noaa_time(r["t"]), float(r["v"]), kind))
        except (KeyError, TypeError, ValueError):
            continue
    out.sort()
    return out


def parse_water_level(doc):
    """Observed water level [(epoch, metres)] in time order (6-minute samples; empty values skipped)."""
    out = []
    for r in _rows(doc, "data"):
        try:
            v = r.get("v")
            if v in (None, ""):
                continue
            out.append((noaa_time(r["t"]), float(v)))
        except (AttributeError, KeyError, TypeError, ValueError):
            continue
    out.sort()
    return out


def url(station, **params):
    q = {"station": station, "datum": "MLLW", "units": "metric", "time_zone": "gmt", "format": "json",
         "application": APPLICATION}
    q.update(params)
    return API + "?" + urlencode(q)


def begin_of(now_s):
    """00:00 UTC yesterday: the window's first instant (the curve starts LEAD_S earlier)."""
    day = datetime.fromtimestamp(now_s, tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return int((day - timedelta(days=1)).timestamp())


def _stamp(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y%m%d %H:%M")


# ---------------------------------------------------------------------------------------------- the curve

def on_grid(points, begin, n=POINTS, step=STEP_S):
    """Values on the grid begin + i * step (None where NOAA gave none)."""
    out = [None] * n
    for t, v in points:
        i, r = divmod(t - begin, step)
        if r == 0 and 0 <= i < n:
            out[i] = round(v, 3)
    return out


def _valid(a, b):
    """Two consecutive extremes bound a curve only when they alternate and are not too far apart (else an extreme is
    missing and anything drawn between them would be invented)."""
    return a[2] != b[2] and 0 < b[0] - a[0] <= MAX_GAP_S


def _bracket(ex, t):
    """Index i with ex[i][0] <= t <= ex[i+1][0] (the earlier pair at a shared instant), or None."""
    lo, hi = 0, len(ex) - 1
    if hi < 1 or t < ex[0][0] or t > ex[-1][0]:
        return None
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if ex[mid][0] <= t:
            lo = mid
        else:
            hi = mid
    return lo


def cosine_at(ex, t):
    """A half cosine between the two extremes around t (exact at both, monotone between), or None outside them or
    across a stretch that is not a valid high-low pair."""
    i = _bracket(ex, t)
    if i is None or not _valid(ex[i], ex[i + 1]):
        return None
    (t1, h1, _), (t2, h2, _) = ex[i], ex[i + 1]
    return h1 + (h2 - h1) * (1.0 - math.cos(math.pi * (t - t1) / (t2 - t1))) / 2.0


def cosine_grid(ex, begin, n=POINTS, step=STEP_S):
    out = []
    for i in range(n):
        v = cosine_at(ex, begin + i * step)
        out.append(None if v is None else round(v, 3))
    return out


def pair_reference(sub_ex, ref_ex, offsets):
    """For each subordinate extreme, the index of its reference extreme: the same kind, nearest to t minus the
    published time offset (minutes; high and low have their own), within PAIR_TOL_S; else None."""
    oh, ol = offsets
    by_kind = {k: [(r[0], j) for j, r in enumerate(ref_ex) if r[2] == k] for k in ("H", "L")}
    out = []
    for t, _, k in sub_ex:
        off = oh if k == "H" else ol
        if off is None:
            out.append(None)
            continue
        target = t - int(off) * 60
        cands = by_kind[k]
        best = min(cands, key=lambda c: abs(c[0] - target)) if cands else None
        out.append(best[1] if best and abs(best[0] - target) <= PAIR_TOL_S else None)
    return out


def _value_at(grid, begin, t, step=STEP_S):
    """A curve's value at t, linear between its grid points; None outside it or next to a gap."""
    x = (t - begin) / step
    i = int(math.floor(x))
    if i < 0 or i >= len(grid):
        return None
    if x == i:
        return grid[i]
    if i + 1 >= len(grid) or grid[i] is None or grid[i + 1] is None:
        return None
    return grid[i] + (grid[i + 1] - grid[i]) * (x - i)


def reference_at(sub_ex, ref_ex, partner, ref_grid, ref_begin, t):
    """The subordinate curve at t: within a valid pair of its extremes whose partners are CONSECUTIVE reference
    extremes, the reference curve mapped linearly in time from the partners' span onto this span and scaled so the
    partners' heights land on these heights. -> (value, True), or (cosine value or None, False) elsewhere."""
    j = _bracket(sub_ex, t)
    if j is None or not _valid(sub_ex[j], sub_ex[j + 1]):
        return None, False
    p1, p2 = partner[j], partner[j + 1]
    if p1 is not None and p2 == p1 + 1:
        (a1, h1, _), (a2, h2, _) = sub_ex[j], sub_ex[j + 1]
        r1, r2 = ref_ex[p1][0], ref_ex[p2][0]
        # the reference curve's OWN values at the partners' instants (its 30-minute samples sit a little inside the
        # exact peaks): so the mapped curve passes through this station's extremes exactly
        g1, g2 = _value_at(ref_grid, ref_begin, r1), _value_at(ref_grid, ref_begin, r2)
        if r2 > r1 and g1 is not None and g2 is not None and abs(g2 - g1) > 1e-6:
            rv = _value_at(ref_grid, ref_begin, r1 + (t - a1) / (a2 - a1) * (r2 - r1))
            if rv is not None:
                return h1 + (rv - g1) / (g2 - g1) * (h2 - h1), True
    return cosine_at(sub_ex, t), False


def reference_grid(sub_ex, ref_ex, ref_grid, ref_begin, offsets, begin, n=POINTS, step=STEP_S):
    """The subordinate curve on the grid (reference_at at every point); -> (values, how many came from the reference)."""
    partner = pair_reference(sub_ex, ref_ex, offsets)
    out, used = [], 0
    for i in range(n):
        v, from_ref = reference_at(sub_ex, ref_ex, partner, ref_grid, ref_begin, begin + i * step)
        used += from_ref
        out.append(None if v is None else round(v, 3))
    return out, used


def night_bands(lat, lon, begin, end):
    """[[start, end]] (epoch) of the nights between begin and end: last light to first light (the sun's centre below
    -6 deg, the forecast graphs' rule). None without PyEphem."""
    if not sky.AVAILABLE:
        return None
    b = datetime.fromtimestamp(begin, tz=timezone.utc)
    e = datetime.fromtimestamp(end, tz=timezone.utc)
    evs = sky.events(lat, lon, b, e)
    if evs is None:
        return None
    night_from = begin if sky.sun_state(sky._observer(lat, lon), b) == "night" else None
    out = []
    for utc, kind in evs:
        t = int(utc.timestamp())
        if kind == "dusk" and night_from is None:
            night_from = t
        elif kind == "dawn" and night_from is not None:
            if t > night_from:
                out.append([night_from, t])
            night_from = None
    if night_from is not None and end > night_from:
        out.append([night_from, end])
    return out


# ---------------------------------------------------------------------------------------------- the stations

def load_stations(path):
    """tide_stations.json -> {id: station dict}. Fields per row are named in the file's "fields"."""
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    fields = doc["fields"]
    out = {}
    for row in doc["stations"]:
        s = dict(zip(fields, row))
        out[s["id"]] = s
    return out, doc


def client_list(doc):
    """The page's copy of the list: [id, name, lat, lon, type, tz, obs] per station (no offsets / references)."""
    fields = doc["fields"]
    keep = [fields.index(k) for k in ("id", "name", "lat", "lon", "type", "tz", "obs")]
    return {"captured": doc.get("captured"), "source": doc.get("source"),
            "fields": ["id", "name", "lat", "lon", "type", "tz", "obs"],
            "stations": [[row[i] for i in keep] for row in doc["stations"]]}


def valid_id(sid):
    return isinstance(sid, str) and bool(ID_RE.fullmatch(sid))


# ---------------------------------------------------------------------------------------------- the service

_SERVICES = weakref.WeakSet()


class TideService:
    """Forecasts and observations per station, cached, one upstream request per key at a time.

    fetch(url, max_bytes) -> bytes (raises on any failure); now() -> epoch seconds. Answers are (status, payload) with
    status "ok", "final" (NOAA has nothing for this station: the same whenever asked), "busy" or "error" (may pass)."""

    def __init__(self, stations, fetch, now=time.time, max_entries=256, final_ttl=24 * 3600, error_ttl=60,
                 obs_ttl=900, builds=2, wait_s=2.0):
        self.stations = stations
        self._fetch = fetch
        self._now = now
        self.max_entries = max_entries
        self.final_ttl, self.error_ttl, self.obs_ttl = final_ttl, error_ttl, obs_ttl
        self._builds_n, self.wait_s = builds, wait_s
        self._reset_locks()
        self._cache = OrderedDict()           # key -> (expires, status, payload)
        _SERVICES.add(self)

    def _reset_locks(self):
        self._lock = threading.Lock()
        self._inflight = {}
        self._builds = threading.BoundedSemaphore(self._builds_n)

    # -- cache
    def _get(self, key):
        with self._lock:
            hit = self._cache.get(key)
            if hit is None:
                return None
            if hit[0] <= self._now():
                del self._cache[key]
                return None
            self._cache.move_to_end(key)
            return hit[1], hit[2]

    def _put(self, key, status, payload, ttl):
        with self._lock:
            self._cache[key] = (self._now() + ttl, status, payload)
            self._cache.move_to_end(key)
            while len(self._cache) > self.max_entries:
                self._cache.popitem(last=False)

    def _key_lock(self, key):
        with self._lock:
            return self._inflight.setdefault(key, threading.Lock())

    def _release_key(self, key, lk):
        lk.release()
        with self._lock:
            if self._inflight.get(key) is lk and not lk.locked():
                del self._inflight[key]

    def _json(self, u):
        try:
            body = self._fetch(u, MAX_BODY)
        except Exception as exc:
            raise TideError(str(exc) or "fetch failed") from None
        try:
            return json.loads(body)
        except ValueError:
            raise TideError("NOAA's answer is not JSON") from None

    def _answer(self, key, build, ttl_ok):
        """Cached answer for key, else one build at a time (others wait up to wait_s, then 'busy')."""
        hit = self._get(key)
        if hit is not None:
            return hit
        deadline = time.monotonic() + self.wait_s
        lk = self._key_lock(key)
        if not lk.acquire(timeout=self.wait_s):
            return "busy", {"error": BUSY}
        try:
            hit = self._get(key)
            if hit is not None:
                return hit
            if not self._builds.acquire(timeout=max(0.0, deadline - time.monotonic())):
                return "busy", {"error": BUSY}
            try:
                status, payload = build()
            except TideError:
                status, payload = "error", {"error": UNAVAILABLE}
            finally:
                self._builds.release()
            ttl = ttl_ok if status == "ok" else self.final_ttl if status == "final" else self.error_ttl
            self._put(key, status, payload, ttl)
            return status, payload
        finally:
            self._release_key(key, lk)

    # -- forecasts
    def forecast(self, sid):
        st = self.stations.get(sid) if valid_id(sid) else None
        if st is None:
            return "final", {"id": sid, "error": "Unknown tide station"}
        begin = begin_of(self._now())
        return self._answer(("p", sid, begin), lambda: self._build(st, begin), self._until_next_day(begin))

    def _until_next_day(self, begin):
        return max(60, begin + 2 * 86400 - self._now())        # the key rolls over at the next UTC midnight

    def _harmonic(self, sid, begin):
        """(curve or None, extremes or None, error message or None) of a harmonic station: NOAA's 30-minute curve and
        its highs and lows. A NOAA error on one leaves the other; on both: the message."""
        hilo_doc = self._json(url(sid, product="predictions", interval="hilo", begin_date=_stamp(begin - 2 * LEAD_S),
                                  range=HILO_HOURS))
        curve_doc = self._json(url(sid, product="predictions", interval="30", begin_date=_stamp(begin - LEAD_S),
                                   range=CURVE_HOURS))
        e1, e2 = noaa_error(hilo_doc), noaa_error(curve_doc)
        ex = None if e1 else parse_hilo(hilo_doc)
        grid = None if e2 else on_grid(parse_predictions(curve_doc), begin - LEAD_S)
        if grid is not None and not any(v is not None for v in grid):
            grid = None
        return grid, ex, (e1 if e1 and e2 else None)

    def _reference_curve(self, ref, begin, deadline):
        """The reference station's curve and extremes, from the cache or fetched under the reference's own key lock
        (an R station never waits on another station, so S -> R is the only lock order). None when unusable."""
        st = self.stations.get(ref)
        if not st or st.get("type") != "R":
            return None
        key = ("p", ref, begin)
        hit = self._get(key)
        if hit is None:
            lk = self._key_lock(key)
            if not lk.acquire(timeout=max(0.0, deadline - time.monotonic())):
                return None
            try:
                hit = self._get(key)
                if hit is None:
                    try:
                        hit = self._build(st, begin)
                    except TideError:
                        return None
                    if hit[0] == "ok":
                        self._put(key, hit[0], hit[1], self._until_next_day(begin))
            finally:
                self._release_key(key, lk)
        status, payload = hit
        if status != "ok" or payload.get("method") != "harmonic":
            return None
        return payload["v"], [tuple(e) for e in payload["hilo"]]

    def _build(self, st, begin):
        sid = st["id"]
        method, ref_used = None, None
        if st.get("type") == "R":
            grid, ex, err = self._harmonic(sid, begin)
            if err:
                return "final", {"id": sid, "error": NO_PREDICTIONS, "noaa": err[:200], "final": True}
            if grid is not None:
                method = "harmonic"
            elif ex:
                grid, method = cosine_grid(ex, begin - LEAD_S), "cosine"
        else:
            doc = self._json(url(sid, product="predictions", interval="hilo", begin_date=_stamp(begin - 2 * LEAD_S),
                                 range=HILO_HOURS))
            err = noaa_error(doc)
            if err:
                return "final", {"id": sid, "error": NO_PREDICTIONS, "noaa": err[:200], "final": True}
            ex = parse_hilo(doc)
            ref = self._reference_curve(st.get("ref"), begin, time.monotonic() + self.wait_s) if st.get("ref") else None
            if ref is not None and ex:
                start = begin - LEAD_S
                grid, used = reference_grid(ex, ref[1], ref[0], start, (st.get("oh"), st.get("ol")), start)
                method, ref_used = ("reference", st.get("ref")) if used else ("cosine", None)
            elif ex:
                grid, method = cosine_grid(ex, begin - LEAD_S), "cosine"
            else:
                grid = None
        if grid is None or not any(v is not None for v in grid):
            return "final", {"id": sid, "error": NO_PREDICTIONS, "final": True}
        ex = ex or []                                          # a harmonic curve without NOAA's list of extremes
        return "ok", {
            "id": sid, "name": st.get("name"), "lat": st.get("lat"), "lon": st.get("lon"), "tz": st.get("tz"),
            "type": st.get("type"), "obs": bool(st.get("obs")), "datum": "MLLW", "units": "m",
            "begin": begin - LEAD_S, "window": [begin, begin + SPAN_S], "step": STEP_S, "v": grid,
            "hilo": [[t, round(h, 3), k] for t, h, k in ex],
            "night": night_bands(st["lat"], st["lon"], begin - LEAD_S, begin + SPAN_S + LEAD_S)
            if st.get("lat") is not None else None,
            "method": method, "ref": ref_used, "source": ATTRIBUTION,
        }

    # -- observations
    def observed(self, sid):
        st = self.stations.get(sid) if valid_id(sid) else None
        if st is None:
            return "final", {"id": sid, "error": "Unknown tide station"}
        if not st.get("obs"):
            return "final", {"id": sid, "t": [], "v": [], "note": "This station does not report its water level"}
        return self._answer(("o", sid), lambda: self._build_observed(sid), self.obs_ttl)

    def _build_observed(self, sid):
        doc = self._json(url(sid, product="water_level", range=48))
        err = noaa_error(doc)
        pts = [] if err else parse_water_level(doc)
        out = {"id": sid, "datum": "MLLW", "units": "m", "t": [t for t, _ in pts], "v": [round(v, 3) for _, v in pts]}
        if err:
            out["note"] = "No recent water level from NOAA"
        return "ok", out


def _after_fork():
    for svc in list(_SERVICES):
        svc._reset_locks()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)
