"""Multi-source live-buoy providers for the worldwide "Live buoys" map layer.

Each provider yields a lean STATION LIST (namespaced ids + lat/lon + capabilities +
attribution) for map markers, and a per-station LATEST observation (bulk wave params).
Heavier per-buoy detail (24h summary, directional spectra) is added per source where
supported. The NDBC provider lives in app.py (it wraps the existing NDBC parsing); the
ERDDAP-based providers here are self-contained (no app.py import -> no circular import).

Conventions (normalize everything here so the render path stays simple):
  - heights in METERS, periods in SECONDS, directions in DEGREES, timestamps ISO-8601 UTC ("...Z").
  - wave direction is the "coming FROM" convention (matches NDBC).
Resilience: every network call has a timeout; a failed station-list fetch keeps the last good
list (retried after retry_after_sec), and nothing raises out of list_stations()/latest().
Freshness: lists are refreshed stale-while-revalidate on the module's refresh runner (see
BuoyProvider.list_stations_versioned / ThreadRunner); only a provider with no list yet makes a
caller wait.
"""
import bisect
import csv
import io
import json
import logging
import math
import os
import re
import socket
import struct
import time
import threading
import weakref
from datetime import datetime, timedelta, timezone

import requests

_log = logging.getLogger(__name__)


_POS3 = struct.Struct("<qdd")   # (epoch, lat, lon): CMEMS position history, 24 bytes


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(min(1.0, math.sqrt(a)))


# Static capability template; providers override the True/False per their data.
def _caps(bulk=False, recent_history=False, directional=False, spectra=False, partitions=False):
    return {"bulk": bulk, "recent_history": recent_history, "directional": directional,
            "spectra": spectra, "partitions": partitions}


class BuoyProvider:
    """Base class. Subclasses set the metadata + capabilities and implement the fetches."""
    source = "?"
    source_name = ""
    source_url = ""
    license_label = ""
    attribution_text = ""
    stale_after_sec = 12 * 3600          # obs older than this -> is_stale
    capabilities = _caps()
    list_ttl_sec = 3600                  # station-list cache TTL
    timeout = 30
    # A refresh that FAILS (network/HTTP/parse error) keeps the last good list instead of
    # publishing an empty one for a whole TTL: the layer used to lose an entire agency's
    # markers for up to list_ttl_sec after one upstream hiccup. The failed provider is
    # retried after retry_after_sec; a good list is kept through failures for at most
    # keep_on_failure_sec after it was fetched (then the old fail-soft [] applies again).
    retry_after_sec = 300
    keep_on_failure_sec = 6 * 3600
    # Stale-while-revalidate: a good list is REFRESHED IN THE BACKGROUND once this fraction of
    # its TTL has passed, while callers keep getting the list in hand at once. Only a provider
    # with no list at all makes a caller wait for a fetch. (The hard expiry at the full TTL
    # still counts as "due", so a list is never older than a TTL plus one refresh.)
    refresh_at = 0.9
    # Keys a provider's fetched rows may carry beyond the station's identity, copied into the lean entry
    # as they are (the wind providers' readings, plan section 39). The live-buoy providers set none.
    extra_keys = ()

    def __init__(self, http=None):
        self.http = http or requests.Session()
        _PROVIDERS.add(self)                 # so a forked child can reset every provider's locks
        self._lock = threading.Lock()
        self._list_cache = None
        self._list_ts = 0.0
        # Monotonic publish counter: every (re)fetch -- successful, empty, or failed -- bumps it,
        # so a snapshot's (list, version) pair identifies exactly which publish it came from.
        self._list_version = 0
        self._list_ok_ts = 0.0           # when the current list was last fetched SUCCESSFULLY
        self._pub_ts = 0.0               # when the list in hand was published (its age for /healthz)
        self._due_ts = 0.0               # when the next refresh is due (refresh_at / retry rule)
        # Singleflight for the refresh itself: concurrent callers that find the list expired
        # wait here and then return the ONE freshly published list instead of each fetching.
        self._refresh_lock = threading.Lock()
        self._refresh_pending = False    # a background refresh job is queued or running
        self._refreshing = False         # a fetch is in flight right now (any path)
        self._fetch_started_ts = 0.0     # when the fetch in flight started
        self._last_attempt_ts = 0.0      # diagnostics for /healthz
        self._last_duration_s = None
        self._last_error = None          # None after a good refresh; the failure text otherwise

    # --- interface ---
    def _fetch_stations(self):
        """Return raw [{local_id, name, lat, lon}] (no namespacing). Override."""
        raise NotImplementedError

    def latest(self, local_id):
        """Return latest bulk obs (SI) {time_utc, hs_m, tp_s, dir_deg} or None. Override."""
        raise NotImplementedError

    # --- shared ---
    def list_stations(self):
        """Cached, fail-soft lean station list with namespaced ids + capabilities + attribution."""
        return self.list_stations_versioned()[0]

    def _reset_after_fork(self):
        """In a forked child the parent's refresh threads do not exist: any lock they held stays
        held and their in-flight / queued flags never clear. Fresh locks and flags; the lists
        themselves (plain data) are kept."""
        self._lock = threading.Lock()
        self._refresh_lock = threading.Lock()
        self._refresh_pending = False
        self._refreshing = False

    def snapshot(self):
        """(list, version, published_ts) of the list in hand, read together under the lock, or
        None when nothing has been published yet. Never fetches, never waits, no TTL check."""
        with self._lock:
            if self._list_cache is None:
                return None
            return self._list_cache, self._list_version, self._list_ts

    def _snapshot_if_fresh(self):
        """(list, version, published_ts) under the lock, or None when expired/never fetched."""
        with self._lock:
            if self._list_cache is not None and (time.time() - self._list_ts) < self.list_ttl_sec:
                return self._list_cache, self._list_version, self._list_ts
        return None

    def refresh_due(self, now=None):
        """True when a refresh should run: no list yet, refresh_at of the TTL passed since the
        last publish, the retry moment after a failure arrived, or the full TTL passed (the
        hard expiry; tests force it by setting _list_ts = 0)."""
        now = time.time() if now is None else now
        with self._lock:
            if self._list_cache is None:
                return True
            return now >= self._due_ts or (now - self._list_ts) >= self.list_ttl_sec

    def list_stations_versioned(self):
        """(list, version, published_ts) read together under one lock, so a caller can key
        derived work on the exact publish it saw. Stale-while-revalidate: the list in hand is
        returned at once; when a refresh is due it is scheduled on the module's refresh runner
        (background threads in production; inline in the tests, which keeps the fetch schedule
        of the golden replay identical). Only a provider with NO list yet fetches inline:
        concurrent cold callers wait on the one fetch (singleflight)."""
        if self.snapshot() is None:
            if _NONBLOCKING:                        # a background scheduler is running: queue it,
                self.schedule_refresh()             # never hold a request thread for a feed (G25 A-1)
                return [], 0, 0.0
            return self.refresh()                   # cold: wait for the one fetch
        if self.refresh_due():
            self.schedule_refresh()                 # inline runner: refreshed before the read below
        return self.snapshot()

    def refresh_runner(self):
        """The runner this provider's background refreshes go to (the live buoys' shared one; the wind feeds keep
        their own, so a slow wind agency never holds a live-buoy worker: G28 A-5)."""
        return get_refresh_runner()

    def schedule_refresh(self):
        """Queue one background refresh (no-op while one is queued or in flight)."""
        with self._lock:
            if self._refresh_pending or self._refreshing:
                return False
            self._refresh_pending = True
        try:
            self.refresh_runner().submit(self._scheduled_refresh, "buoy-refresh-%s" % self.source)
        except Exception as e:
            with self._lock:
                self._refresh_pending = False
            _log.warning("buoy provider %s: could not schedule a refresh (%s)", self.source, e)
            return False
        return True

    def _scheduled_refresh(self):
        changed = False
        try:
            with self._refresh_lock:
                # published or refreshed by someone else while this job waited? (a cold caller
                # on the inline path, or a forced refresh)
                if self.refresh_due():
                    changed = self._refresh_locked()[1]
        finally:
            with self._lock:
                self._refresh_pending = False
        if changed:
            _notify_publish(self)                   # outside every lock

    def refresh(self):
        """Fetch and publish unless a fresh list is already in hand (singleflight through
        _refresh_lock; a caller that waited for another refresh gets that refresh's publish;
        within the retry window after a failure the kept list counts as fresh). Returns the
        snapshot; publish listeners are told outside every lock."""
        with self._refresh_lock:
            snap = self._snapshot_if_fresh()        # published while we waited?
            if snap is not None:
                return snap
            snap, changed = self._refresh_locked()
        if changed:
            _notify_publish(self)
        return snap

    def _refresh_locked(self):
        """The fetch + publish, under _refresh_lock -> (snapshot, changed). The CALLER tells the
        publish listeners after releasing _refresh_lock when `changed` (a new version)."""
        with self._lock:
            before = self._list_version
        result = self._fetch_and_publish()
        return result, result[1] != before

    def _fetch_and_publish(self):
        """Keeps the last good list through a failure (same version -> identical bytes
        downstream) and comes back after retry_after_sec."""
        with self._lock:
            self._refreshing = True
            self._fetch_started_ts = time.time()
        t0 = time.monotonic()
        try:
            out, ok, err = self._build_station_list()
        finally:
            dur = time.monotonic() - t0
        with self._lock:
            now = time.time()
            self._refreshing = False
            self._last_attempt_ts = now
            self._last_duration_s = dur
            if ok and not out and self._list_cache:
                # An agency that answered with NOTHING after a good list is as good as down
                # (an empty catalogue page, a truncated feed): keep the markers, retry.
                ok, err = False, "empty list after %d stations" % len(self._list_cache)
            retry = min(self.retry_after_sec, self.list_ttl_sec)
            if not ok:
                self._last_error = err or "refresh failed"
                if self._list_cache and (now - self._list_ok_ts) < self.keep_on_failure_sec:
                    # Keep the last good list; come back sooner than a full TTL.
                    self._list_ts = now - self.list_ttl_sec + retry
                    self._due_ts = now + retry
                    _log.warning("buoy provider %s: refresh failed after %.1fs (%s), keeping the "
                                 "previous list (%d stations, fetched %ds ago); retry in %ds",
                                 self.source, dur, self._last_error, len(self._list_cache),
                                 int(now - self._list_ok_ts), retry)
                    return self._list_cache, self._list_version, self._list_ts
                if self._list_cache != out:       # [] : no markers rather than a broken map
                    self._list_cache = out
                    self._list_version += 1       # (a feed that keeps failing republishes nothing new)
                    self._pub_ts = now
                self._list_ts = now
                self._due_ts = now + retry
                _log.warning("buoy provider %s: refresh failed after %.1fs (%s), no list kept; "
                             "retry in %ds", self.source, dur, self._last_error, retry)
                return out, self._list_version, self._list_ts
            self._last_error = None
            self._list_cache = out
            self._list_ts = now
            self._list_version += 1
            self._list_ok_ts = now
            self._pub_ts = now
            self._due_ts = now + self.list_ttl_sec * self.refresh_at
            _log.info("buoy provider %s: list refreshed in %.1fs (%d stations, version %d)",
                      self.source, dur, len(out), self._list_version)
            return out, self._list_version, self._list_ts

    def status(self, now=None):
        """Diagnostics for /healthz (no work, no waiting)."""
        now = time.time() if now is None else now
        with self._lock:
            has = self._list_cache is not None
            return {
                "source": self.source,
                "version": self._list_version if has else None,
                "stations": len(self._list_cache) if has else None,
                "age_s": round(now - self._pub_ts, 1) if has else None,     # the list in hand's age
                "good_age_s": round(now - self._list_ok_ts, 1) if self._list_ok_ts else None,
                "due_in_s": (round(min(self._due_ts, self._list_ts + self.list_ttl_sec) - now, 1)
                             if has else 0.0),
                "in_flight": self._refreshing,
                "in_flight_s": round(now - self._fetch_started_ts, 1) if self._refreshing else None,
                "pending": self._refresh_pending,
                "last_error": self._last_error,
                "last_duration_s": (round(self._last_duration_s, 2)
                                    if self._last_duration_s is not None else None),
                "last_attempt_ts": self._last_attempt_ts or None,
            }

    def _build_station_list(self):
        """One upstream fetch -> (lean entries, ok, error). ok=False means the fetch FAILED (as
        opposed to a legitimately empty list); the caller decides whether to keep the last
        good list. Fail-soft: a failure yields [] (no markers rather than a broken map)."""
        out = []
        ok = True
        err = None
        now = time.time()
        try:
            for s in self._fetch_stations():
                lat, lon = s.get("lat"), s.get("lon")
                if lat is None or lon is None:
                    continue
                lat, lon = float(lat), float(lon)
                if not (math.isfinite(lat) and math.isfinite(lon)):
                    continue                      # NaN / inf would make the merged list invalid JSON for everyone
                entry = {
                    "id": "%s:%s" % (self.source.lower(), s["local_id"]),
                    "source": self.source,
                    "source_name": self.source_name,
                    "source_url": self.source_url,
                    "license_label": self.license_label,
                    "attribution_text": self.attribution_text,
                    "name": str(s.get("name") or s["local_id"]),
                    "lat": float(lat),
                    "lon": float(lon),
                    # a provider may set per-station capabilities (e.g. only some AODN
                    # buoys publish spectra); otherwise use the provider default.
                    "capabilities": dict(s.get("capabilities") or self.capabilities),
                    "is_stale": False,
                    "dup_of": None,
                }
                # Providers that fetch the latest obs at list-build time pass 'latest_time';
                # mark the marker stale if that obs is older than stale_after_sec.
                lt = s.get("latest_time")
                if lt:
                    entry["latest_time"] = lt
                    ep = _z_epoch({"time_utc": lt})
                    if ep is not None and (now - ep) > self.stale_after_sec:
                        entry["is_stale"] = True
                for k in self.extra_keys:
                    if k in s:
                        entry[k] = s[k]
                out.append(entry)
        except Exception as e:
            _log.warning("buoy provider %s: station-list fetch failed (%s)", self.source, e)
            out = []                          # fail soft: no markers rather than a broken map
            ok = False
            err = "%s: %s" % (type(e).__name__, e)
        return out, ok, err

    def detail(self, local_id):
        """Return {latest, recent}. Base: latest obs only (no history). Sources with
        recent_history override this to also return a recent obs list (newest-first)."""
        return {"latest": self.latest(local_id), "recent": []}


class ThreadRunner:
    """Runs refresh jobs on `workers` daemon threads fed from a FIFO queue, so a boot-time
    warm-up of ten agencies downloads at most `workers` feeds at once (memory), in the order
    they were submitted (cheap feeds first), and never on a request thread. Daemon threads:
    a process exit never waits for a feed."""

    def __init__(self, workers=3):
        import queue
        self.workers = max(1, int(workers))
        self._q = queue.Queue()
        self._lock = threading.Lock()
        self._threads = []
        self.queued = 0                  # submitted, not finished (waiting or running)
        self.running = 0

    @property
    def waiting(self):
        """Jobs submitted and not started yet."""
        with self._lock:
            return max(0, self.queued - self.running)

    def _worker(self):
        while True:
            name, fn = self._q.get()
            if fn is None:                       # shutdown()
                self._q.task_done()
                return
            with self._lock:
                self.running += 1
            try:
                fn()
            except Exception:
                _log.exception("buoy refresh job %s failed", name)
            finally:
                with self._lock:
                    self.running -= 1
                    self.queued -= 1
                self._q.task_done()

    def shutdown(self, timeout=5.0):
        """Stop the worker threads once the queued jobs have run (tests: no idle thread outlives them)."""
        with self._lock:
            threads = list(self._threads)
            self._threads = []
        for _ in threads:
            self._q.put(("shutdown", None))
        for t in threads:
            t.join(timeout)

    def submit(self, fn, name="buoy-refresh"):
        with self._lock:
            self.queued += 1
            if len(self._threads) < self.workers:          # started lazily, never joined
                t = threading.Thread(target=self._worker, name="buoy-refresh-%d" % len(self._threads),
                                     daemon=True)
                self._threads.append(t)
                t.start()
        self._q.put((name, fn))


class InlineRunner:
    """Runs each job in the calling thread, at once (tests: the fetch schedule of the golden
    replay stays identical to the inline-refresh implementation it was captured on)."""
    queued = 0
    running = 0
    waiting = 0

    def submit(self, fn, name="buoy-refresh"):
        fn()


_REFRESH_RUNNER = None
_RUNNER_LOCK = threading.Lock()
_PUBLISH_LISTENERS = []
_PROVIDERS = weakref.WeakSet()
_CAPS = weakref.WeakSet()


class SharedCap:
    """A cap on concurrent builds shared by several request-path services (forecast points, tide windows, wind
    histories: G28 A-6): acquire(blocking=True, timeout=-1) / release() like a BoundedSemaphore, called from the same
    thread. It keeps when each slot was taken: stuck(wait_s) is True while every slot has been held longer than wait_s,
    so a caller answers "busy" at once instead of spending its wait on a request thread (a slow agency must not tie up
    the threads /healthz needs: G28 re-check R1-N4; the forecast points' own rule, G22 A-6, for all three services).
    A forked child gets a fresh one (a parent's thread may have held a slot)."""

    def __init__(self, n=2):
        self.n = int(n)
        self._sem = threading.BoundedSemaphore(self.n)
        self._lock = threading.Lock()
        self._since = {}                     # thread ident -> the times it took its slots
        _CAPS.add(self)

    def stuck(self, wait_s, now=None):
        now = time.monotonic() if now is None else now
        with self._lock:
            marks = [t for ts in self._since.values() for t in ts]
        return len(marks) >= self.n and all(now - t > wait_s for t in marks)

    def held(self):
        with self._lock:
            return sum(len(ts) for ts in self._since.values())

    def acquire(self, blocking=True, timeout=-1):
        if not blocking:
            ok = self._sem.acquire(False)
        else:
            ok = self._sem.acquire(True, None if timeout is None or timeout < 0 else timeout)
        if ok:
            with self._lock:
                self._since.setdefault(threading.get_ident(), []).append(time.monotonic())
        return ok

    def release(self):
        with self._lock:
            me = threading.get_ident()
            ts = self._since.get(me)
            if not ts and self._since:           # released by another thread: forget the oldest mark
                me = min(self._since, key=lambda k: min(self._since[k]))
                ts = self._since[me]
                ts.remove(min(ts))
            elif ts:
                ts.pop()
            if me in self._since and not self._since[me]:
                del self._since[me]
        self._sem.release()

    def _reset(self):
        self._sem = threading.BoundedSemaphore(self.n)
        self._lock = threading.Lock()
        self._since = {}
_NONBLOCKING = False             # set while a background scheduler refreshes the lists (app.py)


def set_nonblocking(on):
    """While a background scheduler keeps the lists fresh, a caller asking a provider that has no
    list yet gets [] at once (and a refresh is queued) instead of waiting for the feed."""
    global _NONBLOCKING
    _NONBLOCKING = bool(on)


def _reset_after_fork():
    """A forked child (e.g. a gunicorn worker forked by a master that had already refreshed lists)
    gets the parent's memory but none of its threads: a new runner (its worker threads are gone),
    a new runner lock, and every provider's locks and flags reset. Registered with
    os.register_at_fork where the platform has it."""
    global _REFRESH_RUNNER, _RUNNER_LOCK, _NONBLOCKING
    _RUNNER_LOCK = threading.Lock()
    _REFRESH_RUNNER = None
    _NONBLOCKING = False                 # no scheduler runs in the child until it starts its own
    for p in list(_PROVIDERS):
        p._reset_after_fork()
    for c in list(_CAPS):
        c._reset()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


def add_publish_listener(fn):
    """fn(provider) is called (outside every provider lock, in the refreshing thread) whenever a
    provider publishes a new list version: the app's scheduler wakes up and rebuilds the merged
    list at once instead of at its next pass. Idempotent."""
    with _RUNNER_LOCK:
        if fn not in _PUBLISH_LISTENERS:
            _PUBLISH_LISTENERS.append(fn)


def remove_publish_listener(fn):
    with _RUNNER_LOCK:
        if fn in _PUBLISH_LISTENERS:
            _PUBLISH_LISTENERS.remove(fn)


def _notify_publish(provider):
    with _RUNNER_LOCK:
        listeners = list(_PUBLISH_LISTENERS)
    for fn in listeners:
        try:
            fn(provider)
        except Exception:
            _log.exception("buoy publish listener failed")


def get_refresh_runner():
    """The module's refresh runner: a ThreadRunner with LIVE_REFRESH_WORKERS threads (default 3)
    unless set_refresh_runner() installed another."""
    global _REFRESH_RUNNER
    with _RUNNER_LOCK:
        if _REFRESH_RUNNER is None:
            try:
                workers = int(float(os.environ.get("LIVE_REFRESH_WORKERS", "3")))
            except (ValueError, OverflowError):
                workers = 3
            _REFRESH_RUNNER = ThreadRunner(max(1, min(10, workers)))
        return _REFRESH_RUNNER


def set_refresh_runner(runner):
    """Install a runner (None restores the default on the next use). Returns the previous one."""
    global _REFRESH_RUNNER
    with _RUNNER_LOCK:
        prev = _REFRESH_RUNNER
        _REFRESH_RUNNER = runner
    return prev


def _one_shot_get(http, url, timeout):
    """One GET with no automatic retries: the shared app session retries twice, which turns a 40 s
    read timeout into ~2 minutes. A real requests.Session goes through a plain requests.get (its
    default adapter does not retry); a test's fake http is called as is."""
    if isinstance(http, requests.Session):
        return requests.get(url, timeout=timeout)
    return http.get(url, timeout=timeout)


def _erddap_rows(http, url, timeout, get=None):
    """GET an ERDDAP .csv and return (header, [data rows]) using a real CSV parser
    (ERDDAP quotes commas inside fields, e.g. station names). `get(http, url, timeout)` replaces
    the session's own GET (the one-shot variant for a request thread)."""
    r = get(http, url, timeout) if get else http.get(url, timeout=timeout)
    if r.status_code != 200:
        return [], []
    rows = list(csv.reader(io.StringIO(r.text)))
    if len(rows) < 3:
        return (rows[0] if rows else []), []
    return rows[0], rows[2:]                   # rows[1] is the units row


class CDIPProvider(BuoyProvider):
    """CDIP / Scripps -- open ERDDAP wave_agg. Bulk Hs/Tp/Dp here (Phase 1a);
    directional spectra are a later phase. waveDp is the peak direction (deg, coming-from)."""
    source = "CDIP"
    source_name = "CDIP / Scripps"
    source_url = "https://cdip.ucsd.edu"
    license_label = "Public domain (USACE)"
    attribution_text = "Source: CDIP, Scripps Institution of Oceanography, UC San Diego"
    stale_after_sec = 6 * 3600
    capabilities = _caps(bulk=True, directional=True)   # spectra/partitions wired in Phase 1b
    BASE = "https://erddap.cdip.ucsd.edu/erddap/tabledap/wave_agg"

    def _fetch_stations(self):
        # ACTIVE subset only: stations with data in the last 2 days (the live buoys). The
        # full wave_agg also contains many decommissioned stations; the time constraint
        # filters them out and is fast (~0.7s). csv-parsed so quoted names are safe.
        hdr, rows = _erddap_rows(
            self.http,
            self.BASE + ".csv?station_id,metaStationName,latitude,longitude"
                        "&time%3E=now-2days&distinct()",
            self.timeout)
        if not hdr:
            return []
        ix = {c: i for i, c in enumerate(hdr)}
        out = []
        for row in rows:
            try:
                out.append({
                    "local_id": row[ix["station_id"]],
                    "name": "CDIP %s%s" % (
                        row[ix["station_id"]],
                        (" - " + row[ix["metaStationName"]]) if row[ix["metaStationName"]] else ""),
                    "lat": float(row[ix["latitude"]]),
                    "lon": float(row[ix["longitude"]]),
                })
            except (KeyError, ValueError, IndexError):
                continue
        return out

    def latest(self, local_id):
        # Time-constrained single-station orderByMax -> light + reliable (404 if no such station).
        url = (self.BASE + ".csv?station_id,time,waveHs,waveTp,waveDp"
               "&station_id=%22" + str(local_id) + "%22&time%3E=now-3days&orderByMax(%22time%22)")
        hdr, rows = _erddap_rows(self.http, url, self.timeout)
        if not hdr or not rows:
            return None
        ix = {c: i for i, c in enumerate(hdr)}
        try:
            r0 = rows[0]
            def num(k):
                v = r0[ix[k]]
                return float(v) if v not in ("", "NaN") else None
            return {
                "time_utc": r0[ix["time"]],
                "hs_m": num("waveHs"),
                "tp_s": num("waveTp"),
                "dir_deg": num("waveDp"),
                "dir_kind": "from",
            }
        except (KeyError, ValueError, IndexError):
            return None


def _wfs_row_stream(http, url, timeout, headers=None):
    """GET a GeoServer WFS .csv and yield its rows straight off the socket (header first).
    Unlike ERDDAP there is NO units row. An XML ServiceException body (first non-blank
    character '<') or a non-200 yields nothing -- the same "treat as empty" as before, but
    without ever holding the whole body (the AODN map layer is ~4 MB) as one string."""
    r = http.get(url, timeout=timeout, headers=headers, stream=True)
    try:
        if r.status_code != 200:
            return
        lines = r.iter_lines(decode_unicode=True)

        def _text(line):
            return line.decode("utf-8", "replace") if isinstance(line, bytes) else line

        def _all():
            first = None
            for raw in lines:                    # find the first non-blank line = what
                line = _text(raw)                #   .lstrip().startswith("<") looked at
                if line.strip():
                    first = line
                    break
                yield line
            if first is None:
                return
            if first.lstrip().startswith("<"):
                raise _WfsXmlError()
            yield first
            for raw in lines:
                yield _text(raw)
        try:
            yield from csv.reader(_all())
        except _WfsXmlError:
            return
    finally:
        r.close()


class _WfsXmlError(Exception):
    pass


def _wfs_rows(http, url, timeout, headers=None):
    """(header, data rows) of a GeoServer WFS .csv; kept for callers that need the whole thing."""
    rows = list(_wfs_row_stream(http, url, timeout, headers))
    if len(rows) < 2:
        return (rows[0] if rows else []), []
    return rows[0], rows[1:]


def _z(t):
    """Normalize a naive ISO timestamp (already UTC) to a trailing-Z form."""
    if not t:
        return None
    return t if t.endswith("Z") else t + "Z"


def _iso_to_z(dt):
    """Convert an offset-aware ISO string (e.g. '...+10:00') to UTC '...Z'."""
    if not dt:
        return None
    try:
        d = datetime.fromisoformat(dt)
        if d.tzinfo is not None:
            d = d.astimezone(timezone.utc)
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return dt


def _unix_to_z(ts):
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError, OSError):
        return None


def _parse_naive(t):
    try:
        return datetime.strptime(t, "%Y-%m-%dT%H:%M:%S")
    except (TypeError, ValueError):
        return None


def _opendap_array(http, base_url, projection, timeout, n=None):
    """GET one OPeNDAP .ascii projection (e.g. 'ENERGY.ENERGY[669:1:669][0:1:38]') and
    return its trailing numeric values. Grid members are addressed as VAR.VAR to get the
    bare data array (no coordinate maps). Returns the last n values when n is given."""
    r = http.get(base_url + ".ascii?" + projection, timeout=timeout)
    if r.status_code != 200:
        return []
    body = r.text.split("\n", 1)[1] if "\n" in r.text else r.text
    body = re.sub(r"\[\d+\]", "", body)              # strip [index] markers
    vals = []
    for tok in re.findall(r"-?\d+\.?\d*(?:[eE][-+]?\d+)?", body):
        try:
            vals.append(float(tok))
        except ValueError:
            pass
    return vals[-n:] if (n and len(vals) >= n) else vals


def _ndbc_alpha1(a1, b1):
    """Mean direction FROM, degrees clockwise from true north, of the first Fourier moments: NDBC's
    ALPHA1 = 270 - ARCTAN(b1, a1) (https://www.ndbc.noaa.gov/faq/measdes.shtml). The plain atan2 angle is a
    mathematical angle, not a compass direction: AODN's breakdown showed 270 - the true direction until
    2026-10-03 (Wilsons Prom swell 23 deg where the buoy reported 248). With this formula the spectra's peak
    direction equals the buoy's own reported peak direction in 424 of 424 readings at all 17 AODN sites."""
    return (270.0 - math.degrees(math.atan2(b1, a1))) % 360.0


def _ndbc_alpha2(a2, b2, alpha1):
    """Principal direction FROM of the second moments: NDBC's ALPHA2 = 270 - (0.5 * ARCTAN(b2, a2) + {0 or 180}),
    the 180 added when that brings ALPHA2 closer to ALPHA1 (the second moments fix an axis, not a direction)."""
    a = (270.0 - 0.5 * math.degrees(math.atan2(b2, a2))) % 360.0
    b = (a + 180.0) % 360.0

    def off(x):
        return abs((x - alpha1 + 180.0) % 360.0 - 180.0)
    return b if off(b) < off(a) else a


def _z_epoch(o):
    try:
        return datetime.strptime(o["time_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc).timestamp()
    except (KeyError, TypeError, ValueError):
        return None


def _recent_window(obs, hours=24, max_rows=48):
    """From a chronological (oldest->newest) obs list, keep the last `hours` (relative
    to the newest obs, so a clock skew can't empty it), downsample to <= max_rows while
    always keeping the newest, and return NEWEST-FIRST for a readable table."""
    rows = [o for o in obs if o and _z_epoch(o) is not None]
    if not rows:
        return []
    newest = _z_epoch(rows[-1])
    cutoff = newest - hours * 3600
    win = [o for o in rows if _z_epoch(o) >= cutoff] or rows[-max_rows:]
    if len(win) > max_rows:
        step = (len(win) + max_rows - 1) // max_rows
        win = [o for i, o in enumerate(win) if (len(win) - 1 - i) % step == 0]
    return list(reversed(win))


class AODNProvider(BuoyProvider):
    """AODN / IMOS national near-real-time wave buoys (Australia). The open WFS 'map'
    layer carries every site's full recent timeseries; we fetch it once (cached) and
    group to the latest row per site -> station list AND latest obs in one request.
    Bulk parameters only (Hs / peak period / mean period / peak direction); the NRT
    product has no spectra. Some buoys are non-directional (blank direction)."""
    source = "AODN"
    source_name = "AODN / IMOS (Australia)"
    source_url = "https://portal.aodn.org.au"
    license_label = "CC BY 4.0"
    attribution_text = ("Source: Australia's Integrated Marine Observing System (IMOS), "
                        "a NCRIS facility, via AODN")
    stale_after_sec = 15 * 3600          # NRT aggregator: dispatch normally lags several hours
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 1800
    timeout = 90
    BASE = "https://geoserver-123.aodn.org.au/geoserver/ows"
    LAYER = "aodn:aodn_wave_nrt_v2_timeseries_map"
    PROPS = ("site_name,institution,wave_buoy_type,water_depth,time_end,TIME,"
             "significant_wave_height,wave_mean_period,peak_wave_direction,peak_wave_period,geom")
    _POINT = re.compile(r"POINT\s*\(\s*([-0-9.]+)\s+([-0-9.]+)")
    # IMOS/UWA sites that publish full directional spectra (WAVE-SPECTRA) -> can drive
    # NDBC-style partitions + a spectral-density chart. The rest are bulk-only.
    THREDDS = ("https://thredds.aodn.org.au/thredds/dodsC/IMOS/COASTAL-WAVE-BUOYS/"
               "WAVE-BUOYS/REALTIME/WAVE-SPECTRA/")
    SPECTRA_SITES = frozenset({
        "APOLLO-BAY", "BENGELLO", "BOB", "BRIGHTON", "CAPE-BRIDGEWATER", "CEDUNA",
        "CENTRAL", "COCKBURN-SOUND", "COLLAROY-NARRABEEN", "DONGARA-OFFSHORE",
        "FENTON-PATCHES", "HILLARYS", "MANINGRIDA", "MISSION-BEACH",
        "NORTH-KANGAROO-ISLAND", "OCEAN-BEACH", "SHARK-BAY-0-2", "STORM-BAY",
        "TANTABIDDI", "TATHRA", "TORBAY-WEST", "WILSONS-PROM", "WOOLI",
    })
    spectra_ttl_sec = 1200
    PRUNE_KEEP = timedelta(hours=25)     # > the 24 h _recent_window, so nothing in-window goes
    PRUNE_EVERY = 64                     # rows per site between prune passes
    # The map layer's per-site geom is NOT where the buoy is: it is one summary point over the
    # site's whole history and deployments (2026-10-03: "Crowdy" drawn in central Australia,
    # Coral Bay 164 km off, 48 of 60 sites more than 200 m off). The data layer carries the
    # buoy's own GPS position with every observation; a site takes its newest one from the last
    # POS_DAYS, and a site with none is left off the map (no reliable position).
    POS_LAYER = "aodn:aodn_wave_nrt_v2_timeseries_data"
    POS_DAYS = 7                         # = the map layer's window (same rows, same sites)
    POS_MAX_ROWS = 200_000               # 7 days ~ 17k rows; reaching the cap fails the refresh
    POS_MIN_COVER = 0.9                  # positions must cover 90 % of the map layer's sites
    SPEC_PROBE_TTL = 6 * 3600

    def __init__(self, http=None):
        super().__init__(http)
        self._latest_by_id = {}
        self._recent_by_id = {}
        self._spec_cache = {}                 # local_id -> (ts, spectrum dict)
        self._spec_asked = {}                 # the same keys -> when a refresh last asked again (plan section 56)

    @classmethod
    def _spectra_code(cls, local_id):
        code = (local_id or "").upper().replace(" ", "-")
        return code if code in cls.SPECTRA_SITES else None

    @staticmethod
    def _num(row, ix, col):
        if col not in ix or len(row) <= ix[col]:
            return None
        v = row[ix[col]]
        try:
            return float(v) if v not in ("", "NaN") else None
        except ValueError:
            return None

    def _obs(self, row, ix, time_utc):
        return {
            "time_utc": time_utc,
            "hs_m": self._num(row, ix, "significant_wave_height"),
            "tp_s": self._num(row, ix, "peak_wave_period"),
            "mean_period_s": self._num(row, ix, "wave_mean_period"),
            "dir_deg": self._num(row, ix, "peak_wave_direction"),
            "dir_kind": "from",
        }

    def _observed_positions(self):
        """site_name -> (lat, lon) of its newest observation in the data layer over POS_DAYS.
        Positions only, streamed (newest kept per site). No rows at all raises: the caller then
        keeps its last good list instead of publishing sites without positions."""
        since = (datetime.now(timezone.utc) - timedelta(days=self.POS_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
        url = (self.BASE + "?service=WFS&version=1.0.0&request=GetFeature&typeName=" + self.POS_LAYER
               + "&outputFormat=csv&propertyName=site_name,TIME,LATITUDE,LONGITUDE"
               + "&CQL_FILTER=" + requests.utils.quote("TIME >= '%s'" % since, safe="")
               + "&maxFeatures=%d" % self.POS_MAX_ROWS)
        stream = _wfs_row_stream(self.http, url, self.timeout)
        hdr = next(stream, None)
        if not hdr or any(c not in hdr for c in ("site_name", "TIME", "LATITUDE", "LONGITUDE")):
            raise RuntimeError("AODN observed positions: no header")
        ix = {c: i for i, c in enumerate(hdr)}
        need = max(ix["site_name"], ix["TIME"], ix["LATITUDE"], ix["LONGITUDE"])
        best = {}
        rows = 0
        for row in stream:
            rows += 1
            if len(row) <= need or not row[ix["site_name"]]:
                continue
            try:
                lat, lon = float(row[ix["LATITUDE"]]), float(row[ix["LONGITUDE"]])
            except ValueError:
                continue
            if not (math.isfinite(lat) and math.isfinite(lon) and -90 <= lat <= 90 and -180 <= lon <= 360):
                continue
            if lon > 180:
                lon -= 360
            s, t = row[ix["site_name"]], row[ix["TIME"]]
            if s not in best or t > best[s][0]:
                best[s] = (t, lat, lon)
        if not best:
            raise RuntimeError("AODN observed positions: no rows")
        if rows >= self.POS_MAX_ROWS:          # the cap was hit: the newest rows may be missing
            raise RuntimeError("AODN observed positions: %d rows, the query cap" % rows)
        return {s: (lat, lon) for s, (_t, lat, lon) in best.items()}

    def _spectra_available(self):
        """SPECTRA_SITES codes whose current monthly THREDDS spectra file exists (2026-10-03: 6 of the
        23 had none; ranked as spectra buoys they hid the richer AusWaves marker of the same buoy).
        Probed in parallel, cached SPEC_PROBE_TTL; a code that does not answer 200 counts as absent."""
        now = time.time()
        month = datetime.now(timezone.utc).strftime("%Y%m")
        hit = getattr(self, "_spec_probe", None)
        if hit and hit[0] == month and now - hit[1] < self.SPEC_PROBE_TTL:
            return hit[2]
        y, m = month[:4], month[4:]

        def probe(code):
            url = (self.THREDDS + code + "/%s/IMOS_COASTAL-WAVE-BUOYS_%s%s01_%s_RT_WAVE-SPECTRA_monthly.nc.dds"
                   % (y, y, m, code))
            try:
                r = self.http.get(url, timeout=20)
                return code if r.status_code == 200 else None
            except requests.RequestException:
                return None
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=8) as ex:
            ok = frozenset(c for c in ex.map(probe, sorted(self.SPECTRA_SITES)) if c)
        self._spec_probe = (month, now, ok)
        return ok

    def _map_rows(self):
        """(column index, site -> rows) from the map layer, streamed; (None, None) when the answer has no header or
        lacks a needed column."""
        url = (self.BASE + "?service=WFS&version=1.0.0&request=GetFeature&typeName="
               + self.LAYER + "&outputFormat=csv&propertyName=" + self.PROPS)
        stream = _wfs_row_stream(self.http, url, self.timeout)
        hdr = next(stream, None)
        if not hdr:
            return None, None
        ix = {c: i for i, c in enumerate(hdr)}
        if any(c not in ix for c in ("site_name", "time_end", "TIME", "geom")):
            return None, None
        bysite = {}                        # the map layer carries each site's full series
        # Only the newest row and the last 24 h of each site's series end up in the output
        # (_recent_window), yet the layer ships ~7 days per site. Rows are pruned WHILE
        # streaming to those inside PRUNE_KEEP of the site's newest TIME, which cannot change
        # the result: TIME strings in this feed share one fixed format, so string order (the
        # sort below) equals chronological order, the newest row is always kept, and pruned
        # rows lie outside the 24 h window. A site with ANY unparseable TIME is never pruned
        # (its fallback path can put every row inside the window).
        prunable = {}                      # site -> True while every TIME parsed
        maxtl = {}                         # site -> newest parsed TIME
        for row in stream:
            if len(row) <= ix["geom"]:
                continue
            s = row[ix["site_name"]]
            if not s:
                continue
            srows = bysite.setdefault(s, [])
            srows.append(row)
            tl = _parse_naive(row[ix["TIME"]])
            if tl is None:
                prunable[s] = False
            elif prunable.get(s, True):
                prunable[s] = True
                if s not in maxtl or tl > maxtl[s]:
                    maxtl[s] = tl
                if len(srows) >= self.PRUNE_EVERY:
                    cut = maxtl[s] - self.PRUNE_KEEP
                    bysite[s] = [r for r in srows if _parse_naive(r[ix["TIME"]]) >= cut]
        return ix, bysite

    def _fetch_stations(self):
        # Three independent requests: the observed positions and this month's spectra files run beside the map
        # layer's stream, so a refresh waits for the slowest, not their sum (one after another they doubled the
        # refresh, ~5 -> 8-13 s measured 2026-10-03, and the live-buoy list waits for it every 30 min). Nothing
        # is stored before all three answers are in: a failure of any of them leaves every cache as it was.
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=2) as ex:
            pos_f = ex.submit(self._observed_positions)
            spec_f = ex.submit(self._spectra_available)
            ix, bysite = self._map_rows()
            pos = pos_f.result()               # a positions failure raises here, before anything is stored
            spec_ok = spec_f.result()
        if ix is None:
            return []
        covered = sum(1 for s in bysite if s in pos)
        if len(bysite) - covered > max(1, (1 - self.POS_MIN_COVER) * len(bysite)):
            # the two layers cover the same window, so a short positions answer (a stream that
            # ended early) would silently drop sites: keep the last good list instead
            raise RuntimeError("AODN observed positions cover %d of %d sites" % (covered, len(bysite)))
        out, latest_by, recent_by = [], {}, {}
        for s, srows in bysite.items():
            srows.sort(key=lambda r: r[ix["TIME"]])       # TIME is the per-obs timestamp
            last = srows[-1]
            if s not in pos:                   # no observed position: not drawn (geom is unreliable)
                continue
            lat, lon = pos[s]
            # The per-row TIME is the observation time in UTC (2026-10-03: Wilsons Prom TIME
            # 19:50 = AusWaves' 19:50Z for the same reading, and = the THREDDS spectra file's
            # UTC). time_end is ~10 h behind it and is NOT used (the old code subtracted that
            # difference, showing every AODN time 10 h early). A row without a TIME is dropped.
            obs = []
            for r in srows:
                tl = _parse_naive(r[ix["TIME"]])
                if tl is None:
                    continue
                obs.append(self._obs(r, ix, tl.strftime("%Y-%m-%dT%H:%M:%SZ")))
            if not obs:
                continue
            latest_by[s] = obs[-1]
            recent_by[s] = _recent_window(obs)
            inst = last[ix["institution"]] if "institution" in ix else ""
            st = {
                "local_id": s,
                "name": ("%s - %s" % (s, inst)) if inst else s,
                "lat": lat, "lon": lon,
                "latest_time": obs[-1]["time_utc"],
            }
            code = self._spectra_code(s)
            if code and code in spec_ok:  # this buoy publishes full directional spectra (this month's file exists)
                st["capabilities"] = _caps(bulk=True, recent_history=True,
                                           directional=True, spectra=True, partitions=True)
            out.append(st)
        self._latest_by_id = latest_by
        self._recent_by_id = recent_by
        return out

    def detail(self, local_id):
        if local_id not in self._latest_by_id:
            self.list_stations()          # (re)build the map cache
        return {"latest": self._latest_by_id.get(local_id),
                "recent": self._recent_by_id.get(local_id, [])}

    def latest(self, local_id):
        return self.detail(local_id)["latest"]

    def spectrum(self, local_id, count=25, fresh_s=None):
        """Last `count` directional spectra (chronological) for a spectra-capable site,
        via THREDDS OPeNDAP. Returns {freqs:[39], steps:[{time_utc, energy[39],
        alpha1[39], alpha2[39], r1[39], r2[39]}]} or None. alpha1 = mean wave direction
        per bin in the NDBC 'from' convention (degrees clockwise from true north), converted from the files'
        Fourier moments with NDBC's formulas (_ndbc_alpha1/_ndbc_alpha2). One batched request per variable.
        fresh_s: a refresh from the live window (plan section 56): spectra fetched or asked for at least that long ago
        are fetched again; a refresh that fails gives None and the kept spectra stay for everyone else."""
        code = self._spectra_code(local_id)
        if not code:
            return None
        ckey = (local_id, count)
        with self._lock:
            hit = self._spec_cache.get(ckey)
            if hit and (time.time() - hit[0]) < self.spectra_ttl_sec:
                if fresh_s is None or time.time() - max(hit[0], self._spec_asked.get(ckey, 0.0)) < fresh_s:
                    return hit[1]
                self._spec_asked[ckey] = time.time()
        now = datetime.utcnow()
        url = (self.THREDDS + code + "/%04d/IMOS_COASTAL-WAVE-BUOYS_%04d%02d01_%s_"
               "RT_WAVE-SPECTRA_monthly.nc" % (now.year, now.year, now.month, code))
        try:
            dds = self.http.get(url + ".dds", timeout=self.timeout)
            if dds.status_code != 200:
                return None
            m = re.search(r"TIME\s*=\s*(\d+)", dds.text)
            if not m:
                return None
            n = int(m.group(1))
            i1 = n - 1
            i0 = max(0, n - count)
            k = i1 - i0 + 1
            sl = "[%d:1:%d]" % (i0, i1)
            freqs = _opendap_array(self.http, url, "FREQUENCY[0:1:38]", self.timeout, 39)
            energy = _opendap_array(self.http, url, "ENERGY.ENERGY%s[0:1:38]" % sl, self.timeout)
            a1 = _opendap_array(self.http, url, "A1.A1%s[0:1:38]" % sl, self.timeout)
            b1 = _opendap_array(self.http, url, "B1.B1%s[0:1:38]" % sl, self.timeout)
            a2 = _opendap_array(self.http, url, "A2.A2%s[0:1:38]" % sl, self.timeout)
            b2 = _opendap_array(self.http, url, "B2.B2%s[0:1:38]" % sl, self.timeout)
            tvals = _opendap_array(self.http, url, "TIME%s" % sl, self.timeout)
        except (requests.RequestException, ValueError):
            return None
        if len(freqs) != 39 or len(energy) < 39:
            return None

        def rows(v):                                  # reshape flat row-major -> k x 39
            v = v[-(k * 39):]
            out = [v[r * 39:(r + 1) * 39] for r in range(k)]
            return [row for row in out if len(row) == 39]
        E, A1, B1, A2, B2 = rows(energy), rows(a1), rows(b1), rows(a2), rows(b2)
        tvals = tvals[-k:]
        if not E or not (len(E) == len(A1) == len(B1) == len(A2) == len(B2)):
            return None

        steps = []
        for r in range(len(E)):
            t = None
            if r < len(tvals):
                t = (datetime(1950, 1, 1) + timedelta(days=tvals[r])).strftime("%Y-%m-%dT%H:%M:%SZ")
            steps.append({
                "time_utc": t,
                "energy": E[r],
                "alpha1": [_ndbc_alpha1(A1[r][j], B1[r][j]) for j in range(39)],
                "alpha2": [_ndbc_alpha2(A2[r][j], B2[r][j], _ndbc_alpha1(A1[r][j], B1[r][j])) for j in range(39)],
                "r1": [math.hypot(A1[r][j], B1[r][j]) for j in range(39)],
                "r2": [math.hypot(A2[r][j], B2[r][j]) for j in range(39)],
            })
        spec = {"freqs": freqs, "steps": steps}
        with self._lock:
            self._spec_cache[ckey] = (time.time(), spec)
        return spec


class QLDProvider(BuoyProvider):
    """Queensland DETSI Coastal Data System -- 9 live coastal wave buoys, open JSON
    (apps.des.qld.gov.au, CORS-enabled). Positions are static (moored), so the station
    list is hardcoded; latest obs (30-min cadence) is fetched per site on demand."""
    source = "QLD"
    source_name = "Queensland DETSI"
    source_url = "https://www.qld.gov.au/environment/coasts-waterways/beach/monitoring"
    license_label = "CC BY 4.0"
    attribution_text = "Source: State of Queensland (DETSI), Coastal Data System"
    stale_after_sec = 6 * 3600
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 6 * 3600
    timeout = 40
    FEED = "https://apps.des.qld.gov.au/data-sets/wave/wave-%s.json"
    SITES = [
        # (id, name, lat, lon)  -- moored, fixed positions
        (59,   "Albatross Bay (Weipa)", -12.687017, 141.685167),
        (4183, "Brisbane",              -27.49,      153.6341),
        (54,   "Caloundra",             -26.847133,  153.156133),
        (96,   "Emu Park",              -23.30275,   151.069017),
        (60,   "Gladstone",             -23.8950666, 151.5026333),
        (4740, "Mackay",                -21.03592,   149.5482),
        (4,    "Mooloolaba",            -26.566567,  153.181117),
        (52,   "North Moreton Bay",     -26.899783,  153.2822),
        (10,   "Townsville",            -19.176133,  147.074953),
    ]

    def _fetch_stations(self):
        return [{"local_id": str(i), "name": nm, "lat": lat, "lon": lon}
                for (i, nm, lat, lon) in self.SITES]

    @staticmethod
    def _obs(rec):
        def num(k):
            v = rec.get(k)
            try:
                return float(v) if v not in (None, "", "NaN") else None
            except (TypeError, ValueError):
                return None
        return {
            "time_utc": _iso_to_z(rec.get("DateTime")),
            "hs_m": num("Hsig"),
            "hmax_m": num("Hmax"),
            "tp_s": num("Tp"),
            "tz_s": num("Tz"),
            "dir_deg": num("Direction"),
            "sst_c": num("SST"),
            "dir_kind": "from",
        }

    def detail(self, local_id):
        try:
            r = self.http.get(self.FEED % local_id, timeout=self.timeout)
            if r.status_code != 200:
                return {"latest": None, "recent": []}
            arr = r.json()
        except (ValueError, requests.RequestException):
            return {"latest": None, "recent": []}
        if not arr:
            return {"latest": None, "recent": []}
        obs = [self._obs(rec) for rec in arr]      # feed is chronological (newest last)
        return {"latest": obs[-1], "recent": _recent_window(obs)}

    def latest(self, local_id):
        return self.detail(local_id)["latest"]


class AusWavesProvider(BuoyProvider):
    """AusWaves (UWA / IMOS) -- national Sofar Spotter network (Australia). Open
    WordPress REST, but the endpoints reject non-browser requests, so we send a
    browser User-Agent + Referer (no token/key needed). The list gives the catalogue
    + per-buoy recency; the per-buoy feed gives the timeseries (rows are NOT sorted,
    so we take the newest by unix time; -9999 = missing). Bulk + sea/swell params."""
    source = "AusWaves"
    source_name = "AusWaves (UWA / IMOS)"
    source_url = "https://auswaves.org"
    license_label = "CC BY 4.0"
    attribution_text = ("Source: AusWaves (University of Western Australia) and "
                        "Australia's Integrated Marine Observing System (IMOS)")
    stale_after_sec = 12 * 3600
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 1800
    timeout = 40
    LIST = "https://auswaves.org/wp-json/waves/v1/list?type=all"
    BUOY = "https://auswaves.org/wp-json/waves/v1/buoys/%s"
    FRESH_SEC = 24 * 3600                  # skip catalogue entries not updated within this
    HEADERS = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
        "Referer": "https://auswaves.org/",
        "Accept": "application/json",
    }

    def _fetch_stations(self):
        # A request error or a non-200 answer RAISES: the base class then keeps the last good
        # list (fail-soft used to return [], which emptied the layer for a whole TTL).
        r = self.http.get(self.LIST, headers=self.HEADERS, timeout=self.timeout)
        if r.status_code != 200:
            raise RuntimeError("HTTP %s from the AusWaves catalogue" % r.status_code)
        catalogue = r.json()
        now = time.time()
        out = []
        for s in catalogue:
            try:
                if str(s.get("is_enabled")) != "1":
                    continue
                if str(s.get("drifting")) in ("1", "true", "True"):
                    continue              # drifting buoys have no fixed marker position
                lu = s.get("last_update")
                lu = float(lu) if lu not in (None, "", "0") else 0.0
                if not lu or (now - lu) > self.FRESH_SEC:
                    continue              # stale / decommissioned
                lat = float(s["lat"])
                lon = float(s["lng"])
                name = s.get("web_display_name") or s.get("label") or str(s["id"])
                out.append({"local_id": str(s["id"]), "name": name, "lat": lat, "lon": lon,
                            "latest_time": _unix_to_z(lu)})
            except (KeyError, ValueError, TypeError):
                continue
        return out

    @staticmethod
    def _utime(rec):
        try:
            return int(rec.get("Time (UNIX/UTC)") or 0)
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _obs(cls, rec):
        def num(k):
            v = rec.get(k)
            if v is None:
                return None
            try:
                f = float(str(v).strip())
            except ValueError:
                return None
            return None if f <= -9990 else f
        ut = cls._utime(rec)
        obs = {
            "time_utc": _unix_to_z(ut) if ut else None,
            "hs_m": num("Hsig (m)"),
            "tp_s": num("Tp (s)"),
            "mean_period_s": num("Tm (s)"),
            "dir_deg": num("Dp (deg)"),
            "sst_c": num("SST (degC)"),
            "dir_kind": "from",
        }
        # Precomputed 2-way sea/swell split (Sofar Spotter/Smart-Mooring units only;
        # intermittently populated -> include a partition only when its height is real).
        parts = []
        sw_h = num("Hsig_swell (m)")
        if sw_h is not None:
            parts.append({"label": "Swell", "hs_m": sw_h, "period_s": num("Tm_swell (s)"),
                          "dir_deg": num("Dm_swell (deg)"), "spread_deg": num("DmSpr_swell (deg)")})
        se_h = num("Hsig_sea (m)")
        if se_h is not None:
            parts.append({"label": "Wind sea", "hs_m": se_h, "period_s": num("Tm_sea (s)"),
                          "dir_deg": num("Dm_sea (deg)"), "spread_deg": num("DmSpr_sea (deg)")})
        if parts:
            obs["partitions"] = parts
        return obs

    def detail(self, local_id):
        try:
            r = self.http.get(self.BUOY % local_id, headers=self.HEADERS, timeout=self.timeout)
            if r.status_code != 200:
                return {"latest": None, "recent": []}
            d = r.json()
        except (ValueError, requests.RequestException):
            return {"latest": None, "recent": []}
        data = d.get("data") if isinstance(d, dict) else None
        if not data:
            return {"latest": None, "recent": []}
        rows = sorted((rec for rec in data if self._utime(rec) > 0), key=self._utime)
        obs = [self._obs(rec) for rec in rows]     # chronological (the array is unsorted)
        if not obs:
            return {"latest": None, "recent": []}
        return {"latest": obs[-1], "recent": _recent_window(obs)}

    def latest(self, local_id):
        return self.detail(local_id)["latest"]


class IrishMarineProvider(BuoyProvider):
    """Marine Institute Ireland -- Irish Weather Buoy Network (IWBNetwork) realtime ERDDAP.
    Combined met+wave buoys (M2/M3/M5/M6). NOTE: the 'IWaveBNetwork*' datasets are stale;
    the live data is in 'IWBNetwork'. One call (last 2 days, orderBy station_id,time) ->
    station list + latest + 24h history per buoy. SI units, degrees-true 'from'."""
    source = "MI-IE"
    source_name = "Marine Institute (Ireland)"
    source_url = "https://www.marine.ie"
    license_label = "CC BY 4.0"
    attribution_text = "Source: Marine Institute Ireland, Irish Weather Buoy Network"
    stale_after_sec = 6 * 3600
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 1800
    timeout = 40
    # The per-station fallback runs on a visitor's request thread: one attempt, short timeouts.
    FALLBACK_TIMEOUT = (3.05, 8)
    BASE = "https://erddap.marine.ie/erddap/tabledap/IWBNetwork"
    COLS = ("station_id,time,latitude,longitude,WaveHeight,WavePeriod,Tp,"
            "MeanWaveDirection,Hmax,SeaTemperature,SprTp")

    def __init__(self, http=None):
        super().__init__(http)
        self._latest_by_id = {}
        self._recent_by_id = {}

    @staticmethod
    def _obs(row, ix):
        def num(c):
            if c not in ix or ix[c] >= len(row):
                return None
            v = row[ix[c]]
            try:
                return float(v) if v not in ("", "NaN") else None
            except ValueError:
                return None
        return {
            "time_utc": _z(row[ix["time"]]) if "time" in ix and ix["time"] < len(row) else None,
            "hs_m": num("WaveHeight"),
            "tp_s": num("Tp"),
            "mean_period_s": num("WavePeriod"),
            "dir_deg": num("MeanWaveDirection"),
            "dir_spread_deg": num("SprTp"),
            "hmax_m": num("Hmax"),
            "sst_c": num("SeaTemperature"),
            "dir_kind": "from",
        }

    def _fetch_stations(self):
        # ONE call -> station list + latest + 24h history per buoy (only 4 buoys). Robust
        # vs per-click ERDDAP timeouts that previously could leave a buoy history-less.
        url = (self.BASE + ".csv?" + self.COLS +
               "&time%3E=now-2days&orderBy(%22station_id,time%22)")
        hdr, rows = _erddap_rows(self.http, url, self.timeout)
        if not hdr:
            return []
        ix = {c: i for i, c in enumerate(hdr)}
        bysite = {}
        for row in rows:
            try:
                bysite.setdefault(row[ix["station_id"]], []).append(row)
            except (KeyError, IndexError):
                continue
        out, latest, recent = [], {}, {}
        for sid, srows in bysite.items():
            obs = [self._obs(r, ix) for r in srows]      # chronological per station
            if not obs:
                continue
            try:
                lat = float(srows[-1][ix["latitude"]])
                lon = float(srows[-1][ix["longitude"]])
            except (KeyError, ValueError, IndexError):
                continue
            latest[sid] = obs[-1]
            recent[sid] = _recent_window(obs)
            out.append({"local_id": sid, "name": "Ireland %s" % sid, "lat": lat, "lon": lon,
                        "latest_time": obs[-1].get("time_utc")})
        self._latest_by_id = latest
        self._recent_by_id = recent
        return out

    def detail(self, local_id):
        if local_id not in self._latest_by_id:
            self.list_stations()
        rec = self._recent_by_id.get(local_id)
        if rec:
            return {"latest": self._latest_by_id.get(local_id), "recent": rec}
        # Not in the list in hand. When the list's last refresh FAILED the server is known to be down
        # (its ERDDAP read-times-out 3 x 40 s on bad days): asking it again here would hold a visitor's
        # request thread for minutes, so answer with what we have; the background refresh retries it.
        with self._lock:
            failing = self._last_error is not None
        if failing:
            return {"latest": self._latest_by_id.get(local_id), "recent": []}
        # Otherwise a direct per-station history fetch (a buoy outside the list's window), one attempt
        # with short timeouts: belt-and-braces vs the prior single-row bug, never minutes on a request.
        url = (self.BASE + ".csv?" + self.COLS + "&station_id=%22" + str(local_id) +
               "%22&time%3E=now-2days&orderBy(%22time%22)")
        try:
            hdr, rows = _erddap_rows(self.http, url, self.FALLBACK_TIMEOUT, get=_one_shot_get)
        except requests.RequestException:
            hdr, rows = [], []
        if hdr and rows:
            ix = {c: i for i, c in enumerate(hdr)}
            obs = [self._obs(r, ix) for r in rows]
            if obs:
                return {"latest": obs[-1], "recent": _recent_window(obs)}
        return {"latest": self._latest_by_id.get(local_id), "recent": []}

    def latest(self, local_id):
        if local_id not in self._latest_by_id:
            self.list_stations()
        return self._latest_by_id.get(local_id)


class CefasWaveNetProvider(BuoyProvider):
    """CEFAS WaveNet (UK) -- one open JSON Summary call returns every platform with its
    LATEST values. Aggregates Cefas + Met Office + Channel Coastal Observatory + Marine
    Institute buoys (~86 platforms). Latest-only (open history needs registration)."""
    source = "CEFAS"
    source_name = "CEFAS WaveNet (UK)"
    source_url = "https://wavenet.cefas.co.uk"
    license_label = "Open Government Licence"
    attribution_text = ("Source: Cefas WaveNet (incl. Met Office, Channel Coastal "
                        "Observatory, Marine Institute partner buoys)")
    stale_after_sec = 6 * 3600
    capabilities = _caps(bulk=True, directional=True)     # latest-only, no recent history
    list_ttl_sec = 1800
    timeout = 40
    URL = "https://wavenet-api.cefas.co.uk/api/Summary"

    def __init__(self, http=None):
        super().__init__(http)
        self._latest_by_id = {}

    def _fetch_stations(self):
        r = self.http.get(self.URL, timeout=self.timeout)      # errors raise -> keep-last
        if r.status_code != 200:
            raise RuntimeError("HTTP %s from the WaveNet summary" % r.status_code)
        data = r.json()
        out, latest = [], {}
        for p in data:
            try:
                lat = float(p["latitude"])
                lon = float(p["longitude"])
                pid = p["platformId"]
            except (KeyError, ValueError, TypeError):
                continue
            res = {}
            for item in (p.get("results") or []):
                k = item.get("identifier")
                if not k:
                    continue
                try:
                    res[k] = float(item.get("value"))
                except (TypeError, ValueError):
                    res[k] = None
            if res.get("Hm0") is None:        # only platforms reporting a live wave height
                continue
            latest[pid] = {
                "time_utc": _z(p.get("timestamp")),
                "hs_m": res.get("Hm0"),
                "tp_s": res.get("Tpeak"),
                "mean_period_s": res.get("Tz"),
                "dir_deg": res.get("W_PDIR"),
                "dir_spread_deg": res.get("W_SPR"),
                "sst_c": res.get("TEMP"),
                "dir_kind": "from",
            }
            out.append({"local_id": pid, "name": p.get("description") or pid,
                        "lat": lat, "lon": lon, "latest_time": latest[pid].get("time_utc")})
        self._latest_by_id = latest
        return out

    def detail(self, local_id):
        if local_id not in self._latest_by_id:
            self.list_stations()
        return {"latest": self._latest_by_id.get(local_id), "recent": []}

    def latest(self, local_id):
        return self.detail(local_id)["latest"]


def _station_priority(st):
    """Higher = preferred when two sources report the same physical buoy. Richness-first
    so the kept marker carries the best data: NDBC spectra > AODN spectra > AusWaves
    sea/swell split > clean agency bulk > AODN bulk."""
    caps = st.get("capabilities") or {}
    src = st.get("source")
    if src == "NDBC":
        return 100
    if caps.get("spectra"):            # AODN spectra buoys -> NDBC-style partitions
        return 90
    if src == "AusWaves":              # precomputed sea/swell split (where populated)
        return 70
    if src == "MI-IE":                 # Ireland ERDDAP (bulk + 24h history)
        return 65
    if src == "RWS":                   # Netherlands Waterinfo (bulk + history)
        return 62
    if src == "QLD":                   # clean 30-min agency JSON (+ SST)
        return 60
    if src == "SMHI":                  # Sweden (bulk + history)
        return 60
    if src == "CWA":                   # Taiwan (bulk, mean period, 48 h; plan section 69)
        return 60
    if src == "CCO":                   # UK Channel Coastal Observatory: the programme's own buoys, 30-min, with
        return 59                      # spread and 24 h (plan section 69): above CEFAS's latest-only copies
    if src == "CEFAS":                 # UK WaveNet aggregate (latest-only)
        return 58
    if src == "CDIP":
        return 55
    if src == "AODN":                  # bulk only
        return 50
    if src == "GLOS":                  # Great Lakes, 10-min with direction + history (plan section 63); below
        return 49                      # AODN only so the list keeps its pre-section-63 order (they never meet)
    if src == "IOOS":                  # US regional-association aggregator (relays NDBC / CDIP too)
        return 47
    if src == "NDBCW":                 # NDBC stations without a spectral file (bulk report only)
        return 45
    if src == "CMEMS":                 # pan-EU/global aggregator -> fills gaps, national wins
        return 30
    return 40


class SmhiProvider(BuoyProvider):
    """SMHI (Sweden) open oceanographic API -- Skagerrak/Kattegat/Baltic wave buoys. Station
    list from the Hs-parameter endpoint; per-station detail merges several per-parameter
    'latest-day' time series (the API is one call per parameter per station). SI, 'from'."""
    source = "SMHI"
    source_name = "SMHI (Sweden)"
    source_url = "https://www.smhi.se"
    license_label = "CC BY 4.0"
    attribution_text = "Source: SMHI (Swedish Meteorological and Hydrological Institute)"
    stale_after_sec = 6 * 3600
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 3600
    timeout = 40
    BASE = "https://opendata-download-ocobs.smhi.se/api/version/latest"
    PARAMS = (("hs_m", 1), ("tp_s", 9), ("mean_period_s", 10), ("dir_deg", 7), ("hmax_m", 11))

    def _fetch_stations(self):
        r = self.http.get(self.BASE + "/parameter/1.json", timeout=self.timeout)   # errors raise
        if r.status_code != 200:
            raise RuntimeError("HTTP %s from the SMHI station list" % r.status_code)
        d = r.json()
        out = []
        for s in d.get("station", []):
            if not s.get("active"):
                continue
            try:
                out.append({"local_id": str(s["key"]), "name": s.get("name") or str(s["key"]),
                            "lat": float(s["latitude"]), "lon": float(s["longitude"])})
            except (KeyError, ValueError, TypeError):
                continue
        return out

    def _series(self, key, param):
        try:
            r = self.http.get(self.BASE + "/parameter/%d/station/%s/period/latest-day/data.json"
                              % (param, key), timeout=self.timeout)
            if r.status_code != 200:
                return {}
            vals = r.json().get("value") or []
        except (ValueError, requests.RequestException):
            return {}
        out = {}
        for v in vals:
            try:
                out[int(v["date"])] = float(v["value"])
            except (KeyError, TypeError, ValueError):
                pass
        return out

    def detail(self, local_id):
        series = {key: self._series(local_id, p) for key, p in self.PARAMS}
        times = sorted(series["hs_m"].keys())
        if not times:
            return {"latest": None, "recent": []}
        obs = []
        for t in times:
            row = {"time_utc": _unix_to_z(t // 1000), "dir_kind": "from"}
            for key, _ in self.PARAMS:
                row[key] = series[key].get(t)
            obs.append(row)
        return {"latest": obs[-1], "recent": _recent_window(obs)}

    def latest(self, local_id):
        return self.detail(local_id)["latest"]


class RwsProvider(BuoyProvider):
    """Rijkswaterstaat (Netherlands) Waterinfo -- North Sea offshore wave stations. Open POST
    JSON API on the new ddapi20 host. Heights are in CENTIMETRES (->/100=m). Hm0/Tm02/Th0.
    Per-station detail merges per-grootheid series by timestamp (one POST per parameter)."""
    source = "RWS"
    source_name = "Rijkswaterstaat (Netherlands)"
    source_url = "https://waterinfo.rws.nl"
    license_label = "Public domain (CC0)"
    attribution_text = "Source: Rijkswaterstaat (Netherlands), Waterinfo"
    stale_after_sec = 6 * 3600
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 6 * 3600
    timeout = 40
    URL = ("https://ddapi20-waterwebservices.rijkswaterstaat.nl/"
           "ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen")
    SITES = [
        # (code, name, lat, lon) -- fixed offshore platforms/buoys
        ("europlatform", "Europlatform", 51.99781, 3.275071),
        ("goeree.lichteiland", "Goeree Lichteiland", 51.925034, 3.668416),
        ("eurogeul.e13", "Eurogeul E13", 52.009184, 3.741804),
        ("ijgeul", "IJgeul", 52.462272, 4.482472),
        ("hollandsekust.zuid.alpha", "Hollandse Kust Zuid Alpha", 52.232547, 4.19569),
        ("f3", "F3", 54.853199, 4.726133),
        ("j6", "J6", 53.816632, 2.95001),
        ("l9", "L9", 53.616667, 4.966667),
    ]
    GROOTHEDEN = (("Hm0", "hs_m", 0.01), ("Tm02", "mean_period_s", 1.0), ("Th0", "dir_deg", 1.0))

    def _fetch_stations(self):
        return [{"local_id": code, "name": name, "lat": lat, "lon": lon}
                for (code, name, lat, lon) in self.SITES]

    def _series(self, code, grootheid):
        now = datetime.now(timezone.utc)
        body = {
            "AquoPlusWaarnemingMetadata": {"AquoMetadata": {
                "Compartiment": {"Code": "OW"}, "Grootheid": {"Code": grootheid}}},
            "Locatie": {"Code": code, "Coordinatenstelsel": "ETRS89"},
            "Periode": {
                "Begindatumtijd": (now - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S.000+00:00"),
                "Einddatumtijd": now.strftime("%Y-%m-%dT%H:%M:%S.000+00:00")},
        }
        try:
            r = self.http.post(self.URL, json=body, timeout=self.timeout)
            if r.status_code != 200:
                return {}
            d = r.json()
        except (ValueError, requests.RequestException):
            return {}
        out = {}
        for w in (d.get("WaarnemingenLijst") or []):
            for m in (w.get("MetingenLijst") or []):
                t = m.get("Tijdstip")
                v = (m.get("Meetwaarde") or {}).get("Waarde_Numeriek")
                if t is None or v is None:
                    continue
                try:
                    fv = float(v)
                    if fv < 1e5:                       # RWS missing-value sentinel is huge
                        out[t] = fv
                except (TypeError, ValueError):
                    pass
        return out

    def detail(self, local_id):
        series = {}                            # key -> {timestamp: scaled value}
        for i, (g, key, sc) in enumerate(self.GROOTHEDEN):
            if i:
                time.sleep(0.4)                # RWS load-sheds rapid POSTs -> pace them out
            series[key] = {t: v * sc for t, v in self._series(local_id, g).items()}
        hs = series.get("hs_m") or {}
        if not hs:
            return {"latest": None, "recent": []}

        def newest(s):
            return s[max(s)] if s else None
        # 24h history on the Hs timeline; other params filled where their timestamp aligns.
        obs = []
        for t in sorted(hs.keys()):
            row = {"time_utc": _iso_to_z(t), "dir_kind": "from"}
            for g, key, sc in self.GROOTHEDEN:
                row[key] = series[key].get(t)
            obs.append(row)
        # Latest reading: each parameter's OWN most-recent value (they can lag each other a
        # step, and some gauges report no direction), so the current cards aren't left blank.
        latest = {"time_utc": _iso_to_z(max(hs.keys())), "dir_kind": "from"}
        for g, key, sc in self.GROOTHEDEN:
            latest[key] = newest(series.get(key))
        return {"latest": latest, "recent": _recent_window(obs)}

    def latest(self, local_id):
        return self.detail(local_id)["latest"]


class CopernicusProvider(BuoyProvider):
    """Copernicus Marine in-situ NRT -- the open, anonymous-S3 pan-EU/global aggregator.
    The index CSV yields the station list + positions + latest-obs time (no NetCDF needed);
    per-platform values + history come from the platform's daily NetCDF on demand (h5netcdf).
    It re-bundles the national/EuroGOOS feeds -> LOWEST dedup priority. ~6-30h NRT latency.
    Scoped to a European bounding box for now (widen BBOX for worldwide coverage)."""
    source = "CMEMS"
    source_name = "Copernicus Marine in-situ"
    source_url = "https://marine.copernicus.eu"
    license_label = "Copernicus Marine Service (free)"
    attribution_text = ("Source: Copernicus Marine Service in-situ TAC "
                        "(E.U. Copernicus Marine Service Information)")
    stale_after_sec = 36 * 3600          # NRT dispatch lag ~6-30h
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 3 * 3600
    timeout = 60
    S3 = "https://s3.waw3-1.cloudferro.com/mdl-native-01/native/"
    DATASET = ("INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
               "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/")
    BBOX = (30.0, 73.0, -30.0, 42.0)     # (lat_min, lat_max, lon_min, lon_max): Europe
    LIVE_MAX_AGE = 4 * 86400             # only platforms whose latest file is this fresh
    OUTLIER_KM = 1.0                     # a newest position this far from two agreeing earlier ones
    HISTORY_MAX_AGE = 14 * 86400         # files old enough to judge a newest position against

    def __init__(self, http=None):
        super().__init__(http)
        self._file_by_id = {}            # local_id -> relative .nc path (latest per platform)

    @staticmethod
    def _decoded_lines(resp):
        """Yield the index as text lines straight off the socket. The index is ~43 MB; holding
        it as one string (plus the StringIO copy the csv module used to read from) peaked at
        ~260 MB of heap on every 3-hourly refresh and after every restart -- the single largest
        allocation in the process. Streaming keeps the peak at one chunk."""
        for line in resp.iter_lines(decode_unicode=True):
            if isinstance(line, bytes):            # no charset on the response -> bytes
                line = line.decode("utf-8", "replace")
            yield line

    def _fetch_stations(self):
        resp = self.http.get(self.S3 + self.DATASET + "index_latest.txt",
                             timeout=self.timeout, stream=True)     # errors raise -> keep-last
        if resp.status_code != 200:
            resp.close()
            raise RuntimeError("HTTP %s from the CMEMS index" % resp.status_code)
        latmin, latmax, lonmin, lonmax = self.BBOX
        now = time.time()
        best = {}
        # The newest file's position can be wrong on its own (2026-10-03: VillajoyosaBuoy's newest
        # file put it 6.5 km inland while every earlier one had it 3 km off Villajoyosa). Each
        # platform keeps its three newest single-position file records (constant memory: this index
        # is ~43 MB); a newest position more than OUTLIER_KM from the two before it, while those two
        # agree, is an outlier and the platform is drawn at the earlier position. A real move shows
        # in two files and is then taken.
        recent = {}
        try:
            for r in csv.reader(self._decoded_lines(resp)):
                if not r or r[0].startswith("#") or len(r) < 8:
                    continue
                if "VHM0" not in r[-1] and "VAVH" not in r[-1]:    # parameters column
                    continue
                try:
                    la = (float(r[2]) + float(r[3])) / 2.0
                    lo = (float(r[4]) + float(r[5])) / 2.0
                except (ValueError, IndexError):
                    continue
                if not (latmin <= la <= latmax and lonmin <= lo <= lonmax):
                    continue
                tend = r[7].strip()
                tz = tend if tend.endswith("Z") else tend + "Z"
                ep = _z_epoch({"time_utc": tz})
                if ep is None or (now - ep) > self.HISTORY_MAX_AGE:
                    continue
                fn = r[1]
                pid = fn.split("/")[-1].rsplit("_", 1)[0]          # GL_TS_MO_6200064_DATE.nc -> GL_TS_MO_6200064
                # compact per platform: up to 3 packed (t, lat, lon) records, newest first; only
                # files that report ONE position (box <= OUTLIER_KM) are evidence: a full-day row's
                # box can span 3-19 km and its midpoint is nowhere the buoy was (Cerema buoys)
                rec = _POS3.pack(int(ep), la, lo)
                old = recent.get(pid)
                if haversine_km(float(r[2]), float(r[4]), float(r[3]), float(r[5])) > self.OUTLIER_KM:
                    pass
                elif old is None:
                    recent[pid] = rec
                elif len(old) < 72 or int(ep) > _POS3.unpack_from(old, 48)[0]:
                    recs = [old[i:i + 24] for i in range(0, len(old), 24)] + [rec]
                    recs.sort(key=lambda b: _POS3.unpack(b)[0], reverse=True)
                    recent[pid] = b"".join(recs[:3])
                if (now - ep) > self.LIVE_MAX_AGE:                  # skip long-inactive platforms
                    continue
                if pid not in best or tend > best[pid][0]:
                    best[pid] = (tend, fn, la, lo, tz)
        finally:                               # a body that fails mid-stream raises -> keep-last
            resp.close()
        out = []
        files = {}                             # swapped in whole below: detail() never sees a half map
        for pid, (tend, fn, la, lo, tz) in best.items():
            files[pid] = fn
            # the two newest single-position files OLDER than the newest file: if they agree and the
            # newest file's position is more than OUTLIER_KM from them, it is an outlier
            b = recent.get(pid, b"")
            t_new = int(_z_epoch({"time_utc": tz}) or 0)
            older = [rec for rec in (_POS3.unpack(b[i:i + 24]) for i in range(0, len(b), 24)) if rec[0] < t_new]
            if (len(older) >= 2 and haversine_km(older[0][1], older[0][2], older[1][1], older[1][2]) <= self.OUTLIER_KM
                    and haversine_km(la, lo, older[0][1], older[0][2]) > self.OUTLIER_KM):
                la, lo = older[0][1], older[0][2]
            name = pid.split("_")[-1].replace("-", " ") if "_" in pid else pid
            out.append({"local_id": pid, "name": name, "lat": la, "lon": lo, "latest_time": tz})
        self._file_by_id = files
        return out

    def detail(self, local_id):
        fn = self._file_by_id.get(local_id)
        if not fn:
            self.list_stations()
            fn = self._file_by_id.get(local_id)
        if not fn:
            return {"latest": None, "recent": []}
        try:
            import h5netcdf
            import numpy as np
            data = self.http.get(self.S3 + fn, timeout=self.timeout).content
            ds = h5netcdf.File(io.BytesIO(data), "r")
        except Exception:
            return {"latest": None, "recent": []}
        try:
            tvar = np.asarray(ds["TIME"][:]).astype("float64").ravel()

            def col(*names):
                for nm in names:
                    if nm in ds.variables:
                        a = np.asarray(ds[nm][:]).astype("float64")
                        if a.ndim > 1:
                            a = a[:, 0]                 # surface (DEPTH=0)
                        a = a.ravel()
                        a[a > 1e30] = np.nan            # _FillValue ~9.97e36
                        return a
                return None
            hs, tp = col("VHM0", "VAVH"), col("VTPK")
            mp, dr = col("VTZA", "VTZM", "VTM02"), col("VMDR", "VPED")
        except Exception:
            return {"latest": None, "recent": []}

        def val(a, i):
            if a is None or i >= len(a):
                return None
            return float(a[i]) if np.isfinite(a[i]) else None
        obs = []
        for i in range(len(tvar)):
            t = datetime(1950, 1, 1) + timedelta(days=float(tvar[i]))
            h = val(hs, i)
            if h is None:
                continue                                # keep only rows with a real Hs
            obs.append({"time_utc": t.strftime("%Y-%m-%dT%H:%M:%SZ"), "hs_m": h,
                        "tp_s": val(tp, i), "mean_period_s": val(mp, i),
                        "dir_deg": val(dr, i), "dir_kind": "from"})
        if not obs:
            return {"latest": None, "recent": []}
        return {"latest": obs[-1], "recent": _recent_window(obs)}

    def latest(self, local_id):
        return self.detail(local_id)["latest"]


def _name_key(name):
    """Normalized buoy designation for cross-source matching: strip generic descriptors +
    source words + punctuation so 'Ireland M6' and 'M6 Buoy' both reduce to 'M6'. Used as a
    SECONDARY dedup signal (with a looser radius) for the same physical buoy relayed by two
    networks at slightly different reported positions. Keeps distinguishing words like
    INNER/OUTER/NORTH so 'Newcastle Inner' != 'Newcastle Outer'."""
    if not name:
        return ""
    n = name.upper()
    for w in ("WAVENET", "BUOY", "SITE", "STATION", "IRELAND", "WAVE", "OFFSHORE", "INSHORE"):
        n = n.replace(w, " ")
    return re.sub(r"[^A-Z0-9]", "", n)


def _match_name(st):
    """The name the dedup compares. AODN markers are named "<site> - <institution>", so their full name
    never equalled another network's name for the same buoy (2026-10-03: Storm Bay, Wilsons Prom, Mission
    Beach, Inverloch drawn twice 1.7-2.5 km apart); they are matched on the site name alone."""
    name = st.get("name") or ""
    if st.get("source") == "AODN":
        return name.split(" - ")[0]
    return name


# Feeds whose local ids are NDBC / WMO buoy numbers (5 digits, or a C-MAN call sign like FRFN7).
_NUMBERED_SOURCES = frozenset(("NDBC", "NDBCW", "GLOS", "IOOS"))
_BUOY_NUMBER_RE = re.compile(r"^(?:\d{5}|[A-Z]{4}\d)$")


# Another network's id of a numbered buoy (the page's LINK_ALIAS, owner 2026-10-25): CEFAS lists the UK
# platforms Sandettie LV, Clipper and Lomond that NDBC relays as 62304 / 62144 / 62146, 6-7 km from NDBC's
# positions (G42 F10)
_BUOY_ALIAS = {"cefas:8": "62304", "cefas:10": "62144", "cefas:305": "62146"}
_CMEMS_WMO_RE = re.compile(r"^cmems:[A-Z]{2}_TS_MO_6200(\d{3})$", re.I)


def _buoy_number(st):
    """The station's NDBC / WMO number when its feed names buoys by number (or _BUOY_ALIAS names it), else ''.
    Two feeds' markers with the same number are the same buoy even when their positions disagree (2026-10-30:
    the IOOS ERDDAP placed NDBC's 41056 133 km from NDBC's own position)."""
    alias = _BUOY_ALIAS.get(str(st.get("id") or "").lower())
    if alias:
        return alias
    m = _CMEMS_WMO_RE.match(str(st.get("id") or ""))   # cmems:NO_TS_MO_6200144 is WMO 62144 (the page's linkId)
    if m:                                               # (G42 re-check N2)
        return "62" + m.group(1)
    if st.get("source") not in _NUMBERED_SOURCES:
        return ""
    local = str(st.get("id") or "").split(":", 1)[-1].upper()
    return local if _BUOY_NUMBER_RE.match(local) else ""


def merge_stations(provider_lists, radius_km=1.0, name_radius_km=10.0, number_radius_km=300.0):
    """Flatten provider station lists and physically dedup co-located buoys, keeping the
    RICHEST source per cluster (see _station_priority: spectra > sea/swell split > bulk).
    Two cross-source markers merge when within radius_km, OR within the looser name_radius_km
    if their normalized names match (catches the SAME buoy relayed by two networks at slightly
    different reported positions, e.g. Marine Institute M6 vs CEFAS 'M6 Buoy' ~3.6 km apart).
    A hidden duplicate is marked dup_of the kept one; the kept marker lists the extra
    source(s) in 'also_sources'.
    """
    allst = [st for lst in provider_lists for st in lst]
    allst.sort(key=_station_priority, reverse=True)   # stable: ties keep provider order
    kept, keptkey, keptnum = [], [], []
    for st in allst:
        nk = _name_key(_match_name(st))
        num = _buoy_number(st)
        dup = None
        for idx, k in enumerate(kept):
            if k["source"] == st["source"]:
                continue
            dist = haversine_km(k["lat"], k["lon"], st["lat"], st["lon"])
            if (dist <= radius_km or (len(nk) >= 2 and nk == keptkey[idx] and dist <= name_radius_km)
                    or (num and num == keptnum[idx] and dist <= number_radius_km)):
                dup = k
                break
        if dup is not None:
            st = dict(st, dup_of=dup["id"])
            dup.setdefault("also_sources", [])
            if st["source"] not in dup["also_sources"]:
                dup["also_sources"].append(st["source"])
        else:
            kept.append(st)
            keptkey.append(nk)
            keptnum.append(num)
    return kept


# ----------------------- US widen (plan section 63) -----------------------------
# Three more US feeds, each below every national feed with spectra in the dedup order:
#   NDBCW  NDBC stations that report waves in latest_obs.txt but publish no spectral file
#          (Canada's DFO buoys, Great Lakes partner buoys, CORMP, CBIBS ...); the NDBC provider
#          lists only the stations with a .data_spec file.
#   GLOS   Great Lakes Observing System Seagull (anonymous JSON; Sofar Spotters and partner
#          buoys, 10-minute data with direction and history).
#   IOOS   the IOOS sensors ERDDAP (Axiom): the regional associations' wave datasets in one
#          place (CORMP, USACE, CBIBS, CARICOOS, UNC-CSI ...).
# Their station lists are fetched on the refresh runner; a window's 24 hours are asked on
# demand with one short request and kept a few minutes (a refresh press asks again).

class _CappedBody:
    """A fully read response whose body was read under a byte cap: status_code, content, text, json()."""

    def __init__(self, status_code, content, encoding):
        self.status_code = status_code
        self.content = content
        self.encoding = encoding or "utf-8"

    @property
    def text(self):
        return self.content.decode(self.encoding, "replace")

    def json(self):
        return json.loads(self.text)


LIST_MAX_S = 180            # a list download that has not finished after this is given up (the refresh runner's worker)
DETAIL_MAX_S = 20           # a window's download (a visitor's request thread)


def _capped_fetch(http, url, timeout, cap, one_shot=False, max_s=None):
    """GET url with its body read in chunks under `cap` bytes and `max_s` seconds in all: a feed that answers 300 MB,
    or a trickle, is cut off before it fills the instance's memory or holds a thread (G42 F7, F8). Any status is
    returned (a caller reads an error page's text). `one_shot`: no automatic retries (a visitor's request thread). A
    test's fake http (not a requests.Session) is called as is and its body checked against the cap."""
    where = url.split("?")[0]
    if not isinstance(http, requests.Session):
        r = http.get(url, timeout=timeout)
        if len(r.content) > cap:
            raise RuntimeError("%s answered %d bytes (cap %d)" % (where, len(r.content), cap))
        return r
    t0 = time.monotonic()
    # A check between chunks never fires on a body smaller than one chunk, so a body that trickles is also cut from
    # outside when max_s runs out (G42 re-check N4): its socket's reads get the time left as their timeout (a stall
    # ends then), and a timer shuts the socket (a blocked read ends at once on Linux; on Windows the next read after a
    # trickled byte does). urllib3 sets the timeout again for the next answer on a pooled connection.
    held = {"sock": None, "shut": None, "late": False, "done": False}
    guard = threading.Lock()

    def cut():
        with guard:
            if held["done"]:
                return
            held["late"] = True
            sock, shut = held["sock"], held["shut"]
        for stop in (shut, sock and (lambda: sock.shutdown(socket.SHUT_RDWR))):
            if stop:
                try:
                    stop()
                except Exception:
                    pass

    def over():
        return held["late"] or (max_s is not None and time.monotonic() - t0 >= max_s)

    killer = None
    if max_s is not None:
        killer = threading.Timer(max_s, cut)
        killer.daemon = True
        killer.start()
    try:
        r = (requests.get if one_shot else http.get)(url, timeout=timeout, stream=True)
        try:
            with guard:
                held["sock"] = _answer_socket(r)
                held["shut"] = getattr(r.raw, "shutdown", None)
                late = held["late"]
            if late:
                cut()
            elif max_s is not None and held["sock"] is not None:
                try:
                    held["sock"].settimeout(max(0.01, max_s - (time.monotonic() - t0)))
                except Exception:
                    pass
            n = r.headers.get("Content-Length")
            if n and n.isdigit() and int(n) > cap:
                raise RuntimeError("%s announced %s bytes (cap %d)" % (where, n, cap))
            buf = bytearray()
            try:
                for chunk in r.iter_content(65536):
                    buf += chunk
                    if len(buf) > cap:
                        raise RuntimeError("%s answered more than %d bytes (cap)" % (where, cap))
                    if max_s is not None and time.monotonic() - t0 > max_s:
                        raise RuntimeError("%s took longer than %g s" % (where, max_s))
            except (requests.RequestException, OSError, ValueError):
                if over():
                    raise RuntimeError("%s took longer than %g s" % (where, max_s))
                raise
            with guard:
                held["done"] = True
            return _CappedBody(r.status_code, bytes(buf), r.encoding)
        finally:
            r.close()
    finally:
        with guard:
            held["done"] = True
        if killer is not None:
            killer.cancel()


def _answer_socket(r):
    """The socket a streamed answer reads from: the pooled connection's, else the one under the answer's file (a
    server that closes after its answer leaves the connection without one). None when neither is there."""
    raw = getattr(r, "raw", None)
    sock = getattr(getattr(raw, "_connection", None), "sock", None)
    if sock is None:
        fp = getattr(getattr(raw, "_fp", None), "fp", None)
        sock = getattr(getattr(fp, "raw", None), "_sock", None)
    return sock if hasattr(sock, "settimeout") else None


def _size_capped_get(http, url, timeout, cap, one_shot=False, max_s=LIST_MAX_S):
    """GET url and return the response after checking its size: a body larger than `cap`
    bytes raises (a feed that suddenly answers 50 MB must not reach the list builder), and so
    does a download past `max_s` seconds. `one_shot`: no automatic retries (a visitor's request thread)."""
    r = _capped_fetch(http, url, timeout, cap, one_shot=one_shot, max_s=max_s)
    if r.status_code != 200:
        raise RuntimeError("HTTP %s from %s" % (r.status_code, url.split("?")[0]))
    return r


# One reading check for every section-63 feed and the wave probe (G42 F3): a row stamped more than
# FUTURE_SLACK_S ahead of the clock is left out (one future row would make itself the newest and cut the
# 24-hour window to itself), so is a height outside 0-30 m; any other value outside its range is blanked.
FUTURE_SLACK_S = 3600
HS_MAX_M = 30.0
_FIELD_RANGES = {                       # key -> (lowest, highest, lowest allowed)
    "tp_s": (0.0, 40.0, False),
    "mean_period_s": (0.0, 40.0, False),
    "dir_deg": (0.0, 360.0, True),
    "dir_spread_deg": (0.0, 180.0, True),
    "hmax_m": (0.0, 40.0, True),
    "sst_c": (-5.0, 40.0, True),
}


def _in_range(v, lo, hi, lo_ok=True):
    return v is not None and math.isfinite(v) and (v >= lo if lo_ok else v > lo) and v <= hi


def _sane_reading(o, now=None):
    """The reading with impossible values blanked, or None when it cannot be shown (no time, a time in the
    future, no height or an impossible one)."""
    if not o:
        return None
    ep = _z_epoch(o)
    if ep is None or ep > (time.time() if now is None else now) + FUTURE_SLACK_S:
        return None
    if not _in_range(o.get("hs_m"), 0.0, HS_MAX_M):
        return None
    for key, (lo, hi, lo_ok) in _FIELD_RANGES.items():
        if key in o and o[key] is not None and not _in_range(o[key], lo, hi, lo_ok):
            o[key] = None
    if o.get("partitions"):
        parts = []
        for q in o["partitions"]:
            if not _in_range(q.get("hs_m"), 0.0, HS_MAX_M):
                continue
            if q.get("period_s") is not None and not _in_range(q["period_s"], 0.0, 40.0, False):
                q["period_s"] = None
            if q.get("dir_deg") is not None and not _in_range(q["dir_deg"], 0.0, 360.0):
                q["dir_deg"] = None
            parts.append(q)
        if parts:
            o["partitions"] = parts
        else:
            o.pop("partitions")
    return o


def _ndbc_num(v):
    """An NDBC text-file cell: 'MM' (missing) and anything unreadable -> None."""
    if v is None or v == "MM":
        return None
    try:
        x = float(v)
    except ValueError:
        return None
    return x if math.isfinite(x) else None


def _ndbc_header(line):
    """The column names of an NDBC text file's first header line ('#STN LAT ...' or '#YY MM ...')."""
    return line.lstrip("#").split()


def _ndbc_time(row, ix):
    """'YYYY MM DD hh mm' columns (latest_obs uses YYYY, realtime2 '#YY') -> ISO Z or None."""
    try:
        y = int(row[ix["YYYY"]] if "YYYY" in ix else row[ix["YY"]])
        if y < 100:
            y += 2000
        d = datetime(y, int(row[ix["MM"]]), int(row[ix["DD"]]), int(row[ix["hh"]]), int(row[ix["mm"]]),
                     tzinfo=timezone.utc)
    except (KeyError, IndexError, ValueError):
        return None
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


def _ndbc_wave_obs(row, ix):
    """One NDBC text row -> a reading in the site's units (heights m, periods s, FROM degrees, C)."""
    def num(c):
        return _ndbc_num(row[ix[c]]) if c in ix and ix[c] < len(row) else None
    return {
        "time_utc": _ndbc_time(row, ix),
        "hs_m": num("WVHT"),
        "tp_s": num("DPD"),
        "mean_period_s": num("APD"),
        "dir_deg": num("MWD"),
        "sst_c": num("WTMP"),
        "dir_kind": "from",
    }


def parse_ndbc_latest_waves(text, now=None):
    """NDBC latest_obs.txt -> {station id: {"lat", "lon", "obs"}} for the rows that carry a
    significant wave height (the file lists every station's newest observation, wind-only ones too).
    A row whose dominant period reads exactly 0 is a placeholder, not a reading (Korea's KMA buoys arrive
    through the GTS as 0.5 m / 0 s at every report): left out (G42 F15)."""
    out = {}
    lines = text.splitlines()
    if not lines or not lines[0].startswith("#"):
        raise ValueError("not an NDBC latest_obs file")
    hdr = _ndbc_header(lines[0])
    ix = {c: i for i, c in enumerate(hdr)}
    if not {"STN", "LAT", "LON", "WVHT"} <= set(ix):
        raise ValueError("latest_obs header without STN / LAT / LON / WVHT")
    for line in lines[1:]:
        if not line or line.startswith("#"):
            continue
        row = line.split()
        if len(row) < len(hdr):
            continue
        if "DPD" in ix and _ndbc_num(row[ix["DPD"]]) == 0:
            continue
        obs = _sane_reading(_ndbc_wave_obs(row, ix), now)
        if obs is None:
            continue
        lat, lon = _ndbc_num(row[ix["LAT"]]), _ndbc_num(row[ix["LON"]])
        if lat is None or lon is None or not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue
        out[row[ix["STN"]]] = {"lat": lat, "lon": lon, "obs": obs}
    return out


def parse_ndbc_realtime_waves(lines, max_rows=400, now=None):
    """An NDBC realtime2/<id>.txt (newest first; two header lines) -> its wave readings,
    chronological, rows without a wave height left out. Reads at most `max_rows` data rows."""
    hdr, ix, obs = None, None, []
    for n, line in enumerate(lines):
        if isinstance(line, bytes):
            line = line.decode("ascii", "replace")
        if n == 0:
            if not line.startswith("#"):
                raise ValueError("not an NDBC realtime2 file")
            hdr = _ndbc_header(line)
            ix = {c: i for i, c in enumerate(hdr)}
            if "WVHT" not in ix:
                raise ValueError("realtime2 header without WVHT")
            continue
        if not line or line.startswith("#"):
            continue
        if len(obs) >= max_rows:
            break
        row = line.split()
        if len(row) < len(hdr):
            continue
        o = _sane_reading(_ndbc_wave_obs(row, ix), now)
        if o is not None:
            obs.append(o)
    obs.reverse()
    return obs


class _DetailCache:
    """A window's on-demand answers, kept `ttl` seconds per station (at most `size` stations)."""

    def __init__(self, ttl, size=256):
        self.ttl = ttl
        self.size = size
        self._d = {}
        self._lock = threading.Lock()

    def get(self, key, fresh_s=None):
        """The kept answer, or None when there is none, it is older than ttl, or a refresh
        (fresh_s) finds it older than fresh_s seconds."""
        with self._lock:
            hit = self._d.get(key)
        if not hit:
            return None
        age = time.time() - hit[0]
        if age > self.ttl or (fresh_s is not None and age >= fresh_s):
            return None
        return hit[1]

    def put(self, key, value):
        with self._lock:
            if len(self._d) >= self.size and key not in self._d:
                self._d.pop(next(iter(self._d)))      # oldest first (insertion order)
            self._d[key] = (time.time(), value)


class NdbcBulkProvider(BuoyProvider):
    """NOAA NDBC stations that report waves but publish no spectral file. One GET of the
    all-stations latest_obs.txt (~100 KB) gives the list and the newest reading; a window's
    24 hours come from the station's realtime2/<id>.txt (the file NDBC's own pages read).
    Bulk parameters only: height, dominant and mean period, the direction at the dominant period
    (NDBC's MWD) where the buoy measures it (Canada's DFO buoys do not), water temperature."""
    source = "NDBCW"
    source_name = "NOAA NDBC (bulk report)"
    source_url = "https://www.ndbc.noaa.gov"
    # NDBC relays partner networks' buoys (Environment and Climate Change Canada, the UK Met Office, Great
    # Lakes partners): their data stay under their operators' terms (G42 F12)
    license_label = "Public data (NOAA NDBC; partner buoys under their operators' terms)"
    attribution_text = ("Source: NOAA National Data Buoy Center, incl. partner networks relayed by NDBC (e.g. "
                        "Environment and Climate Change Canada, UK Met Office); partner data under their operators' terms.")
    stale_after_sec = 6 * 3600
    capabilities = _caps(bulk=True, recent_history=True)
    list_ttl_sec = 1800
    timeout = 30
    detail_fresh = True                  # the window's refresh button asks again (app.py route)
    LATEST_URL = "https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt"
    REALTIME_URL = "https://www.ndbc.noaa.gov/data/realtime2/%s.txt"
    LATEST_CAP = 1 << 20
    DETAIL_TIMEOUT = (3.05, 10)
    DETAIL_TTL = 300
    DETAIL_CAP = 4 << 20
    # Korea's KMA buoys arrive through the GTS with placeholder values (0.5 m, period 0, no
    # direction at every report on 2026-10-30): left out until their values look real (the
    # parser also leaves out any row with a 0-s period, so a new id with the same values is too).
    EXCLUDE = frozenset("2210%d" % i for i in range(1, 9))
    _ID_RE = re.compile(r"^[A-Za-z0-9]{3,8}$")

    def __init__(self, http=None, spectral_ids=None, names=None):
        super().__init__(http)
        self._spectral_ids = spectral_ids    # callable -> ids the NDBC provider lists (spectral file)
        self._names = names                  # callable -> {id: name} from activestations.xml
        self._latest_by_id = {}
        self._dir_ids = set()
        self._cache = _DetailCache(self.DETAIL_TTL)

    def _call(self, fn, default):
        if fn is None:
            return default
        try:
            return fn() or default
        except Exception as e:                # the merge's position + name dedup still applies
            _log.warning("buoy provider %s: %s unavailable (%s)", self.source, getattr(fn, "__name__", fn), e)
            return default

    def _fetch_stations(self):
        r = _size_capped_get(self.http, self.LATEST_URL, self.timeout, self.LATEST_CAP)
        table = parse_ndbc_latest_waves(r.text)
        spectral = set(self._call(self._spectral_ids, set()))
        names = self._call(self._names, {})
        out, latest, dirs = [], {}, set()
        for sid, rec in table.items():
            if sid in spectral or sid in self.EXCLUDE or not self._ID_RE.match(sid):
                continue
            obs = rec["obs"]
            latest[sid] = obs
            caps = dict(self.capabilities)
            if obs.get("dir_deg") is not None:
                caps["directional"] = True
                dirs.add(sid)
            name = str(names.get(sid) or "").strip() or sid
            out.append({"local_id": sid, "name": name, "lat": rec["lat"], "lon": rec["lon"],
                        "latest_time": obs["time_utc"], "capabilities": caps})
        self._latest_by_id = latest
        self._dir_ids = dirs
        return out

    def latest(self, local_id):
        if local_id not in self._latest_by_id:
            self.list_stations()
        return self._latest_by_id.get(local_id)

    def detail(self, local_id, fresh_s=None):
        local_id = str(local_id)
        if not self._ID_RE.match(local_id):
            return {"latest": None, "recent": []}
        hit = self._cache.get(local_id, fresh_s)
        if hit is not None:
            return hit
        latest = self.latest(local_id)
        if latest is None:                     # not a listed station: never a request to NDBC for it (G42 F14)
            return {"latest": None, "recent": []}
        recent = []
        try:
            r = _size_capped_get(self.http, self.REALTIME_URL % local_id, self.DETAIL_TIMEOUT, self.DETAIL_CAP,
                                 one_shot=True, max_s=DETAIL_MAX_S)
            obs = parse_ndbc_realtime_waves(r.text.splitlines())
            if obs:
                recent = _recent_window(obs)
                if (_z_epoch(obs[-1]) or 0) > (_z_epoch(latest) or 0):
                    latest = obs[-1]
        except (requests.RequestException, RuntimeError, ValueError) as e:
            _log.info("buoy provider %s: %s history unavailable (%s)", self.source, local_id, e)
        out = {"latest": latest, "recent": recent}
        if recent:
            self._cache.put(local_id, out)
        return out


def _call_quietly(fn, default, source):
    """fn() or default; a failure is logged and the default used (an optional helper list, e.g. the ids the NDBC
    provider lists, must never fail a provider's own list)."""
    if fn is None:
        return default
    try:
        return fn() or default
    except Exception as e:
        _log.warning("buoy provider %s: %s unavailable (%s)", source, getattr(fn, "__name__", fn), e)
        return default


def _kelvin_to_c(v):
    """GLOS stores temperatures in kelvin (288.6 = 15.45 C); values that already look like
    Celsius (below 150) are kept."""
    if v is None:
        return None
    return round(v - 273.15, 2) if v > 150 else v


def _num_or_none(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _json_array_items(text):
    """The objects of a JSON array, one at a time (json.loads of GLOS's 3.6 MB parameter list peaked at ~27 MB of
    Python objects; this holds the text and one item). Anything but an array of values raises ValueError."""
    dec = json.JSONDecoder()
    n = len(text)
    i = 0
    while i < n and text[i] in " \t\r\n":
        i += 1
    if i >= n or text[i] != "[":
        raise ValueError("not a JSON array")
    i += 1
    while True:
        while i < n and text[i] in " \t\r\n,":
            i += 1
        if i >= n:
            raise ValueError("unterminated JSON array")
        if text[i] == "]":
            return
        item, i = dec.raw_decode(text, i)
        yield item


PROBE_CAP = 1 << 20
PROBE_MAX_S = 30            # one probe's whole download
PROBE_BATCH_S = 150         # every probe of one list refresh (the refresh runner's worker)
_ERDDAP_NO_MATCH = "no matching results"


def _erddap_wave_newest(http, url, timeout, now=None):
    """One ERDDAP 'newest wave height' query (orderByMax time, NaN rows excluded) -> ("yes", ISO time) when the
    dataset has a wave height in the query's window, ("no", None) when ERDDAP answers that nothing matches (its
    404 "Your query produced no matching results"), ("unknown", None) on any other answer or failure: another 404
    ("Currently unknown datasetID" while ERDDAP reloads a dataset, G42 F6), an unreadable body (G42 F4), a body
    over PROBE_CAP or slower than PROBE_MAX_S. A row in the future or with an impossible height does not count; an
    answer holding only such rows is "unknown" (the sensor reports something: G42 re-check N3)."""
    try:
        r = _capped_fetch(http, url, timeout, PROBE_CAP, one_shot=True, max_s=PROBE_MAX_S)
    except (requests.RequestException, RuntimeError):
        return "unknown", None
    try:
        if r.status_code == 404:
            return ("no", None) if _ERDDAP_NO_MATCH in r.text.lower() else ("unknown", None)
        if r.status_code != 200:
            return "unknown", None
        rows = list(csv.reader(io.StringIO(r.text)))
        if not rows or "time" not in rows[0]:
            return "unknown", None
        ti = rows[0].index("time")
        hi = [i for i, c in enumerate(rows[0]) if c.endswith("significant_height")]
        limit = (time.time() if now is None else now) + FUTURE_SLACK_S
        times, refused = [], 0
        for row in rows[2:]:
            if len(row) <= ti or not row[ti].endswith("Z"):
                continue
            ep = _z_epoch({"time_utc": row[ti]})
            if ep is None or ep > limit:
                refused += ep is not None
                continue
            if hi and not (len(row) > hi[0] and _in_range(_num_or_none(row[hi[0]]), 0.0, HS_MAX_M)):
                if len(row) > hi[0] and _num_or_none(row[hi[0]]) is not None:
                    refused += 1           # a number out of range: not "no wave height" (G42 re-check N3)
                continue
            times.append(row[ti])
        if times:
            return "yes", max(times)
        return ("unknown", None) if refused else ("no", None)
    except Exception:                          # csv field limits, bad bytes ...: one probe never fails the list
        return "unknown", None


def _wave_probes(http, jobs, timeout, workers=4, now=None, deadline_s=None):
    """{key: url} -> {key: (status, newest)}, a few at a time (one short request each). Every answer that has not
    come back within `deadline_s` (PROBE_BATCH_S) counts as "unknown" and so does one that raised (G42 F4, F7):
    the refresh goes on with the verdicts it holds."""
    from concurrent.futures import ThreadPoolExecutor, wait
    if not jobs:
        return {}
    keys = list(jobs)
    ex = ThreadPoolExecutor(max_workers=min(workers, len(keys)))
    try:
        futs = {ex.submit(_erddap_wave_newest, http, jobs[k], timeout, now): k for k in keys}
        done, _left = wait(futs, timeout=PROBE_BATCH_S if deadline_s is None else deadline_s)
        out = {}
        for f, k in futs.items():
            res = ("unknown", None)
            if f in done:
                try:
                    res = f.result()
                except Exception:
                    pass
            out[k] = res
        return out
    finally:
        ex.shutdown(wait=False, cancel_futures=True)


class GlosProvider(BuoyProvider):
    """Great Lakes Observing System (GLOS) Seagull: the Great Lakes' buoys incl. many Sofar
    Spotters NDBC does not carry. The roster (GeoJSON) gives the station list; a window's 24
    hours come from one obs call. The API names an observation series only by a numeric
    parameter id: the parameter list (~3.6 MB) maps ids to CF standard names and is read once a
    day on the refresh runner. Seasonal: buoys come out of the water Nov-Apr ('recovered')."""
    source = "GLOS"
    source_name = "GLOS Seagull (Great Lakes)"
    source_url = "https://seagull.glos.org"
    license_label = "Free to use and redistribute (GLOS)"
    attribution_text = ("Source: Great Lakes Observing System (GLOS) Seagull and its partner buoys. "
                        "Free to use and redistribute; not intended for legal use.")
    # A GLOS marker's time is its newest wave height at the ERDDAP mirror, which runs about 30 h behind the API (and a
    # verdict is kept up to PROBE_TTL): a marker turns grey after 2 days without a wave height there (G42 F11).
    stale_after_sec = 48 * 3600
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 3600
    timeout = 60
    detail_fresh = True
    BASE = "https://seagull-api.glos.org/api/v1/"
    # GLOS's ERDDAP mirror lists each obs dataset's newest time (about a day behind the API):
    # good enough to leave out a platform the roster still calls 'activated' that has sent
    # nothing for days (45204 on 2026-10-30: newest 10-15), not to judge staleness.
    MIRROR_URL = ("https://seagull-erddap.glos.org/erddap/tabledap/allDatasets.csv?datasetID,maxTime"
                  "&datasetID=~%22obs_.*%22")
    SILENT_AFTER_S = 4 * 86400
    # The mirror itself can stall (it runs about a day behind the API): when its newest dataset is older than
    # MIRROR_STALL_S nothing is judged by it (every activated platform listed, no probe asked) (G42 F5). A probe's
    # "no" is believed only for a dataset the mirror lists (its own row): a sensor off while the rest of the platform
    # reports, which in winter is most of them (G42 re-check N1). A mirror that cannot be read keeps the last
    # good judgement up to MIRROR_HOLD_S (held verdicts and marker times, no new probes) (G42 re-check N6).
    MIRROR_STALL_S = 2 * 86400
    MIRROR_HOLD_S = 6 * 3600
    EARLY_START_S = 36 * 3600            # the mirror runs ~30 h behind: older than this, the platform has gone quiet
    clock = staticmethod(time.time)          # tests pin it (the mirror's dates and the obs start date)
    ROSTER_CAP = 8 << 20
    PARAMS_CAP = 16 << 20
    OBS_CAP = 4 << 20
    PARAMS_TTL = 24 * 3600
    FILL_S = 1800
    DETAIL_TIMEOUT = (3.05, 15)
    DETAIL_TTL = 300
    HS = "sea_surface_wave_significant_height"
    # reading key -> CF standard names in order of preference
    FIELDS = (
        ("hs_m", (HS,)),
        ("tp_s", ("sea_surface_wave_period_at_variance_spectral_density_maximum",)),
        ("mean_period_s", ("sea_surface_wave_mean_period",)),
        ("dir_deg", ("sea_surface_wave_from_direction_at_variance_spectral_density_maximum",
                     "sea_surface_wave_from_direction")),
        ("dir_spread_deg", ("sea_surface_wave_directional_spread_at_variance_spectral_density_maximum",
                            "sea_surface_wave_directional_spread")),
        ("hmax_m", ("sea_surface_wave_maximum_height",)),
        ("sst_c", ("sea_surface_temperature", "sea_water_temperature")),
    )
    _ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,39}$")

    # A platform the roster calls 'activated' can report wind and temperatures for weeks with its wave sensor
    # off (West Lake Erie 45005 and ECCC's 451xx buoys on 2026-10-30): the mirror's newest wave height decides
    # (it lags the API by about a day, hence 3 days). Asked again after PROBE_TTL; a failed answer keeps the
    # last verdict; platforms the NDBC provider lists with spectra are not asked (NDBC's marker wins there).
    # ERDDAP leaves out rows in the future and heights outside 0-30 m, so the newest row it gives is a sane one
    # (G42 re-check N3).
    PROBE_URL = ("https://seagull-erddap.glos.org/erddap/tabledap/obs_%d.csv?time,sea_surface_wave_significant_height"
                 "&time%%3E=now-3days&time%%3C=now%%2B1hour"
                 "&sea_surface_wave_significant_height%%3E=0&sea_surface_wave_significant_height%%3C=30"
                 "&sea_surface_wave_significant_height!=NaN&orderByMax(%%22time%%22)")
    PROBE_TTL = 6 * 3600
    PROBE_TIMEOUT = (3.05, 20)

    def __init__(self, http=None, covered_ids=None):
        super().__init__(http)
        self._covered_ids = covered_ids      # callable -> ids the NDBC provider lists (spectral file)
        self._dataset_by_id = {}             # local id -> obs_dataset_id
        self._params = {}                    # parameter_id -> (standard_name, depth)
        self._params_ts = 0.0
        self._probe = {}                     # obs_dataset_id -> (asked epoch, "yes" / "no", newest at the mirror)
        self._mirror_held = None             # (silent ids, {id: maxTime epoch}, read epoch): the last good mirror read
        self._cache = _DetailCache(self.DETAIL_TTL)

    def _wave_verdicts(self, cands, listed=None, ask=True):
        """{local: ds} -> the locals to leave out (no wave height at the mirror within its window). `listed`: the
        datasets the mirror lists; a "no" for one it does not list is not kept (the mirror lacks it, not the sensor).
        `ask`: False = judge by the held verdicts only (the mirror could not be read this time)."""
        now = self.clock()
        if ask:
            jobs = {ds: self.PROBE_URL % ds for ds in cands.values()
                    if not (ds in self._probe and now - self._probe[ds][0] < self.PROBE_TTL)}
            for ds, (status, newest) in _wave_probes(self.http, jobs, self.PROBE_TIMEOUT, now=now).items():
                if status == "yes" or (status == "no" and (listed is None or ds in listed)):
                    self._probe[ds] = (now, status, newest)
        return {loc for loc, ds in cands.items() if self._probe.get(ds, (0, "yes"))[1] == "no"}

    def _refresh_params(self):
        if self._params and time.time() - self._params_ts < self.PARAMS_TTL:
            return
        try:
            r = _size_capped_get(self.http, self.BASE + "parameters", self.timeout, self.PARAMS_CAP)
            wanted = {n for _key, names in self.FIELDS for n in names}
            m = {}
            for p in _json_array_items(r.text):          # one item at a time; only the names the window uses
                if not isinstance(p, dict):
                    continue
                try:
                    pid = int(p["parameter_id"])
                except (KeyError, TypeError, ValueError):
                    continue
                name = p.get("standard_name") or p.get("parameter_name")
                if isinstance(name, str) and name in wanted:
                    m[pid] = (name, _num_or_none(p.get("depth")))
            if m:
                self._params = m
                self._params_ts = time.time()
        except Exception as e:                 # the kept map serves; the list itself still refreshes
            _log.warning("buoy provider %s: parameter list unavailable (%s)", self.source, e)

    def _mirror_state(self):
        """(silent obs_dataset_ids (newest data at the mirror older than SILENT_AFTER_S), the datasets the mirror
        lists, mode). mode "fresh": the mirror judges (probes asked); "held": it could not be read, the last good
        read (up to MIRROR_HOLD_S old) judges with the held verdicts and no new probe; "off": not judged at all (a
        stalled mirror, or nothing held) -- every activated platform listed."""
        now = self.clock()
        try:
            hdr, rows = _erddap_rows(self.http, self.MIRROR_URL, 30)
        except requests.RequestException:
            hdr, rows = [], []
        if not hdr or "maxTime" not in hdr:
            held = self._mirror_held
            if held and now - held[2] < self.MIRROR_HOLD_S:
                _log.warning("buoy provider %s: the ERDDAP mirror could not be read; judging by the read of %.1f h ago",
                             self.source, (now - held[2]) / 3600.0)
                return held[0], held[1], "held"
            return set(), {}, "off"
        ix = {c: i for i, c in enumerate(hdr)}
        out, listed, newest = set(), {}, None
        for row in rows:
            try:
                m = re.match(r"^obs_(\d+)$", row[ix["datasetID"]])
                ep = _z_epoch({"time_utc": row[ix["maxTime"]]})
            except (KeyError, IndexError):
                continue
            if not m or ep is None or ep > now + FUTURE_SLACK_S:
                continue
            listed[int(m.group(1))] = ep
            newest = ep if newest is None else max(newest, ep)
            if now - ep > self.SILENT_AFTER_S:
                out.add(int(m.group(1)))
        if newest is None or now - newest > self.MIRROR_STALL_S:
            if newest is not None:
                _log.warning("buoy provider %s: the ERDDAP mirror's newest data is %.0f h old; not judging by it",
                             self.source, (now - newest) / 3600.0)
            self._mirror_held = None
            return set(), {}, "off"
        self._mirror_held = (out, listed, now)
        return out, listed, "fresh"

    def _fetch_stations(self):
        r = _size_capped_get(self.http, self.BASE + "obs-datasets.geojson", self.timeout, self.ROSTER_CAP)
        doc = r.json()
        feats = doc.get("features") if isinstance(doc, dict) else None
        if not isinstance(feats, list):
            raise ValueError("GLOS roster without features")
        del doc, r                                # not kept through the ~50 s of probes (G42 F9)
        out, by_id = [], {}
        silent, listed, mode = self._mirror_state()
        covered = {str(x).upper() for x in _call_quietly(self._covered_ids, set(), self.source)}
        for f in feats:
            try:
                p = f["properties"]
                if p.get("platform_event") != "activated":
                    continue
                names = {q.get("standard_name") for q in p.get("parameters") or [] if isinstance(q, dict)}
                if self.HS not in names:
                    continue
                g = f.get("geometry") or {}
                if g.get("type") != "Point":
                    continue
                lon, lat = float(g["coordinates"][0]), float(g["coordinates"][1])
                ds = int(p["obs_dataset_id"])
            except (KeyError, TypeError, ValueError, IndexError):
                continue
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):        # G42 F13
                continue
            if ds in silent:
                continue
            org = str(p.get("org_platform_id") or "").strip()
            local = org if self._ID_RE.match(org) else str(ds)
            if local in by_id:
                continue
            by_id[local] = ds
            caps = dict(self.capabilities)
            caps["directional"] = bool(names & {"sea_surface_wave_from_direction",
                                                "sea_surface_wave_from_direction_at_variance_spectral_density_maximum"})
            out.append({"local_id": local, "name": str(p.get("platform_name") or local), "lat": lat, "lon": lon,
                        "capabilities": caps})
        del feats
        judged = mode in ("fresh", "held")
        quiet = (self._wave_verdicts({s["local_id"]: by_id[s["local_id"]] for s in out if s["local_id"] not in covered},
                                     listed, ask=(mode == "fresh"))
                 if judged else set())
        out = [s for s in out if s["local_id"] not in quiet]
        if judged:                          # the marker's time: its newest wave height at the mirror (G42 F11)
            for s in out:
                rec = self._probe.get(by_id[s["local_id"]])
                if rec and len(rec) > 2 and rec[1] == "yes" and rec[2]:
                    s["latest_time"] = rec[2]
        # the windows name a series only through the parameter map: a list without it would open empty windows,
        # and a window must never fetch the 3.6 MB list on a visitor's thread (G42 F2)
        self._refresh_params()
        if not self._params:
            raise RuntimeError("GLOS parameter list unavailable")
        if not out and self._list_cache:      # keep-last keeps the markers: keep their windows with them (G42 F1);
            # once the keep window ends the base publishes the empty list and the next refresh accepts it (N5)
            raise ValueError("GLOS listed nothing after %d stations" % len(self._list_cache))
        self._dataset_by_id = {k: v for k, v in by_id.items() if k not in quiet}
        return out

    def _obs_from(self, answer):
        """The obs answer -> chronological readings: each parameter's series is named through the
        parameter map; the shallowest depth wins for temperatures."""
        series = {}                                 # standard name -> (depth, {timestamp: value})
        blocks = answer if isinstance(answer, list) else []
        for block in blocks:
            for par in (block.get("parameters") or []) if isinstance(block, dict) else []:
                try:
                    name, depth = self._params[int(par["parameter_id"])]
                except (KeyError, TypeError, ValueError):
                    continue
                vals = {}
                for o in par.get("observations") or []:
                    try:
                        t = _glos_naive_utc(str(o["timestamp"]))
                        v = _num_or_none(o["value"])
                    except (KeyError, TypeError):
                        continue
                    if v is not None and t is not None:
                        vals[t] = v
                d = depth if depth is not None else 0.0
                if name not in series or d < series[name][0]:
                    series[name] = (d, vals)
        hs = series.get(self.HS)
        if not hs:
            return []
        epochs = {}                                  # standard name -> (sorted epochs, values)
        for name, (_d, vals) in series.items():
            pairs = sorted((e, v) for e, v in ((_z_epoch({"time_utc": _z(k)}), v) for k, v in vals.items())
                           if e is not None)
            epochs[name] = ([e for e, _v in pairs], [v for _e, v in pairs])

        def at(name, ep):
            """The series' value at ep, else its newest value up to FILL_S earlier (GLOS stamps a
            buoy's height and its period minutes apart), else None."""
            if name not in epochs:
                return None
            es, vs = epochs[name]
            i = bisect.bisect_right(es, ep) - 1
            if i >= 0 and ep - es[i] <= self.FILL_S:
                return vs[i]
            return None
        obs = []
        now = self.clock()
        for t in sorted(hs[1]):
            o = {"time_utc": _z(t), "dir_kind": "from"}
            ep = _z_epoch(o)
            if ep is None:
                continue
            for key, names in self.FIELDS:
                o[key] = None
                for n in names:
                    v = at(n, ep)
                    if v is not None:
                        o[key] = v
                        break
            o["sst_c"] = _kelvin_to_c(o["sst_c"])
            o = _sane_reading(o, now)
            if o is not None:
                obs.append(o)
        return obs

    def detail(self, local_id, fresh_s=None):
        local_id = str(local_id)
        if local_id not in self._dataset_by_id:
            self.list_stations()
        ds = self._dataset_by_id.get(local_id)
        if ds is None:
            return {"latest": None, "recent": []}
        hit = self._cache.get(local_id, fresh_s)
        if hit is not None:
            return hit
        # yesterday (UTC); a platform whose newest wave height at the mirror is older starts a day before that,
        # so its window shows its last readings rather than nothing (G42 F11)
        start_ep = self.clock() - 86400
        rec = self._probe.get(ds)
        newest = _z_epoch({"time_utc": rec[2]}) if rec and len(rec) > 2 and rec[2] else None
        if newest is not None and self.clock() - newest > self.EARLY_START_S:
            start_ep = min(start_ep, newest - 86400)
        start = datetime.fromtimestamp(start_ep, timezone.utc).strftime("%Y-%m-%d")
        url = self.BASE + "obs?obsDatasetId=%d&startDate=%s" % (ds, start)
        try:
            r = _size_capped_get(self.http, url, self.DETAIL_TIMEOUT, self.OBS_CAP, one_shot=True,
                                 max_s=DETAIL_MAX_S)
            obs = self._obs_from(r.json())
        except (requests.RequestException, RuntimeError, ValueError) as e:
            _log.info("buoy provider %s: %s obs unavailable (%s)", self.source, local_id, e)
            return {"latest": None, "recent": []}
        out = {"latest": obs[-1] if obs else None, "recent": _recent_window(obs) if obs else []}
        if obs:
            self._cache.put(local_id, out)
        return out

    def latest(self, local_id):
        return self.detail(local_id)["latest"]


def _glos_naive_utc(t):
    """A GLOS timestamp ('2026-10-30T05:20:00', '...Z', '...-04:00', with or without fractions of a second) ->
    the naive UTC 'YYYY-MM-DDTHH:MM:SS' the reader keys on; an offset is applied, not cut off (G42 F3)."""
    try:
        d = datetime.fromisoformat(t.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if d.tzinfo is not None:
        d = d.astimezone(timezone.utc).replace(tzinfo=None)
    return d.strftime("%Y-%m-%dT%H:%M:%S")


def _erddap_index_variables(summary):
    """An ERDDAP index row's Summary text -> the dataset's variable names (the lines after
    'VARIABLES:', each 'name (long name, units)'). ERDDAP writes the line breaks as a literal
    backslash + n in the CSV."""
    text = summary.replace(chr(92) + "n", "\n")
    out, on = [], False
    for line in text.split("\n"):
        line = line.strip()
        if line.startswith("VARIABLES:"):
            on = True
            continue
        if on:
            m = re.match(r"([A-Za-z_][A-Za-z0-9_]*) \(", line)
            if m:
                out.append(m.group(1))
    return out


_IOOS_ID_PATTERNS = (
    re.compile(r"^\((\w{5})\s*/"),                 # "(41110 / ILM2WAVE / ILM2W) Wrightsville Beach ..."
    re.compile(r"\((\d{5}|[A-Z]{4}\d)\)\s*$"),     # "181 - Rincon, PUERTO RICO (41115)", "... (SIPF1)"
    re.compile(r"^(\d{5}|[A-Z]{4}\d)\b"),          # "46076 - Cape Cleare AK", "44008 (LLNR 827) - ..."
)


def _ioos_local_id(dataset_id, title):
    """The buoy's own number when the dataset names it (NDBC / WMO ids join other networks'
    markers and the forecast-point links), else the dataset id."""
    m = re.match(r"^(?:ism-[a-z]+-)?(?:gov-ndbc-|wmo_)(\w+)$", dataset_id)
    if m:
        return m.group(1).upper()
    t = re.sub(r"^\[[^\]]*\]\s*", "", title or "")
    for pat in _IOOS_ID_PATTERNS:
        m = pat.search(t)
        if m:
            return m.group(1).upper()
    return re.sub(r"^ism-[a-z]+-", "", dataset_id)     # a regional association's copy = the network's own


def _ioos_name(title):
    """'(41110 / ILM2WAVE / ILM2W) Wrightsville Beach ..., NC' -> 'Wrightsville Beach ..., NC'."""
    t = re.sub(r"^\[[^\]]*\]\s*", "", title or "")                 # "[SFC Buoy] 44063 - Annapolis, MD"
    t = re.sub(r"^\([^)]*\)\s*(?:-\s*)?", "", t).strip()
    return t or (title or "").strip()


class IoosSensorsProvider(BuoyProvider):
    """The IOOS sensors ERDDAP (run by Axiom for U.S. IOOS): every regional association's wave
    datasets in one place. Two calls a refresh: the wave-height category index (each dataset's
    variable list is in its summary) and allDatasets (positions and the newest time). A window's
    24 hours are one tabledap CSV naming only the variables that dataset has (ERDDAP refuses a
    query naming one it does not have). Datasets of the same buoy (a network's copy and the
    regional association's) are one marker: the newest wins."""
    source = "IOOS"
    source_name = "U.S. IOOS regional observing systems"
    source_url = "https://sensors.ioos.us"
    # every dataset carries its operator's own licence text (G42 F12): the window names the operator
    license_label = "Open data (U.S. IOOS; each operator's terms)"
    attribution_text = ("Source: U.S. IOOS regional ocean observing systems and NOAA, via the IOOS sensor "
                        "map (Axiom Data Science). Data under each operator's terms; not intended for legal use.")
    stale_after_sec = 6 * 3600
    capabilities = _caps(bulk=True, recent_history=True)
    # Every hour: allDatasets (~1.8 MB) and the wave-height probes, so a marker's time (its newest wave height) and its
    # grey stale look are at most an hour behind; the category index (~2 MB, each dataset's variables) is kept
    # INDEX_TTL and used up to INDEX_KEEP when it cannot be read (G42 F11).
    list_ttl_sec = 3600
    INDEX_TTL = 6 * 3600
    INDEX_KEEP = 24 * 3600
    timeout = 90
    detail_fresh = True
    BASE = "https://erddap.sensors.ioos.us/erddap/"
    INDEX_URL = BASE + "categorize/standard_name/sea_surface_wave_significant_height/index.csv?page=1&itemsPerPage=5000"
    LIVE_URL = (BASE + "tabledap/allDatasets.csv?datasetID,institution,minLongitude,maxLongitude,minLatitude,"
                "maxLatitude,maxTime,title&maxTime%3E=now-2days")
    # A moored buoy's reported positions stay within a few km; a dataset whose positions span more
    # than this is a glider / drifter / Saildrone (or a buoy re-deployed elsewhere): left out.
    MAX_SPAN_DEG = 0.2
    LIST_CAP = 16 << 20
    DETAIL_CAP = 4 << 20
    DETAIL_TIMEOUT = (3.05, 15)
    DETAIL_TTL = 300
    HS = "sea_surface_wave_significant_height"
    # reading key -> variable names in order of preference
    FIELDS = (
        ("tp_s", ("sea_surface_wave_period_at_variance_spectral_density_maximum",)),
        ("mean_period_s", ("sea_surface_wave_mean_period",)),
        ("dir_deg", ("sea_surface_wave_from_direction_at_variance_spectral_density_maximum",
                     "sea_surface_wave_from_direction")),
        ("hmax_m", ("sea_surface_wave_maximum_height",)),
        ("sst_c", ("sea_surface_temperature", "sea_water_temperature")),
    )
    PARTS = (("Swell", "sea_surface_swell_wave_significant_height", "sea_surface_swell_wave_period",
              "sea_surface_swell_wave_from_direction"),
             ("Wind sea", "sea_surface_wind_wave_significant_height", "sea_surface_wind_wave_period",
              "sea_surface_wind_wave_from_direction"))
    _DS_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")

    # allDatasets' maxTime is the dataset's STATED end, which can lag its rows by a day (gov-ndbc-46004 on
    # 2026-10-30: stated 10-29 00:00, rows to 10-30 04:00), and it counts every variable: a buoy whose wave
    # sensor is out (44009, 62305) still looks fresh. One "newest wave height" query per station decides
    # (4 days back: never past the stated end, which allDatasets keeps within 2 days); its time is the marker's.
    # A failed answer keeps the last verdict; stations the NDBC provider lists with spectra are not asked.
    PROBE_SUFFIX = (".csv?time,sea_surface_wave_significant_height&time%3E=now-4days&time%3C=now%2B1hour"
                    "&sea_surface_wave_significant_height%3E=0&sea_surface_wave_significant_height%3C=30"
                    "&sea_surface_wave_significant_height!=NaN&orderByMax(%22time%22)")
    PROBE_TIMEOUT = (3.05, 20)
    clock = staticmethod(time.time)

    def __init__(self, http=None, covered_ids=None):
        super().__init__(http)
        self._covered_ids = covered_ids        # callable -> ids the NDBC provider lists (spectral file)
        self._ds = {}                          # local id -> {"dataset", "vars", "institution", "maxtime"}
        self._probe = {}                       # dataset id -> ("yes" / "no", newest)
        self._meta = {}                        # dataset id -> (title, variables), from the category index
        self._meta_ts = 0.0
        self._cache = _DetailCache(self.DETAIL_TTL)

    def _index_meta(self):
        """The category index's wave datasets: read again after INDEX_TTL; a failed read keeps the held index up to
        INDEX_KEEP (the hourly refresh then goes on with it), else raises (keep-last)."""
        now = self.clock()
        if self._meta and now - self._meta_ts < self.INDEX_TTL:
            return self._meta
        try:
            meta = self._read_index()
        except Exception as e:
            if self._meta and now - self._meta_ts < self.INDEX_KEEP:
                _log.warning("buoy provider %s: index unavailable (%s), using the one from %.1f h ago",
                             self.source, e, (now - self._meta_ts) / 3600.0)
                return self._meta
            raise
        self._meta, self._meta_ts = meta, now
        return meta

    def _read_index(self):
        r = _size_capped_get(self.http, self.INDEX_URL, self.timeout, self.LIST_CAP)
        rows = csv.reader(io.StringIO(r.text))                  # streamed: a list of every row peaked at ~24 MB
        head = next(rows, None)
        if not head or "Dataset ID" not in head:
            raise ValueError("IOOS index without a Dataset ID column")
        ix = {c: i for i, c in enumerate(head)}
        meta = {}
        for row in rows:
            try:
                dsid = row[ix["Dataset ID"]]
                variables = _erddap_index_variables(row[ix["Summary"]])
            except (KeyError, IndexError):
                continue
            if self._DS_RE.match(dsid) and self.HS in variables and "time" in variables:
                meta[dsid] = (row[ix["Title"]] if "Title" in ix else "", variables)
        if not meta:
            raise ValueError("IOOS index listed no wave datasets")
        return meta

    def _fetch_stations(self):
        meta = self._index_meta()
        r = _size_capped_get(self.http, self.LIVE_URL, self.timeout, self.LIST_CAP)
        live = csv.reader(io.StringIO(r.text))          # streamed: a list of every dataset's row peaked at ~18 MB
        hdr = next(live, None)
        next(live, None)                                 # the units row
        if not hdr or "datasetID" not in hdr:
            raise ValueError("IOOS allDatasets answered nothing")
        li = {c: i for i, c in enumerate(hdr)}
        best = {}                               # local id -> (maxTime, row data)
        for row in live:
            try:
                dsid = row[li["datasetID"]]
                if dsid not in meta:
                    continue
                la0, la1 = float(row[li["minLatitude"]]), float(row[li["maxLatitude"]])
                lo0, lo1 = float(row[li["minLongitude"]]), float(row[li["maxLongitude"]])
                maxt = row[li["maxTime"]]
            except (KeyError, IndexError, ValueError):
                continue
            if not all(math.isfinite(x) for x in (la0, la1, lo0, lo1)):
                continue
            if la1 - la0 > self.MAX_SPAN_DEG or lo1 - lo0 > self.MAX_SPAN_DEG:
                continue
            lat, lon = (la0 + la1) / 2.0, (lo0 + lo1) / 2.0
            if not (-90 <= lat <= 90 and -180 <= lon <= 180) or not maxt.endswith("Z"):
                continue
            title, variables = meta[dsid]
            title = row[li["title"]] or title
            local = _ioos_local_id(dsid, title)
            if local in best and best[local][0] >= maxt:
                continue
            best[local] = (maxt, {"dataset": dsid, "vars": variables, "title": title,
                                  "institution": row[li["institution"]], "lat": lat, "lon": lon, "maxtime": maxt})
        del live, r
        covered = {str(x).upper() for x in _call_quietly(self._covered_ids, set(), self.source)}
        jobs = {d["dataset"]: self.BASE + "tabledap/" + d["dataset"] + self.PROBE_SUFFIX
                for local, (_m, d) in best.items() if local not in covered}
        for dsid, (status, newest) in _wave_probes(self.http, jobs, self.PROBE_TIMEOUT, now=self.clock()).items():
            if status in ("yes", "no"):
                self._probe[dsid] = (status, newest)
        out, ds = [], {}
        for local, (maxt, d) in best.items():
            verdict = self._probe.get(d["dataset"]) if local not in covered else None
            if verdict and verdict[0] == "no":
                continue
            ds[local] = d
            caps = dict(self.capabilities)
            caps["directional"] = any(v in d["vars"] for v in self.FIELDS[2][1])
            when = verdict[1] if verdict and verdict[1] else maxt
            out.append({"local_id": local, "name": _ioos_name(d["title"]) or local, "lat": d["lat"],
                        "lon": d["lon"], "latest_time": when, "capabilities": caps})
        if not out and self._list_cache:        # keep-last keeps the markers: keep their windows with them (G42 F1)
            raise ValueError("IOOS listed nothing after %d stations" % len(self._list_cache))
        self._ds = ds
        return out

    def _window_start(self, d):
        """The window query's start: 24 hours back, but never after the dataset's stated end (ERDDAP refuses a time
        constraint past it although newer rows exist); _recent_window keeps the newest 24 hours of the answer."""
        start = self.clock() - 24 * 3600
        end = _z_epoch({"time_utc": d.get("maxtime") or ""})
        if end is not None:
            start = min(start, end - 3600)
        return datetime.fromtimestamp(start, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _columns(self, variables):
        cols = [self.HS]
        if self.HS + "_qc_agg" in variables:
            cols.append(self.HS + "_qc_agg")
        for _key, names in self.FIELDS:
            for n in names:
                if n in variables:
                    cols.append(n)
                    break
        for _label, h, t, d in self.PARTS:
            if h in variables:
                cols.append(h)
                cols.extend(x for x in (t, d) if x in variables)
        return cols

    def detail(self, local_id, fresh_s=None):
        local_id = str(local_id)
        if local_id not in self._ds:
            self.list_stations()
        d = self._ds.get(local_id)
        if d is None:
            return {"latest": None, "recent": []}
        hit = self._cache.get(local_id, fresh_s)
        if hit is not None:
            return hit
        cols = self._columns(d["vars"])
        url = (self.BASE + "tabledap/" + d["dataset"] + ".csv?time," + ",".join(cols) +
               "&time%3E=" + self._window_start(d) + "&orderBy(%22time%22)")
        try:
            r = _size_capped_get(self.http, url, self.DETAIL_TIMEOUT, self.DETAIL_CAP, one_shot=True,
                                 max_s=DETAIL_MAX_S)
            rows = list(csv.reader(io.StringIO(r.text)))
        except (requests.RequestException, RuntimeError, csv.Error) as e:
            _log.info("buoy provider %s: %s data unavailable (%s)", self.source, local_id, e)
            return {"latest": None, "recent": [], "attribution_text": self._attribution(d)}
        obs = []
        if len(rows) >= 3:
            hi = {c: i for i, c in enumerate(rows[0])}
            for row in rows[2:]:
                o = self._obs(row, hi)
                if o is not None:
                    obs.append(o)
        out = {"latest": obs[-1] if obs else None, "recent": _recent_window(obs) if obs else [],
               "attribution_text": self._attribution(d)}
        if obs:
            self._cache.put(local_id, out)
        return out

    def _obs(self, row, hi):
        def num(c):
            if c not in hi or hi[c] >= len(row):
                return None
            return _num_or_none(row[hi[c]])
        hs = num(self.HS)
        qc = num(self.HS + "_qc_agg")
        if hs is None or qc == 4:                 # QARTOD aggregate 4 = failed
            return None
        try:
            t = row[hi["time"]]
        except (KeyError, IndexError):
            return None
        o = {"time_utc": t if t.endswith("Z") else None, "hs_m": hs, "dir_kind": "from"}
        if _z_epoch(o) is None:
            return None
        for key, names in self.FIELDS:
            o[key] = None
            for n in names:
                if n in hi:
                    o[key] = num(n)
                    break
        parts = []
        for label, h, t_, d_ in self.PARTS:
            ph = num(h)
            if ph is not None:
                parts.append({"label": label, "hs_m": ph, "period_s": num(t_), "dir_deg": num(d_)})
        if parts:
            o["partitions"] = parts
        return _sane_reading(o, self.clock())

    @staticmethod
    def _attribution(d):
        inst = (d.get("institution") or "").strip()
        base = "Source: %s, via the U.S. IOOS sensor map (Axiom Data Science). " % inst if inst else \
            "Source: U.S. IOOS regional ocean observing systems via the IOOS sensor map (Axiom Data Science). "
        return base + "Data under the operator's terms; not intended for legal use."

    def latest(self, local_id):
        return self.detail(local_id)["latest"]


# ---------------------------------------------------------------------------------------------------------------------
# Plan section 69: the buoy feeds that need a key (CCO_API_KEY, CWA_API_KEY in the environment). A key is never put in
# a message, a log line or an answer: CCO's goes in a request header; CWA's must go in the URL's query, so every error
# text passes through _redact_key first.

_KEY_QUERY = re.compile(r"(Authorization=)[^&\s'\"]+", re.I)


def _redact_key(text, key):
    s = str(text or "")
    if key:
        s = s.replace(key, "[key]")
    return _KEY_QUERY.sub(r"\1[key]", s)


def _num(v, lo=None, hi=None):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(x) or (lo is not None and x < lo) or (hi is not None and x > hi):
        return None
    return x


def _cco_time(v):
    """CCO's time ("YYYYMMDD#HHMMSS", or ISO with or without a zone) as YYYY-MM-DDTHH:MM:SSZ (UTC when no zone)."""
    s = str(v or "").strip()
    if re.search(r"(Z|[+-]\d\d:?\d\d)$", s) and "T" in s:
        try:
            d = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            pass
    m = re.search(r"(\d{4})-?(\d\d)-?(\d\d)\D?(\d\d):?(\d\d):?(\d\d)", s)
    if not m:
        return None
    try:
        d = datetime(*(int(x) for x in m.groups()), tzinfo=timezone.utc)
    except ValueError:
        return None
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


CCO_BAD = {"hs_m": (1, 9), "tp_s": (1, 2, 9), "mean_period_s": (1, 2, 9), "dir_deg": (1, 2, 3, 6, 9),
           "dir_spread_deg": (1, 2, 3, 6, 9), "sst_c": (8, 9)}


def parse_cco_features(doc):
    """[(sensor id, name, lat, lon, obs)] of a CCO GeoJSON answer; obs carries the bulk wave parameters (SI, direction
    FROM) with the fields the quality flag marks bad left out."""
    out = []
    for f in (doc or {}).get("features") or ():
        if not isinstance(f, dict):
            continue
        p = f.get("properties") or {}
        try:
            lon, lat = (f.get("geometry") or {})["coordinates"][:2]
            lat, lon = float(lat), float(lon)
        except (KeyError, TypeError, ValueError, IndexError):
            continue
        name = " ".join(str(p.get("sensor") or p.get("name") or "").split())
        sid = next((str(p[k]) for k in ("id", "sensor_id", "sensorId", "sensorid") if p.get(k) not in (None, "")), None)
        sid = re.sub(r"[^A-Za-z0-9_-]", "_", sid or name).strip("_")[:40]
        if not sid or not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue
        try:
            qc = int(float(p.get("QCflag")))
        except (TypeError, ValueError):
            qc = None
        obs = {"time_utc": _cco_time(p.get("date") or p.get("time")),
               "hs_m": _num(p.get("hs"), 0, 30), "hmax_m": _num(p.get("hmax"), 0, 60), "tp_s": _num(p.get("tp"), 0, 40),
               "mean_period_s": _num(p.get("tz"), 0, 40), "dir_deg": _num(p.get("pdir"), 0, 360),
               "dir_spread_deg": _num(p.get("spread"), 0, 180), "sst_c": _num(p.get("sst"), -5, 40), "dir_kind": "from"}
        if qc is not None:
            for k, bad in CCO_BAD.items():
                if qc in bad:
                    obs[k] = None
            if obs["hs_m"] is None:
                obs["hmax_m"] = None
        out.append((sid, name or sid, lat, lon, obs))
    return out


class CcoProvider(BuoyProvider):
    """UK Channel Coastal Observatory (Regional Coastal Monitoring Programme): every wave sensor's latest reading in
    one answer, a sensor's 24 hours on demand. The key goes in X-API-Key; it is bound to allshoresurf.com, so every
    request names the site in Referer."""
    source = "CCO"
    source_name = "Channel Coastal Observatory (UK)"
    source_url = "https://coastalmonitoring.org/realtimedata/"
    license_label = "Open Government Licence"
    attribution_text = ("Real time data displayed on this page are from the Regional Coastal Monitoring Programme, made "
                        "freely available under the terms of the Open Government Licence. Please note that these are "
                        "real-time data and are not quality-controlled.")
    stale_after_sec = 6 * 3600
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 1800
    timeout = 40
    BASE = "https://coastalmonitoring.org/observations/waves/latest.geojson"
    MAX = 4 * 1024 * 1024
    DETAIL_TTL = 600

    def __init__(self, key, http=None):
        super().__init__(http)
        self._key = key
        self._latest_by_id = {}
        self._names = {}
        self._detail = {}

    def _headers(self):
        return {"X-API-Key": self._key, "Referer": "https://allshoresurf.com/", "Accept": "application/geo+json",
                "User-Agent": "allshoresurf.com live buoys (https://allshoresurf.com)"}

    def _get(self, url):
        try:
            r = self.http.get(url, headers=self._headers(), timeout=self.timeout)
            if r.status_code != 200:
                raise RuntimeError("HTTP %s from CCO" % r.status_code)
            if len(r.content) > self.MAX:
                raise RuntimeError("the CCO answer is too large")
            return r.json()
        except Exception as exc:                              # noqa: BLE001
            raise RuntimeError(_redact_key("%s: %s" % (type(exc).__name__, exc), self._key)) from None

    def _fetch_stations(self):
        latest, meta = {}, {}
        for sid, name, lat, lon, obs in parse_cco_features(self._get(self.BASE)):
            if obs.get("hs_m") is None or not obs.get("time_utc"):
                continue
            old = latest.get(sid)
            if old is not None and old["time_utc"] >= obs["time_utc"]:
                continue
            latest[sid] = obs
            meta[sid] = (name, lat, lon)
        if not latest:
            raise RuntimeError("the CCO answer holds no wave reading")
        self._latest_by_id = latest
        self._names.update({sid: m[0] for sid, m in meta.items()})
        return [{"local_id": sid, "name": meta[sid][0], "lat": meta[sid][1], "lon": meta[sid][2],
                 "latest_time": latest[sid]["time_utc"]} for sid in sorted(latest)]

    detail_fresh = True                                       # the window's refresh button (plan section 56)

    def detail(self, local_id, fresh_s=None):
        if local_id not in self._latest_by_id:
            self.list_stations()
        if local_id not in self._latest_by_id:
            return {"latest": None, "recent": []}
        now = time.time()
        hit = self._detail.get(local_id)
        if hit and now - hit[0] < (self.DETAIL_TTL if fresh_s is None else min(self.DETAIL_TTL, fresh_s)):
            return hit[1]
        sensor = self._names.get(local_id) or local_id
        try:
            doc = self._get(self.BASE + "?duration=24&sensor=" + requests.utils.quote(sensor))
            obs = sorted((o for (s, _n, _la, _lo, o) in parse_cco_features(doc)
                          if s == local_id and o.get("time_utc") and o.get("hs_m") is not None),
                         key=lambda o: o["time_utc"])
        except RuntimeError:
            obs = []
        latest = self._latest_by_id.get(local_id)
        if obs and (latest is None or obs[-1]["time_utc"] >= latest["time_utc"]):
            latest = obs[-1]
        out = {"latest": latest, "recent": _recent_window(obs) if obs else []}
        if obs:                                               # a failed window is asked again next time
            self._detail[local_id] = (now, out)
        return out

    def latest(self, local_id):
        if local_id not in self._latest_by_id:
            self.list_stations()
        return self._latest_by_id.get(local_id)


def _cwa_val(v, lo=None, hi=None):
    if v is None or str(v).strip() in ("None", "-", "", "-99", "-999"):
        return None
    return _num(v, lo, hi)


def _cwa_locations(doc):
    """The SeaSurfaceObs Location list wherever the answer holds it (REST: records; the file: cwaopendata)."""
    todo, n = [doc], 0
    while todo and n < 20000:
        o = todo.pop(0)
        n += 1
        if isinstance(o, dict):
            sso = o.get("SeaSurfaceObs")
            if isinstance(sso, dict) and isinstance(sso.get("Location"), list):
                return sso["Location"]
            todo.extend(v for v in o.values() if isinstance(v, (dict, list)))
        elif isinstance(o, list):
            todo.extend(v for v in o if isinstance(v, (dict, list)))
    return []


def parse_cwa_buoy_stations(doc):
    """{StationID: {name, lat, lon}} of O-B0076-001: the stations whose observed properties include WaveHeight."""
    out = {}
    for loc in _cwa_locations(doc):
        if not isinstance(loc, dict):
            continue
        st = loc.get("Station") or {}
        sid = str(st.get("StationID") or "").strip()
        props = str((loc.get("StationObsStatus") or {}).get("ObservedPropertyNames") or "")
        if not re.fullmatch(r"[A-Za-z0-9]{2,12}", sid) or "WaveHeight" not in props:
            continue
        lat, lon = _num(st.get("StationLatitude"), -90, 90), _num(st.get("StationLongitude"), -180, 180)
        if lat is None or lon is None:
            continue
        name = " ".join(str(st.get("StationNameEN") or st.get("StationName") or sid).split())
        out[sid] = {"name": name, "lat": lat, "lon": lon}
    return out


def _cwa_z(s):
    try:
        d = datetime.fromisoformat(str(s).strip())
    except (TypeError, ValueError):
        return None
    if d.tzinfo is None:
        return None
    return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_cwa_buoy_obs(doc):
    """{StationID: [obs oldest first]} of O-B0075-001 (hourly, the last 48 hours). WavePeriod is a MEAN period (CWA's
    buoys report no peak period): mean_period_s, never tp_s. Directions FROM."""
    out = {}
    for loc in _cwa_locations(doc):
        if not isinstance(loc, dict):
            continue
        sid = str((loc.get("Station") or {}).get("StationID") or "").strip()
        times = (loc.get("StationObsTimes") or {}).get("StationObsTime") or []
        if isinstance(times, dict):
            times = [times]
        rows = []
        for t in times:
            if not isinstance(t, dict):
                continue
            we = t.get("WeatherElements") or {}
            wind = we.get("PrimaryAnemometer") or {}
            obs = {"time_utc": _cwa_z(t.get("DataTime")), "hs_m": _cwa_val(we.get("WaveHeight"), 0, 30),
                   "mean_period_s": _cwa_val(we.get("WavePeriod"), 0, 40), "tp_s": None,
                   "dir_deg": _cwa_val(we.get("WaveDirection"), 0, 360), "sst_c": _cwa_val(we.get("SeaTemperature"), -5, 40),
                   "wind_speed_ms": _cwa_val(wind.get("WindSpeed"), 0, 120),
                   "wind_dir_deg": _cwa_val(wind.get("WindDirection"), 0, 360), "dir_kind": "from"}
            if obs["time_utc"] and obs["hs_m"] is not None:
                rows.append(obs)
        if sid and rows:
            out[sid] = sorted({o["time_utc"]: o for o in rows}.values(), key=lambda o: o["time_utc"])
    return out


class CwaBuoyProvider(BuoyProvider):
    """Taiwan Central Weather Administration buoys: the station list (O-B0076-001) once a day and every station's last
    48 hours (O-B0075-001) in one answer, serving the list, the latest reading and the windows. The key goes in the
    URL's query (CWA takes it nowhere else): every error text is redacted."""
    source = "CWA"
    source_name = "Central Weather Administration (Taiwan)"
    source_url = "https://opendata.cwa.gov.tw/"
    license_label = "Open Government Data License"
    attribution_text = ("Source: Central Weather Administration (Taiwan), marine observations O-B0075-001. The Open Data "
                        "is made available to the public under the Open Government Data License, "
                        "https://data.gov.tw/license")
    stale_after_sec = 6 * 3600
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 1800
    timeout = 40
    API = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/%s?format=JSON&Authorization=%s"
    MAX = 16 * 1024 * 1024
    STATIONS_TTL = 24 * 3600

    def __init__(self, key, http=None):
        super().__init__(http)
        self._key = key
        self._stations = {}
        self._stations_at = 0.0
        self._obs = {}

    def _get(self, dataset):
        try:
            r = self.http.get(self.API % (dataset, self._key), timeout=self.timeout,
                              headers={"User-Agent": "allshoresurf.com live buoys (https://allshoresurf.com)"})
            if r.status_code != 200:
                raise RuntimeError("HTTP %s from CWA %s" % (r.status_code, dataset))
            if len(r.content) > self.MAX:
                raise RuntimeError("the CWA answer is too large")
            return r.json()
        except Exception as exc:                              # noqa: BLE001
            raise RuntimeError(_redact_key("%s: %s" % (type(exc).__name__, exc), self._key)) from None

    def _fetch_stations(self):
        now = time.time()
        if not self._stations or now - self._stations_at > self.STATIONS_TTL:
            found = parse_cwa_buoy_stations(self._get("O-B0076-001"))
            if found:
                self._stations, self._stations_at = found, now
        if not self._stations:
            raise RuntimeError("no CWA station with a wave sensor")
        obs = parse_cwa_buoy_obs(self._get("O-B0075-001"))
        out = []
        for sid, st in sorted(self._stations.items()):
            rows = obs.get(sid)
            if rows:
                out.append({"local_id": sid, "name": st["name"], "lat": st["lat"], "lon": st["lon"],
                            "latest_time": rows[-1]["time_utc"]})
        if not out:
            raise RuntimeError("the CWA answer holds no wave reading")
        self._obs = obs
        return out

    def detail(self, local_id):
        if local_id not in self._obs:
            self.list_stations()
        rows = self._obs.get(local_id) or []
        return {"latest": rows[-1] if rows else None, "recent": _recent_window(rows) if rows else []}

    def latest(self, local_id):
        return self.detail(local_id)["latest"]
