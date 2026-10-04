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
import csv
import io
import logging
import math
import os
import re
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

    def schedule_refresh(self):
        """Queue one background refresh (no-op while one is queued or in flight)."""
        with self._lock:
            if self._refresh_pending or self._refreshing:
                return False
            self._refresh_pending = True
        try:
            get_refresh_runner().submit(self._scheduled_refresh, "buoy-refresh-%s" % self.source)
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

    def spectrum(self, local_id, count=25):
        """Last `count` directional spectra (chronological) for a spectra-capable site,
        via THREDDS OPeNDAP. Returns {freqs:[39], steps:[{time_utc, energy[39],
        alpha1[39], alpha2[39], r1[39], r2[39]}]} or None. alpha1 = mean wave direction
        per bin in the NDBC 'from' convention (degrees clockwise from true north), converted from the files'
        Fourier moments with NDBC's formulas (_ndbc_alpha1/_ndbc_alpha2). One batched request per variable."""
        code = self._spectra_code(local_id)
        if not code:
            return None
        ckey = (local_id, count)
        with self._lock:
            hit = self._spec_cache.get(ckey)
            if hit and (time.time() - hit[0]) < self.spectra_ttl_sec:
                return hit[1]
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
    if src == "CEFAS":                 # UK WaveNet aggregate (latest-only)
        return 58
    if src == "CDIP":
        return 55
    if src == "AODN":                  # bulk only
        return 50
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


def merge_stations(provider_lists, radius_km=1.0, name_radius_km=10.0):
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
    kept, keptkey = [], []
    for st in allst:
        nk = _name_key(_match_name(st))
        dup = None
        for idx, k in enumerate(kept):
            if k["source"] == st["source"]:
                continue
            dist = haversine_km(k["lat"], k["lon"], st["lat"], st["lon"])
            if dist <= radius_km or (len(nk) >= 2 and nk == keptkey[idx] and dist <= name_radius_km):
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
    return kept
