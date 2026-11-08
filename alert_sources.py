"""NWS coastal hazard products for the map's "Coastal hazards" layer (plan section 90).

One request to the NWS API (api.weather.gov/alerts/active, public domain, the site's User-Agent) lists every active
coastal product: High Surf, Rip Current, Beach Hazards, Coastal / Lakeshore Flood, Storm Surge. Those products carry
no shape: they name forecast zones (UGC ids such as HIZ007). The coastline of each zone comes from alert_coast.json
(tools/alerts/build_alert_coast.py), so the page draws the affected shore only.

Classes (the page's colours; owner, 2026-11-08): "warning" red; "advisory" and "beach" (Rip Current / Beach Hazards
Statements) orange; "flood_statement" (Coastal / Lakeshore Flood Statements) blue; "watch" yellow dashed. Anything
issued but not yet in effect (onset in the future) is drawn yellow dashed too: the page judges that from `onset`.

AlertService keeps the newest good answer in memory: the live scheduler refreshes it in the background every
REFRESH_S; a failed refresh keeps the last answer up to STALE_S, after which nothing is served ("stale"). No request
of a visitor ever waits for NWS.
"""
import json
import logging
import threading
import time
import urllib.parse
from datetime import datetime

logger = logging.getLogger(__name__)

NWS_API = "https://api.weather.gov"
USER_AGENT = "allshoresurf.com coastal hazards (https://allshoresurf.com)"
HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/geo+json"}
EVENTS = {
    "High Surf Warning": "warning",
    "High Surf Advisory": "advisory",
    "Rip Current Statement": "beach",
    "Beach Hazards Statement": "beach",
    "Coastal Flood Warning": "warning",
    "Coastal Flood Advisory": "advisory",
    "Coastal Flood Watch": "watch",
    "Coastal Flood Statement": "flood_statement",
    "Lakeshore Flood Warning": "warning",
    "Lakeshore Flood Advisory": "advisory",
    "Lakeshore Flood Watch": "watch",
    "Lakeshore Flood Statement": "flood_statement",
    "Storm Surge Warning": "warning",
    "Storm Surge Watch": "watch",
}
RANK = {"warning": 4, "advisory": 3, "beach": 3, "flood_statement": 2, "watch": 1}
URL = (NWS_API + "/alerts/active?status=actual&event="
       + urllib.parse.quote(",".join(EVENTS), safe=","))
MAX_BYTES = 4 * 1024 * 1024
REFRESH_S = 120            # one request every two minutes
RETRY_S = 60               # after a failure
STALE_S = 30 * 60          # a held answer older than this is not served
TEXT_CAP = 6000            # characters of a description / instruction kept
FORMAT = "coastal-alerts-v1"


def _epoch(value):
    """ISO 8601 with an offset -> epoch seconds, or None."""
    if not isinstance(value, str) or not value:
        return None
    try:
        t = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if t.tzinfo is None:
        return None
    return int(t.timestamp())


def _text(value, cap=TEXT_CAP):
    if not isinstance(value, str):
        return ""
    s = value.replace("\r\n", "\n").replace("\r", "\n").strip()
    return s if len(s) <= cap else s[:cap].rstrip() + " ..."


HEADLINE_KEY = {"Rip Current Statement": "RIP CURRENT"}   # NWS writes "HIGH RIP CURRENT RISK ..." for this product


def _headline(event, props):
    """The NWS headline part that names this product. One NWSheadline often joins every hazard of the segment
    ("COASTAL FLOOD ADVISORY ... ...HIGH SURF ADVISORY ..."): the part naming the event is kept, else the whole line,
    else the API's own headline ("High Surf Advisory issued ... by NWS ...")."""
    params = props.get("parameters") or {}
    heads = params.get("NWSheadline")
    line = heads[0] if isinstance(heads, list) and heads and isinstance(heads[0], str) else ""
    parts = [x.strip(" .") for x in line.split("...")]
    parts = [x for x in parts if x]
    key = HEADLINE_KEY.get(event, event.upper())
    for x in parts:
        if key in x.upper():
            return x
    if parts:
        return " ... ".join(parts)
    head = props.get("headline")
    return head if isinstance(head, str) else ""


def _zone_ids(props):
    out = []
    for u in props.get("affectedZones") or []:
        if isinstance(u, str):
            z = u.rstrip("/").rsplit("/", 1)[-1]
            if z not in out:
                out.append(z)
    for z in ((props.get("geocode") or {}).get("UGC") or []):
        if isinstance(z, str) and z not in out:
            out.append(z)
    return out


def parse(doc, coast_zones, now):
    """The NWS answer (GeoJSON FeatureCollection) -> (alerts, stats). An alert is kept when its event is a coastal
    product, its status Actual, it is not a Cancel, it has not ended, nothing in the answer replaces it, and at least
    one of its zones has a coastline. ValueError for an answer that is not a feature collection."""
    if not isinstance(doc, dict) or not isinstance(doc.get("features"), list):
        raise ValueError("alerts: not a feature collection")
    stats = {"listed": len(doc["features"]), "kept": 0, "ended": 0, "replaced": 0, "cancel": 0, "other": 0,
             "no_coast": []}
    props_list = []
    replaced = set()
    for f in doc["features"]:
        p = (f or {}).get("properties") if isinstance(f, dict) else None
        if not isinstance(p, dict):
            stats["other"] += 1
            continue
        for ref in p.get("references") or []:
            if isinstance(ref, dict) and isinstance(ref.get("identifier"), str):
                replaced.add(ref["identifier"])
        props_list.append(p)
    alerts = []
    for p in props_list:
        cls = EVENTS.get(p.get("event"))
        if cls is None or p.get("status") != "Actual":
            stats["other"] += 1
            continue
        if p.get("messageType") == "Cancel":
            stats["cancel"] += 1
            continue
        if p.get("id") in replaced:
            stats["replaced"] += 1
            continue
        sent = _epoch(p.get("sent"))
        effective = _epoch(p.get("effective")) or sent
        onset = _epoch(p.get("onset")) or effective
        ends = _epoch(p.get("ends"))
        expires = _epoch(p.get("expires"))
        end = ends or expires
        if end is None or onset is None or end <= now:
            stats["ended"] += 1
            continue
        zones = _zone_ids(p)
        kept = [z for z in zones if z in coast_zones]
        if not kept:
            for z in zones:
                if z not in stats["no_coast"] and len(stats["no_coast"]) < 50:
                    stats["no_coast"].append(z)
            stats["other"] += 1
            continue
        headline = _headline(p["event"], p)
        alerts.append({
            "id": p.get("id") if isinstance(p.get("id"), str) else "",
            "event": p["event"], "cls": cls,
            "sent": sent, "onset": onset, "ends": ends, "expires": expires,
            "office": _text(p.get("senderName"), 120),
            "headline": _text(headline, 600),
            "description": _text(p.get("description")),
            "instruction": _text(p.get("instruction")),
            "zones": kept,
        })
    alerts.sort(key=lambda a: (-RANK[a["cls"]], a["onset"] or 0, a["event"], a["id"]))
    stats["kept"] = len(alerts)
    return alerts, stats


class AlertService:
    """The held coastal alerts and the zone coastlines they name. Thread-safe; refresh() runs in a background
    thread of the live scheduler (or a test calls it with its own fetch)."""

    def __init__(self, coast_path, clock=time.time):
        self.coast_path = coast_path
        self.clock = clock
        self.lock = threading.Lock()
        self.coast = None                 # {zone id: {"name", "tz", "lines"}}
        self.coast_error = None
        self.alerts = None                # the last good list (None: never read)
        self.stats = {}
        self.ts = 0.0                     # when it was read
        self.failed = 0.0
        self.error = None
        self.fetches = 0
        self.thread = None
        self._payload = None              # (key, payload str, etag)

    def after_fork(self):
        self.lock = threading.Lock()
        self.thread = None

    def load_coast(self):
        """The zone coastlines, read once (alert_coast.json, ~1.9 MB)."""
        if self.coast is not None:
            return self.coast
        try:
            with open(self.coast_path, encoding="utf-8") as f:
                doc = json.load(f)
            if doc.get("format") != "alert-coast-v1" or not isinstance(doc.get("zones"), dict):
                raise ValueError("alert_coast.json: format")
            self.coast = doc["zones"]
        except (OSError, ValueError) as exc:
            self.coast_error = str(exc)
            self.coast = {}
            logger.warning("coastal alerts: no zone coastlines: %s", exc)
        return self.coast

    def due(self, now=None):
        now = self.clock() if now is None else now
        with self.lock:
            if self.thread is not None and self.thread.is_alive():
                return False
            if now - self.failed < RETRY_S:
                return False
            return self.alerts is None or now - self.ts >= REFRESH_S

    def refresh(self, fetch):
        """One read: fetch() -> the answer's bytes. A failure keeps the last good list."""
        coast = self.load_coast()
        now = self.clock()
        try:
            doc = json.loads(fetch())
            alerts, stats = parse(doc, coast, now)
        except Exception as exc:
            with self.lock:
                self.failed = now
                self.error = "%s: %s" % (type(exc).__name__, str(exc)[:200])
                self.fetches += 1
            logger.warning("coastal alerts: read failed: %s", exc)
            return False
        with self.lock:
            self.alerts, self.stats, self.ts = alerts, stats, now
            self.error = None
            self.fetches += 1
            self._payload = None
        if stats["no_coast"]:
            logger.info("coastal alerts: zones without a coastline: %s", ", ".join(stats["no_coast"]))
        return True

    def start(self, fetch):
        """refresh() in a daemon thread when due; True when one was started."""
        if not self.due():
            return False
        t = threading.Thread(target=self.refresh, args=(fetch,), name="coastal-alerts", daemon=True)
        with self.lock:
            if self.thread is not None and self.thread.is_alive():
                return False
            self.thread = t
        t.start()
        return True

    def served(self, now=None):
        """(payload str, etag): the alerts in force or pending at `now` and the coastlines of their zones."""
        import hashlib
        now = self.clock() if now is None else now
        coast = self.load_coast()
        with self.lock:
            alerts, ts, error = self.alerts, self.ts, self.error
            if alerts is None:
                state, alerts = ("unavailable" if error else "loading"), []
            elif now - ts > STALE_S:
                state, alerts = "stale", []
            else:
                state = "ok"
            alerts = [a for a in alerts if (a["ends"] or a["expires"]) > now]
            key = (state, ts, tuple(a["id"] for a in alerts))
            if self._payload is not None and self._payload[0] == key:
                return self._payload[1], self._payload[2]
        zones = {}
        for a in alerts:
            for z in a["zones"]:
                if z in coast and z not in zones:
                    c = coast[z]
                    zones[z] = {"name": c.get("name", ""), "tz": c.get("tz"), "lines": c.get("lines", [])}
        body = {"format": FORMAT, "state": state, "updated": int(ts) if ts else None, "alerts": alerts,
                "zones": zones}
        payload = json.dumps(body, separators=(",", ":"), sort_keys=True)
        etag = hashlib.md5(payload.encode("utf-8")).hexdigest()
        with self.lock:
            self._payload = (key, payload, etag)
        return payload, etag

    def health(self, now=None):
        now = self.clock() if now is None else now
        with self.lock:
            return {"alerts": None if self.alerts is None else len(self.alerts),
                    "age_s": round(now - self.ts) if self.ts else None,
                    "fetches": self.fetches, "last_error": self.error,
                    "zones_without_coast": list(self.stats.get("no_coast", [])),
                    "coast_zones": None if self.coast is None else len(self.coast),
                    "coast_error": self.coast_error}
