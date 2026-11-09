"""The coastal hazards layer's server side (alert_sources.py, app.py /api/alerts/coastal; plan section 90), on a real
NWS answer (tests/fixtures/alerts/coastal_active.json, cut by tests/capture_alert_fixtures.py) and synthetic edits."""
import copy
import json
import os
import threading
import time
from datetime import datetime, timezone

import pytest

import alert_sources as S

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
COAST = os.path.join(ROOT, "alert_coast.json")
NOW = int(datetime(2026, 11, 8, 23, 30, tzinfo=timezone.utc).timestamp())    # the fixture was read at ~23:2x UTC


def fixture():
    with open(os.path.join(HERE, "fixtures", "alerts", "coastal_active.json"), encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def coast():
    with open(COAST, encoding="utf-8") as f:
        return json.load(f)["zones"]


def feature(**props):
    base = {"id": "urn:test:1", "event": "High Surf Advisory", "status": "Actual", "messageType": "Alert",
            "sent": "2026-11-08T12:00:00-10:00", "effective": "2026-11-08T12:00:00-10:00",
            "onset": "2026-11-08T12:00:00-10:00", "ends": "2026-11-09T18:00:00-10:00",
            "expires": "2026-11-09T06:00:00-10:00", "senderName": "NWS Honolulu HI",
            "headline": "High Surf Advisory issued", "description": "* WHAT...Surf 15 to 20 feet.",
            "instruction": "Stay out of the water.", "references": [],
            "affectedZones": ["https://api.weather.gov/zones/forecast/HIZ007"],
            "parameters": {"NWSheadline": ["HIGH SURF ADVISORY IN EFFECT UNTIL 6 PM HST MONDAY"]}}
    base.update(props)
    return {"type": "Feature", "geometry": None, "properties": base}


def collection(*feats):
    return {"type": "FeatureCollection", "features": list(feats)}


# ---------------------------------------------------------------- parse
def test_the_real_answer(coast):
    alerts, stats = S.parse(fixture(), coast, NOW)
    assert stats["listed"] == 10 and stats["kept"] == 10 and stats["no_coast"] == []
    by_event = {}
    for a in alerts:
        by_event.setdefault(a["event"], []).append(a)
    assert {a["cls"] for a in by_event["High Surf Warning"]} == {"warning"}
    assert {a["cls"] for a in by_event["Rip Current Statement"]} == {"beach"}
    assert {a["cls"] for a in by_event["Beach Hazards Statement"]} == {"beach"}
    assert {a["cls"] for a in by_event["Coastal Flood Statement"]} == {"flood_statement"}
    assert [a["cls"] for a in alerts[:2]] == ["warning", "warning"]          # the strongest first
    ranks = [S.RANK[a["cls"]] for a in alerts]
    assert ranks == sorted(ranks, reverse=True)
    ore = [a for a in alerts if a["zones"] == ["ORZ103"]][0]
    assert ore["office"] == "NWS Portland OR" and ore["ends"] == int(datetime(2026, 11, 9, 6, tzinfo=timezone.utc).timestamp())
    assert ore["headline"] == "HIGH SURF WARNING IN EFFECT UNTIL 10 PM PST THIS EVENING"
    assert ore["description"].startswith("* WHAT...Large waves") and "Central Coast of Oregon" in ore["description"]
    heads = {(a["event"], tuple(a["zones"])): a["headline"] for a in alerts}
    assert heads[("High Surf Advisory", ("CAZ354",))] == "HIGH SURF ADVISORY REMAINS IN EFFECT UNTIL NOON PST TUESDAY"
    assert heads[("Coastal Flood Advisory", ("CAZ103",))] == "COASTAL FLOOD ADVISORY IN EFFECT FROM 10 AM TO NOON PST MONDAY"
    txz = ("TXZ451", "TXZ454", "TXZ455")
    assert heads[("Rip Current Statement", txz)] == "HIGH RIP CURRENT RISK REMAINS IN EFFECT THROUGH MONDAY AFTERNOON"
    assert heads[("Coastal Flood Statement", txz)] == "COASTAL FLOOD STATEMENT REMAINS IN EFFECT THROUGH LATE TONIGHT"
    pending = [a for a in alerts if a["zones"] == ["CAZ103"]][0]               # a Coastal Flood Advisory from 10 AM tomorrow
    assert pending["onset"] > NOW and pending["cls"] == "advisory"
    pr = [a for a in alerts if a["zones"][0] == "PRZ001"][0]
    assert pr["zones"] == ["PRZ001", "PRZ002", "PRZ005", "PRZ008", "PRZ010", "PRZ012"]


def test_cancel_test_unknown_ended_and_replaced(coast):
    doc = collection(
        feature(id="a", messageType="Cancel"),
        feature(id="b", status="Test"),
        feature(id="c", event="Wind Advisory"),
        feature(id="d", ends="2026-11-08T08:00:00-10:00", expires="2026-11-08T09:00:00-10:00"),   # over
        feature(id="e"),
        feature(id="f", messageType="Update", references=[{"identifier": "e", "sender": "w-nws.webmaster@noaa.gov"}]),
    )
    alerts, stats = S.parse(doc, coast, NOW)
    assert [a["id"] for a in alerts] == ["f"]
    assert stats == {"listed": 6, "kept": 1, "ended": 1, "replaced": 1, "cancel": 1, "other": 2, "no_coast": []}


def test_end_falls_back_to_expiry_and_onset_to_effective(coast):
    doc = collection(feature(id="x", ends=None, onset=None, expires="2026-11-09T06:00:00-10:00"))
    (a,), _ = S.parse(doc, coast, NOW)
    assert a["ends"] is None and a["expires"] == int(datetime(2026, 11, 9, 16, tzinfo=timezone.utc).timestamp())
    assert a["onset"] == a["sent"]


def test_zones_without_a_coastline_are_counted(coast):
    doc = collection(feature(id="y", affectedZones=["https://api.weather.gov/zones/county/TXC123",
                                                    "https://api.weather.gov/zones/forecast/COZ001"]),
                     feature(id="z", affectedZones=["https://api.weather.gov/zones/forecast/HIZ007",
                                                    "https://api.weather.gov/zones/forecast/HIZ010"]))
    alerts, stats = S.parse(doc, coast, NOW)
    assert [(a["id"], a["zones"]) for a in alerts] == [("z", ["HIZ007"])]     # Central Oahu has no coast
    assert stats["no_coast"] == ["TXC123", "COZ001"]


def test_ugc_codes_count_as_zones(coast):
    doc = collection(feature(id="u", affectedZones=[], geocode={"UGC": ["HIZ006"]}))
    (a,), _ = S.parse(doc, coast, NOW)
    assert a["zones"] == ["HIZ006"]


@pytest.mark.parametrize("params,want", [
    ({"NWSheadline": ["A ... ...B"]}, "A ... B"),          # no part names the event: the whole line
    ({}, "High Surf Advisory issued"),                      # no NWS headline: the API's own
    ({"NWSheadline": [7]}, "High Surf Advisory issued"),
])
def test_headline_fallbacks(params, want, coast):
    (a,), _ = S.parse(collection(feature(id="h", parameters=params)), coast, NOW)
    assert a["headline"] == want


def test_texts_capped_and_plain(coast):
    doc = collection(feature(id="t", description="x" * 9000 + "\r\nend", instruction=None, senderName=7))
    (a,), _ = S.parse(doc, coast, NOW)
    assert len(a["description"]) == S.TEXT_CAP + 4 and a["description"].endswith(" ...")
    assert a["instruction"] == "" and a["office"] == ""


@pytest.mark.parametrize("bad", [[], {"type": "x"}, {"features": "no"}, None])
def test_not_a_feature_collection(bad, coast):
    with pytest.raises(ValueError):
        S.parse(bad, coast, NOW)


def test_bad_times_are_left_out(coast):
    doc = collection(feature(id="n", ends="tomorrow", expires="soon"), feature(id="m", onset="2026-11-09T06:00:00"))
    alerts, stats = S.parse(doc, coast, NOW)
    assert [a["id"] for a in alerts] == ["m"] and stats["ended"] == 1
    assert alerts[0]["onset"] == alerts[0]["sent"]                             # an onset without an offset is ignored


def test_ends_win_over_an_earlier_expiry(coast):
    # G59 F6: NWS often lets a product's message expire before the hazard ends (a Beach Hazards Statement expiring
    # tonight, ending in two days): the hazard's end decides
    doc = collection(feature(id="k", expires="2026-11-08T12:00:00-10:00", ends="2026-11-10T18:00:00-10:00"))
    (a,), stats = S.parse(doc, coast, NOW)
    assert a["id"] == "k" and stats["ended"] == 0


@pytest.mark.parametrize("bad", [
    {"event": ["High Surf Advisory"]},                  # unhashable
    {"id": {"x": 1}},
    {"affectedZones": 7},
    {"geocode": ["HIZ006"]},
    {"parameters": ["NWSheadline"]},
    {"references": 5},
    {"description": {"text": "x"}},
])
def test_one_malformed_feature_never_fails_the_read(bad, coast):
    # G59 F5: a malformed feature is left out (or read as far as it goes); the others stay
    doc = collection(feature(id="good"), feature(**dict({"id": "odd"}, **bad)))
    alerts, stats = S.parse(doc, coast, NOW)
    assert "good" in [a["id"] for a in alerts] and stats["listed"] == 2


def test_an_exception_in_one_feature_is_counted(coast, monkeypatch):
    real = S._zone_ids

    def boom(p):
        if p.get("id") == "odd":
            raise RuntimeError("bad")
        return real(p)
    monkeypatch.setattr(S, "_zone_ids", boom)
    alerts, stats = S.parse(collection(feature(id="good"), feature(id="odd")), coast, NOW)
    assert [a["id"] for a in alerts] == ["good"] and stats["other"] == 1 and stats["kept"] == 1


def test_the_url_names_every_product():
    assert S.URL.startswith("https://api.weather.gov/alerts/active?status=actual&event=")
    for ev in S.EVENTS:
        assert ev.replace(" ", "%20") in S.URL


# ---------------------------------------------------------------- the service
class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def service(t=NOW):
    return S.AlertService(COAST, clock=Clock(t))


def answer(doc=None):
    body = json.dumps(doc if doc is not None else fixture()).encode("utf-8")
    return lambda: body


def body(svc, now=None):
    payload, etag = svc.served(now)
    return json.loads(payload), etag


def test_service_states():
    svc = service()
    b, _ = body(svc)
    assert b["state"] == "loading" and b["alerts"] == [] and b["zones"] == {} and b["updated"] is None
    assert svc.refresh(lambda: b"not json") is False
    b, _ = body(svc)
    assert b["state"] == "unavailable"
    assert svc.refresh(answer()) is True
    b, _ = body(svc)
    assert b["state"] == "ok" and len(b["alerts"]) == 10 and b["updated"] == NOW
    named = {z for a in b["alerts"] for z in a["zones"]}
    assert set(b["zones"]) == named and all(b["zones"][z]["lines"] for z in named)
    assert b["zones"]["ORZ103"]["tz"] == "America/Los_Angeles" and b["zones"]["ORZ103"]["name"].startswith("Central Coast")
    b, _ = body(svc, NOW + S.STALE_S + 1)
    assert b["state"] == "stale" and b["alerts"] == [] and b["zones"] == {}


def test_failure_keeps_the_last_list():
    svc = service()
    svc.refresh(answer())
    svc.clock.t = NOW + 200
    assert svc.refresh(lambda: (_ for _ in ()).throw(ConnectionError("down"))) is False
    b, _ = body(svc)
    assert b["state"] == "ok" and len(b["alerts"]) == 10 and b["updated"] == NOW
    h = svc.health()
    assert h["alerts"] == 10 and h["fetches"] == 2 and h["last_error"].startswith("ConnectionError") and h["age_s"] == 200


def test_ended_alerts_leave_at_serve_time():
    svc = service()
    svc.refresh(answer())
    assert len(body(svc, NOW + 1800)[0]["alerts"]) == 10                     # within the held answer's life
    svc.ts = int(datetime(2026, 11, 9, 6, tzinfo=timezone.utc).timestamp())  # a read held from 06:00 UTC
    b, _ = body(svc, svc.ts + 60)                                            # the Oregon warnings end at 06:00 UTC
    zones = {z for a in b["alerts"] for z in a["zones"]}
    assert len(b["alerts"]) == 8 and not zones & {"ORZ101", "ORZ102", "ORZ103", "WAZ201"}
    assert set(b["zones"]) == zones


def test_payload_and_etag_are_reused_until_something_changes():
    svc = service()
    svc.refresh(answer())
    p1, e1 = svc.served()
    p2, e2 = svc.served()
    assert p1 is p2 and e1 == e2
    svc.clock.t = NOW + 130
    svc.refresh(answer())
    p3, e3 = svc.served()
    assert p3 == p1.replace('"updated":%d' % NOW, '"updated":%d' % (NOW + 130))
    assert e3 == e1                    # the same products: the same tag, so the page gets a 304 (G59 F8)
    doc = fixture()
    doc["features"] = doc["features"][1:]
    svc.refresh(answer(doc))
    assert svc.served()[1] != e1       # a product gone: a new tag


def test_due_rules():
    svc = service()
    assert svc.due()                                    # nothing held
    svc.refresh(answer())
    assert not svc.due(NOW + S.REFRESH_S - 1) and svc.due(NOW + S.REFRESH_S)
    svc.clock.t = NOW + S.REFRESH_S
    svc.refresh(lambda: b"[")
    assert not svc.due(NOW + S.REFRESH_S + S.RETRY_S - 1) and svc.due(NOW + S.REFRESH_S + S.RETRY_S)


def test_start_runs_one_read_in_the_background():
    svc = service()
    gate, calls = threading.Event(), []

    def slow():
        calls.append(1)
        gate.wait(5)
        return answer()()

    assert svc.start(slow) is True
    assert svc.start(slow) is False and not svc.due()   # one read at a time
    gate.set()
    svc.thread.join(5)
    assert calls == [1] and body(svc)[0]["state"] == "ok"
    svc.after_fork()
    assert svc.thread is None


def test_a_missing_coast_file_serves_no_zones(tmp_path):
    svc = S.AlertService(str(tmp_path / "none.json"), clock=Clock(NOW))
    svc.refresh(answer())
    b, _ = body(svc)
    assert b["alerts"] == [] and b["zones"] == {} and b["state"] == "ok"
    assert svc.health()["coast_zones"] == 0 and svc.health()["coast_error"]


# ---------------------------------------------------------------- the route
@pytest.fixture()
def client(monkeypatch):
    import app as A
    svc = service(int(time.time()))
    monkeypatch.setattr(A, "ALERTS", svc)
    monkeypatch.setattr(A, "COASTAL_ALERTS", True)
    return A, svc, A.app.test_client()


def shifted_answer(now):
    """The fixture moved so its alerts are in force at `now` (the route reads the real clock)."""
    doc = fixture()
    shift = now - NOW
    for f in doc["features"]:
        p = f["properties"]
        for k in ("sent", "effective", "onset", "ends", "expires"):
            if p.get(k):
                t = datetime.fromisoformat(p[k]).timestamp() + shift
                p[k] = datetime.fromtimestamp(t, timezone.utc).isoformat()
    return answer(doc)


def test_route(client):
    A, svc, c = client
    r = c.get("/api/alerts/coastal")
    assert r.status_code == 200 and r.get_json()["state"] == "loading"
    svc.refresh(shifted_answer(svc.clock.t))
    r = c.get("/api/alerts/coastal")
    b = r.get_json()
    assert r.status_code == 200 and b["state"] == "ok" and len(b["alerts"]) == 10
    assert r.headers["Cache-Control"] == "public, max-age=60" and r.headers["CDN-Cache-Control"] == "no-store"
    etag = r.headers["ETag"]
    r2 = c.get("/api/alerts/coastal", headers={"If-None-Match": etag})
    assert r2.status_code == 304 and r2.headers["ETag"] == etag


def test_route_off(client, monkeypatch):
    A, _svc, c = client
    monkeypatch.setattr(A, "COASTAL_ALERTS", False)
    assert c.get("/api/alerts/coastal").status_code == 404
    assert A._alerts_health() == {"enabled": False} and A._alerts_tick() is False


def test_route_starts_a_read_only_with_the_background_service(client, monkeypatch):
    A, svc, c = client
    started = []
    monkeypatch.setattr(svc, "start", lambda fetch: started.append(fetch) or True)
    monkeypatch.setattr(A, "LIVE_BACKGROUND", False)
    c.get("/api/alerts/coastal")
    assert started == []
    monkeypatch.setattr(A, "LIVE_BACKGROUND", True)
    monkeypatch.setattr(A, "start_live_background", lambda: None, raising=False)
    monkeypatch.setattr(A, "_live_background_start", lambda: None, raising=False)
    c.get("/api/alerts/coastal")
    assert started == [A._alerts_fetch]


def test_live_tick_reads_the_alerts(client, monkeypatch):
    A, svc, _c = client
    started = []
    monkeypatch.setattr(svc, "start", lambda fetch: started.append(fetch) or True)
    A._live_tick(providers=[])
    assert started == [A._alerts_fetch]


def test_live_tick_survives_an_alerts_failure(client, monkeypatch):
    A, svc, _c = client

    def boom(fetch):
        raise RuntimeError("x")
    monkeypatch.setattr(svc, "start", boom)
    assert "warm" in A._live_tick(providers=[])


def test_healthz_reports_the_alerts(client):
    A, svc, c = client
    svc.refresh(shifted_answer(svc.clock.t))
    h = c.get("/healthz").get_json()["alerts"]
    assert h["enabled"] is True and h["alerts"] == 10 and h["last_error"] is None and h["coast_zones"] > 330


def test_tests_never_reach_nws():
    import app as A
    with pytest.raises(ConnectionError):
        A._alerts_fetch()
