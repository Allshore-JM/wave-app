"""The trust list (plan sections 47 and 57).

Section 47 (2026-10-21) added a copy of the "emSign Root CA - G1" root for the SWAN request when Render's
servers lacked it. Section 57 (2026-10-27, eMudhra's compromised intermediates) removed it: every request
verifies against certifi's list, pinned at a release that left the compromised certificates out, and
/healthz reports which list the process uses.
"""
import os

import certifi
import requests

import app

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_requirements_pin_a_certifi_without_the_compromised_certificates():
    lines = [l.strip() for l in open(os.path.join(REPO, "requirements.txt"), encoding="utf-8")]
    pins = [l for l in lines if l.lower().startswith("certifi")]
    assert pins == ["certifi>=2026.10.23"]


def test_no_extra_roots_are_kept():
    assert not os.path.exists(os.path.join(REPO, "certs"))
    for name in ("_ca_bundle", "_EXTRA_CA_FILES", "_CA_BUNDLE"):
        assert not hasattr(app, name)
    assert "verify=" not in open(os.path.join(REPO, "app.py"), encoding="utf-8").read()


def test_the_swan_download_uses_the_default_list(monkeypatch):
    seen = {}

    def fake_get(url, **kw):
        seen["url"], seen["kw"] = url, kw
        raise RuntimeError("stop here")

    monkeypatch.setattr(app.HTTP, "get", fake_get)
    out = app._parse_swan_uncached("51201", None)
    assert out[-1] == "Could not download SWAN forecast for 51201"
    assert seen["url"] == app.SWAN_TABLE_BASE + "/buoy.cdip106.table"
    assert "verify" not in seen["kw"]


def test_tls_health_reports_certifi_and_overrides(monkeypatch):
    for k in app._CA_ENV_VARS:
        monkeypatch.delenv(k, raising=False)
    out = app._tls_health()
    assert out["certifi"] == certifi.__version__
    assert out["bundle_is_certifi"] is (os.path.abspath(requests.certs.where()) == os.path.abspath(certifi.where()))
    assert out["env_override"] == []
    monkeypatch.setenv("SSL_CERT_FILE", "/x.pem")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/y.pem")
    assert app._tls_health()["env_override"] == ["REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"]


def test_healthz_carries_the_tls_key(monkeypatch):
    monkeypatch.setattr(app, "_tls_health", lambda: {"certifi": "9.9.9"})
    body = app.app.test_client().get("/healthz").get_json()
    assert body["tls"] == {"certifi": "9.9.9"}
