"""The trust list (plan sections 47 and 57).

Section 47 (2026-10-21) added a copy of the "emSign Root CA - G1" root for the SWAN request when Render's
servers lacked it. Section 57 (2026-10-27, eMudhra's compromised intermediates) removed it: every request
verifies against certifi's list, pinned at a release that left the compromised certificates out, and
/healthz reports which list the process uses.
"""
import os
import re

import certifi
import requests

import app

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_requirements_pin_a_certifi_without_the_compromised_certificates():
    lines = [l.strip() for l in open(os.path.join(REPO, "requirements.txt"), encoding="utf-8")]
    pins = [l for l in lines if l.lower().startswith("certifi")]
    assert pins == ["certifi>=2026.10.23"]


# Python files the running site imports (the repo root); tests/ and tools/ are not part of it.
SITE_FILES = sorted(f for f in os.listdir(REPO) if f.endswith(".py"))
# Anything that would make a request verify against another list or not at all (G40 P3-3: the first version of this
# test looked for the literal "verify=" in app.py only and let a session attribute or another module through).
WEAKENING = re.compile(r"\bverify\s*[=:]|\.verify\s*=|CERT_NONE|_create_unverified_context|check_hostname\s*=\s*False"
                       r"|\bssl_context\b|trust_env\s*=")
# A CA variable SET anywhere the site or its start runs (reading them in _tls_health is fine).
CA_VAR_SET = re.compile(r"(?:export\s+|environ\[\s*[\"'])\s*(?:REQUESTS_CA_BUNDLE|CURL_CA_BUNDLE|SSL_CERT_FILE|SSL_CERT_DIR)"
                        r"|(?:REQUESTS_CA_BUNDLE|CURL_CA_BUNDLE|SSL_CERT_FILE|SSL_CERT_DIR)\s*[=:]\s*\S"
                        r"|setdefault\(\s*[\"'](?:REQUESTS_CA_BUNDLE|CURL_CA_BUNDLE|SSL_CERT_FILE|SSL_CERT_DIR)")


def test_no_extra_roots_are_kept():
    assert not os.path.exists(os.path.join(REPO, "certs"))
    for name in ("_ca_bundle", "_EXTRA_CA_FILES", "_CA_BUNDLE"):
        assert not hasattr(app, name)


# The one exception (plan section 69, owner 2026-10-31): Taiwan CWA's chain without the strict X.509 flag, pinned below.
TLS_EXCEPTION = "cwa_tls.py"


def test_no_site_module_changes_how_requests_verify():
    assert "app.py" in SITE_FILES and "buoy_sources.py" in SITE_FILES and len(SITE_FILES) >= 8
    assert TLS_EXCEPTION in SITE_FILES
    for name in SITE_FILES:
        if name == TLS_EXCEPTION:
            continue
        text = open(os.path.join(REPO, name), encoding="utf-8").read()
        hits = [m.group(0) for m in WEAKENING.finditer(text)]
        assert not hits, (name, hits)


def test_no_ca_variable_is_set_for_the_site():
    names = SITE_FILES + [f for f in ("start.sh", "render.yaml", "Procfile", "runtime.txt", "requirements.txt")
                          if os.path.exists(os.path.join(REPO, f))]
    assert "start.sh" in names
    for name in names:
        text = open(os.path.join(REPO, name), encoding="utf-8").read()
        hits = [m.group(0) for m in CA_VAR_SET.finditer(text)]
        assert not hits, (name, hits)


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
    assert out["bundle_is_certifi"] is True and out["env_override"] == []
    # SSL_CERT_FILE does not change what requests verifies against: listed, still certifi (G40 P3-2)
    monkeypatch.setenv("SSL_CERT_FILE", "/x.pem")
    out = app._tls_health()
    assert out["env_override"] == ["SSL_CERT_FILE"] and out["bundle_is_certifi"] is True
    # REQUESTS_CA_BUNDLE / CURL_CA_BUNDLE replace certifi's file for every request: the flag says so
    for var in ("REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE"):
        monkeypatch.setenv(var, "/y.pem")
        out = app._tls_health()
        assert out["bundle_is_certifi"] is False and var in out["env_override"], var
        monkeypatch.delenv(var)
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", certifi.where())                # pointing at certifi itself is fine
    assert app._tls_health()["bundle_is_certifi"] is True


def test_healthz_carries_the_tls_key(monkeypatch):
    monkeypatch.setattr(app, "_tls_health", lambda: {"certifi": "9.9.9"})
    body = app.app.test_client().get("/healthz").get_json()
    assert body["tls"] == {"certifi": "9.9.9"}


def test_the_one_exception_is_cwa_without_the_strict_flag_and_nothing_else():
    import ssl
    import cwa_tls
    text = open(os.path.join(REPO, TLS_EXCEPTION), encoding="utf-8").read()
    assert sorted({m.group(0) for m in WEAKENING.finditer(text)}) == ["ssl_context"]
    assert cwa_tls.HOST == "https://opendata.cwa.gov.tw/"
    import requests.adapters
    # the context requests itself verifies with, but for the one flag (G45 F-13)
    ctx, default = cwa_tls.context(), requests.adapters._preloaded_ssl_context
    assert ctx.verify_mode == ssl.CERT_REQUIRED and ctx.check_hostname is True
    assert ctx.verify_flags == default.verify_flags & ~ssl.VERIFY_X509_STRICT
    assert ctx.options == default.options and ctx.minimum_version == default.minimum_version >= ssl.TLSVersion.TLSv1_2
    assert ctx.maximum_version == default.maximum_version and ctx.hostname_checks_common_name is False
    assert ctx.cert_store_stats()["x509_ca"] ==         ssl.create_default_context(cafile=certifi.where()).cert_store_stats()["x509_ca"] > 100
    for session in (app.HTTP, app._POINT_HTTP):
        adapter = session.get_adapter("https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0003-001")
        assert isinstance(adapter, cwa_tls.CwaAdapter)
        assert adapter.poolmanager.connection_pool_kw["ssl_context"].verify_flags == ctx.verify_flags
        for other in ("https://www.pacioos.hawaii.edu/x", "https://opendata.cwa.gov.tw.example.com/x",
                      "https://cwaopendata.s3.ap-northeast-1.amazonaws.com/x", "http://opendata.cwa.gov.tw/x"):
            assert not isinstance(session.get_adapter(other), cwa_tls.CwaAdapter), other
    assert app.HTTP.get_adapter(cwa_tls.HOST).max_retries.total == app.HTTP.get_adapter("https://x.org/").max_retries.total
    proxied = app.HTTP.get_adapter(cwa_tls.HOST).proxy_manager_for("http://proxy.example:3128")
    assert proxied.connection_pool_kw["ssl_context"].verify_flags == ctx.verify_flags      # the proxy path too
