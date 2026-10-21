"""SWAN tables (www.pacioos.hawaii.edu) are verified against certifi's list plus the emSign root.

In October 2026 every SWAN download on Render failed with CERTIFICATE_VERIFY_FAILED: PacIOOS's chain ends at
"emSign Root CA - G1", which the list the servers verified against did not contain. app._ca_bundle() joins
certifi's list and certs/emsign_root_ca_g1.pem; the SWAN request passes it as verify=, so a host setting
(REQUESTS_CA_BUNDLE / CURL_CA_BUNDLE) cannot take the root away again.
"""
import hashlib
import os
import ssl
import tempfile

import certifi
import pytest

import app

ROOT_PEM = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "certs", "emsign_root_ca_g1.pem")
# Mozilla's published SHA-256 for "emSign Root CA - G1" (the same as certifi's comment).
ROOT_SHA256 = "40f6af0346a99aa1cd1d555a4e9cce62c7f9634603ee406615833dc8c8d00367"


@pytest.fixture
def fresh_bundle(monkeypatch, tmp_path):
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    monkeypatch.setitem(app._CA_BUNDLE, "path", None)
    return tmp_path


def _pem_body(text):
    return text[text.index("-----BEGIN CERTIFICATE-----"):text.index("-----END CERTIFICATE-----") + 25]


def test_the_extra_root_is_the_emsign_root_and_is_used():
    assert app._EXTRA_CA_FILES == (ROOT_PEM,)
    der = ssl.PEM_cert_to_DER_cert(_pem_body(open(ROOT_PEM, encoding="utf-8").read()))
    assert hashlib.sha256(der).hexdigest() == ROOT_SHA256


def test_bundle_is_certifi_plus_the_root_written_once(fresh_bundle, monkeypatch):
    path = app._ca_bundle()
    assert os.path.dirname(path) == str(fresh_bundle)
    text = open(path, encoding="utf-8").read()
    assert text.startswith(open(certifi.where(), encoding="utf-8").read().rstrip("\n"))
    assert _pem_body(open(ROOT_PEM, encoding="utf-8").read()) in text
    assert text.endswith("-----END CERTIFICATE-----\n")
    # loadable as a CA file
    ssl.create_default_context(cafile=path)
    # cached in memory: the second call reads and writes nothing
    monkeypatch.setattr(app.os, "replace", lambda *a: (_ for _ in ()).throw(AssertionError("written twice")))
    monkeypatch.setattr(app, "open", lambda *a, **k: (_ for _ in ()).throw(AssertionError("read again")), raising=False)
    assert app._ca_bundle() == path


def test_an_existing_bundle_of_the_same_content_is_reused(fresh_bundle, monkeypatch):
    path = app._ca_bundle()
    monkeypatch.setitem(app._CA_BUNDLE, "path", None)
    monkeypatch.setattr(app.os, "replace", lambda *a: (_ for _ in ()).throw(AssertionError("rewritten")))
    assert app._ca_bundle() == path


def test_a_failure_falls_back_to_the_default(fresh_bundle, monkeypatch):
    monkeypatch.setattr(app, "_EXTRA_CA_FILES", (str(fresh_bundle / "missing.pem"),))
    assert app._ca_bundle() is True
    assert app._CA_BUNDLE["path"] is None


def test_the_swan_download_verifies_with_the_bundle(fresh_bundle, monkeypatch):
    seen = {}

    def fake_get(url, **kw):
        seen["url"], seen["kw"] = url, kw
        raise RuntimeError("stop here")

    monkeypatch.setattr(app.HTTP, "get", fake_get)
    out = app._parse_swan_uncached("51201", None)
    assert out[-1] == "Could not download SWAN forecast for 51201"
    assert seen["url"] == app.SWAN_TABLE_BASE + "/buoy.cdip106.table"
    assert seen["kw"]["verify"] == app._ca_bundle()
    assert seen["kw"]["verify"] is not True
