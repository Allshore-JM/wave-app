"""The trust-list pins and the check tool (plan section 57: eMudhra's compromised intermediates).

Every publisher's environment pins certifi and ca-certificates at a release without the compromised
certificates (the cached environments predated it) and records them; coast-build's pip jobs install
certifi so botocore uses it; tools/tls_check.py tells a refused host from a trusted one. Offline.
"""
import os
import re
import ssl
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import tls_check  # noqa: E402

WF = os.path.join(ROOT, ".github", "workflows")
PUBLISHERS = ("frames", "ecmwf", "points", "sst", "glwu", "gfswinds")
PIN = "2026.10.23"


def _read(name):
    return open(os.path.join(WF, name), encoding="utf-8").read()


def test_every_publisher_pins_and_records_the_trust_list():
    for n in PUBLISHERS:
        text = _read("model-%s.yml" % n)
        pkgs = text.split("create-args: >-", 1)[1].split("cache-environment", 1)[0].split()
        assert "certifi=" + PIN in pkgs and "ca-certificates=" + PIN in pkgs, n
        record = re.search(r"awk '\$1 ~ /\^\(([^)]*)\)\$/", text).group(1).split("|")
        assert "certifi" in record and "ca-certificates" in record, n


def test_coast_build_installs_certifi_and_the_latest_node_24():
    text = _read("coast-build.yml")
    pips = re.findall(r"pip install ([^\n]+)", text)
    assert len(pips) == 2 and all("certifi==" + PIN in p.split() for p in pips)
    assert re.search(r'node-version: "24"\n\s+check-latest: true', text)


def test_the_check_tool_classifies_answers(monkeypatch):
    calls = []

    def fake(host, cafile, timeout=15):
        calls.append((host, cafile))
        return "refused" if host in tls_check.COMPROMISED else "ok"

    monkeypatch.setattr(tls_check, "connect", fake)
    monkeypatch.setattr(tls_check, "contexts", lambda: {"certifi": "/c.pem", "default": None})
    assert tls_check.main(["--real"]) == 0
    assert len(calls) == 2 * (len(tls_check.COMPROMISED) + len(tls_check.REAL))
    monkeypatch.setattr(tls_check, "connect", lambda h, c, timeout=15: "ok")       # a trusted bad host
    assert tls_check.main([]) == 1
    monkeypatch.setattr(tls_check, "connect", lambda h, c, timeout=15: "error: timeout")   # cannot tell = fail
    assert tls_check.main([]) == 1
    monkeypatch.setattr(tls_check, "connect",
                        lambda h, c, timeout=15: "refused" if h in tls_check.COMPROMISED else "error: x")
    assert tls_check.main([]) == 0 and tls_check.main(["--real"]) == 1


def test_connect_reports_a_verification_failure_as_refused(monkeypatch):
    class Sock:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class Ctx:
        def wrap_socket(self, sock, server_hostname):
            err = ssl.SSLCertVerificationError("bad")
            err.verify_code = 20
            raise err

    monkeypatch.setattr(tls_check.socket, "create_connection", lambda addr, timeout: Sock())
    monkeypatch.setattr(tls_check.ssl, "create_default_context", lambda cafile=None: Ctx())
    assert tls_check.connect("x.example", None) == "refused"
    monkeypatch.setattr(tls_check.socket, "create_connection",
                        lambda addr, timeout: (_ for _ in ()).throw(OSError("down")))
    assert tls_check.connect("x.example", None) == "error: OSError"

