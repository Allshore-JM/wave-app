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
    curls = re.findall(r"curl [^\n]+", text)
    # GitHub's Ubuntu list still trusted a compromised chain (2026-10-27): every download takes certifi's file
    assert len(curls) == 3 and all('--cacert "$CA" --capath "$NOCAPATH"' in c for c in curls), curls
    # Ubuntu's curl keeps its own CA folder (/etc/ssl/certs) next to --cacert: the dry run saw it trust
    # dv-g3b-emudhra with --cacert alone; an empty --capath leaves certifi's file as the only list
    assert text.count("certifi.where()") == 2 and text.count('NOCAPATH="$(mktemp -d)"') == 2


def test_the_check_tool_classifies_answers(monkeypatch):
    monkeypatch.setattr(__import__("time"), "sleep", lambda s: None)          # connect_retry's pause
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
                        lambda h, c, timeout=15: "refused" if h in tls_check.COMPROMISED else "refused")
    assert tls_check.main([]) == 0 and tls_check.main(["--real"]) == 1      # a real host refused fails
    for answer in ("bad certificate (code 10: certificate has expired)", "tls error: X"):
        monkeypatch.setattr(tls_check, "connect", lambda h, c, timeout=15, a=answer: a)
        assert tls_check.main([]) == 1, answer                              # a test host: proves nothing = fail
        monkeypatch.setattr(tls_check, "connect",
                            lambda h, c, timeout=15, a=answer: "refused" if h in tls_check.COMPROMISED else a)
        assert tls_check.main(["--real"]) == 1, answer                      # a real host: not a network result


def test_the_real_hosts_include_coast_builds_downloads():
    for host in ("github.com", "release-assets.githubusercontent.com", "raw.githubusercontent.com",
                 "download.geonames.org"):
        assert host in tls_check.REAL


def test_unreachable_real_hosts_are_reported_not_failed(monkeypatch, capsys):
    monkeypatch.setattr(tls_check, "contexts", lambda: {"certifi": "/c.pem"})
    monkeypatch.setattr(__import__("time"), "sleep", lambda s: None)          # connect_retry's pause
    asked = []

    def fake(h, c, timeout=15):
        asked.append(h)
        if h in tls_check.COMPROMISED:
            return "refused"
        return "error: TimeoutError" if h == "www.pacioos.hawaii.edu" else "ok"

    monkeypatch.setattr(tls_check, "connect", fake)
    assert tls_check.main(["--real"]) == 0
    assert asked.count("www.pacioos.hawaii.edu") == 2                       # asked once more before giving up
    txt = capsys.readouterr().out
    assert "NOTE www.pacioos.hawaii.edu" in txt and "all passed (1 unreachable)" in txt
    # a test host that cannot be reached proves nothing: it fails
    monkeypatch.setattr(tls_check, "connect", lambda h, c, timeout=15: "error: OSError")
    assert tls_check.main([]) == 1
    # a second answer after a connection error counts
    answers = iter(["error: TimeoutError", "ok"])
    monkeypatch.setattr(tls_check, "connect", lambda h, c, timeout=15: next(answers))
    assert tls_check.connect_retry("x", None, sleep=lambda s: None) == "ok"


def test_connect_reports_a_verification_failure_as_refused(monkeypatch):
    class Sock:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class Ctx:
        code = 20

        def wrap_socket(self, sock, server_hostname):
            if self.code is None:
                err = ssl.SSLError("alert")
                err.reason = "SSLV3_ALERT_HANDSHAKE_FAILURE"
                raise err
            err = ssl.SSLCertVerificationError("bad")
            err.verify_code, err.verify_message = self.code, "test"
            raise err

    monkeypatch.setattr(tls_check.socket, "create_connection", lambda addr, timeout: Sock())
    monkeypatch.setattr(tls_check.ssl, "create_default_context", lambda cafile=None: Ctx())
    for code in (2, 19, 20, 21):                     # the issuer is not trusted: what the test hosts must show
        Ctx.code = code
        assert tls_check.connect("x.example", None) == "refused", code
    # refused for another reason proves nothing about the trust list (G40 P3-1): both test certificates expire on
    # 2027-05-03, after which a list that trusts the compromised chain would also answer "expired"
    for code in (10, 62):
        Ctx.code = code
        got = tls_check.connect("x.example", None)
        assert got.startswith("bad certificate (code %d" % code), got
    Ctx.code = None                                  # a TLS answer that is no verified handshake (a handshake alert)
    assert tls_check.connect("x.example", None) == "tls error: SSLV3_ALERT_HANDSHAKE_FAILURE"
    monkeypatch.setattr(tls_check.socket, "create_connection",
                        lambda addr, timeout: (_ for _ in ()).throw(OSError("down")))
    assert tls_check.connect("x.example", None) == "error: OSError"


def test_the_check_tool_limits_the_trust_paths(monkeypatch, capsys):
    seen = []
    monkeypatch.setattr(tls_check, "contexts", lambda: {"certifi": "/c.pem", "default": None, "botocore": "/b.pem"})
    monkeypatch.setattr(tls_check, "connect", lambda h, c, timeout=15: seen.append(c) or "refused")
    assert tls_check.main(["--paths", "certifi,botocore"]) == 0
    assert set(seen) == {"/c.pem", "/b.pem"}                    # the system list was not asked
    seen.clear()
    assert tls_check.main([]) == 0 and None in seen              # without --paths: every path found
    assert tls_check.main(["--paths", "certifi,nope"]) == 1      # a path the environment lacks fails
    assert "trust path nope: FAIL" in capsys.readouterr().out


def test_the_temporary_check_workflow_is_gone_and_the_pins_run_in_ci():
    """The push-triggered dry run (step 1-2) is removed before the merge; the pin tests read coast-build.yml and the
    check tool, so the job-tests workflow runs when either changes (push and pull request)."""
    assert not os.path.exists(os.path.join(WF, "tls-dryrun.yml"))
    tests = _read("model-frames-tests.yml")
    push, pr = tests.split("pull_request:", 1)
    for part in (push, pr.split("permissions:", 1)[0]):
        assert '"tools/tls_check.py"' in part and '".github/workflows/coast-build.yml"' in part



def test_no_workflow_downloads_or_installs_around_the_pins():
    """Every workflow, not only coast-build (G40 P3-3): each curl takes certifi's file and an empty CA folder, nothing
    uses wget, every certifi named anywhere is the pinned release, and no workflow sets a CA variable."""
    names = sorted(f for f in os.listdir(WF) if f.endswith(".yml"))
    assert "coast-build.yml" in names and len(names) >= 8
    curls = 0
    for n in names:
        text = _read(n)
        for line in re.findall(r"[^\n]*\bcurl\b[^\n]*", text):
            line = line.split(" #", 1)[0]               # a trailing shell comment may mention curl
            if not re.search(r"\bcurl\b", line) or line.strip().startswith("#"):
                continue
            curls += 1
            assert '--cacert "$CA" --capath "$NOCAPATH"' in line, (n, line)
        assert not re.search(r"(?m)^[^#\n]*\bwget\b", text), n
        for pin in re.findall(r"certifi\s*(?:==|=|>=|<=|~=|>|<)\s*([0-9][0-9.]*)", text):
            assert pin == PIN, (n, pin)
        assert not re.search(r"(?m)^[^#\n]*\b(REQUESTS_CA_BUNDLE|CURL_CA_BUNDLE|SSL_CERT_FILE|SSL_CERT_DIR|AWS_CA_BUNDLE)\b",
                             text), n
    assert curls == 3                                # coast-build's three downloads; a new one must be looked at
