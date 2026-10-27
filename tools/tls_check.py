"""Check which TLS trust list this Python environment uses (plan section 57).

Usage:  python tools/tls_check.py [--real] [--paths certifi,default,botocore]

Connects to the two hosts Render named for eMudhra's compromised certificates (both must be REFUSED as
unverifiable) through every trust path the site and the jobs use:
  * certifi    -- requests (the site) and botocore when certifi is installed (the jobs' uploads)
  * default    -- ssl.create_default_context(): urllib, i.e. the jobs' downloads (fetch.py, ecmwf_fetch.py)
  * botocore   -- the CA file botocore would pass (its own cacert.pem when certifi is missing)
With --real it also connects to the hosts the site and the jobs read (all must verify). --paths limits the
check to the trust paths an environment actually uses: the site uses requests only (certifi); on GitHub's Ubuntu
runners the system list ("default") still trusted one of the compromised chains in October 2026, so jobs that
download with curl pass it certifi's file.

A test host passes only when the certificate is refused because its issuer is not trusted (OpenSSL verify codes 2,
19, 20, 21). Any other verdict proves nothing about the trust list and fails: an expired leaf (code 10: both test
certificates expire on 2027-05-03), a wrong host name (62), another TLS error. Of the two hosts, only
dv-g3b-emudhra (chain to "emSign ECC Root CA - G3") tells an old certifi from a new one; dv-g3b-aaa-emudhra (Comodo's
"AAA Certificate Services" cross-sign) is refused by old certifi releases too and matters for system lists.

A real host that cannot be reached (a timeout or a refused connection: no TLS answer at all) is asked once more
after a pause and then reported as "unreachable": a network result, not a trust one, so it does not fail the check.
A TLS answer that is not a verified handshake (a certificate error, a handshake alert) fails it. The two test hosts
must answer and be refused: unreachable there fails (nothing was proved). Not checked: the R2 upload host (its name
holds the account id); the first publish after a change proves it.

Exit 0 when every check passes, 1 otherwise. One line per check, then a summary line; nothing secret.
"""
import argparse
import importlib
import socket
import ssl
import sys

COMPROMISED = (
    "dv-g3b-emudhra.we-have-a-problem.com",
    "dv-g3b-aaa-emudhra.we-have-a-problem.com",
)
REAL = (
    "www.ndbc.noaa.gov", "nomads.ncep.noaa.gov", "noaa-gfs-bdp-pds.s3.amazonaws.com", "api.weather.gov",
    "api.tidesandcurrents.noaa.gov", "aviationweather.gov", "madis-data.ncep.noaa.gov", "coastwatch.pfeg.noaa.gov",
    "data.ecmwf.int", "ecmwf-forecasts.s3.eu-central-1.amazonaws.com", "s3.waw3-1.cloudferro.com",
    "www.pacioos.hawaii.edu", "thredds.aodn.org.au", "erddap.cdip.ucsd.edu", "erddap.marine.ie",
    "wavenet-api.cefas.co.uk", "opendata-download-ocobs.smhi.se", "ddapi20-waterwebservices.rijkswaterstaat.nl",
    "apps.des.qld.gov.au", "auswaves.org", "models.allshoresurf.com", "allshoresurf.com",
    # coast-build's downloads (curl with certifi's file): GSHHG's release (a redirect), Natural Earth, GeoNames
    "github.com", "release-assets.githubusercontent.com", "raw.githubusercontent.com", "download.geonames.org",
)
UNTRUSTED_ISSUER = (2, 19, 20, 21)    # unable to get issuer / self-signed in chain / no local issuer / leaf unverifiable


def contexts():
    """name -> (ssl context, CA file or None for the system default)."""
    out = {}
    try:
        import certifi
        out["certifi"] = certifi.where()
    except ImportError:
        pass
    out["default"] = None
    try:
        httpsession = importlib.import_module("botocore.httpsession")
        out["botocore"] = httpsession.get_cert_path(True)
    except Exception:
        pass
    return out


def connect(host, cafile, timeout=15):
    """'ok'; 'refused' (the issuer is not trusted); 'bad certificate (code N: ...)' (refused for another reason, e.g.
    expired); 'tls error: ...' (a TLS answer that is no verified handshake); 'error: ...' (no TLS answer at all)."""
    ctx = ssl.create_default_context(cafile=cafile) if cafile else ssl.create_default_context()
    try:
        with socket.create_connection((host, 443), timeout=timeout) as sock:
            with ctx.wrap_socket(sock, server_hostname=host):
                return "ok"
    except ssl.SSLCertVerificationError as exc:
        if exc.verify_code in UNTRUSTED_ISSUER:
            return "refused"
        return "bad certificate (code %s: %s)" % (exc.verify_code, exc.verify_message)
    except ssl.SSLError as exc:
        return "tls error: %s" % (getattr(exc, "reason", None) or type(exc).__name__)
    except Exception as exc:
        return "error: %s" % type(exc).__name__


def versions():
    out = ["python=%s" % sys.version.split()[0], "openssl=%s" % ssl.OPENSSL_VERSION.split()[1]]
    for mod in ("certifi", "botocore", "requests"):
        try:
            out.append("%s=%s" % (mod, importlib.import_module(mod).__version__))
        except Exception:
            out.append("%s=none" % mod)
    return " ".join(out)


def connect_retry(host, cafile, pause=5.0, sleep=None):
    """connect(), asked once more after `pause` seconds when the first answer was a connection error."""
    got = connect(host, cafile)
    if got.startswith("error"):
        (sleep or __import__("time").sleep)(pause)
        got = connect(host, cafile)
    return got


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--real", action="store_true", help="also check that the real hosts verify")
    ap.add_argument("--paths", default="", help="comma list of the trust paths to check (default: every one found)")
    args = ap.parse_args(argv)
    print(versions())
    found = contexts()
    want = [p for p in args.paths.split(",") if p] or list(found)
    bad = unreachable = 0
    for name in want:
        if name not in found:
            bad += 1
            print("trust path %s: FAIL not available in this environment" % name)
            continue
        cafile = found[name]
        print("trust path %s: %s" % (name, cafile or "system default"))
        for host in COMPROMISED:
            got = connect_retry(host, cafile)
            good = got == "refused"
            bad += not good
            print("  %-4s %-45s %s (must be refused)" % ("PASS" if good else "FAIL", host, got))
        if args.real:
            for host in REAL:
                got = connect_retry(host, cafile)
                if got.startswith("error"):
                    unreachable += 1
                    print("  %-4s %-45s %s (could not connect: no certificate result)" % ("NOTE", host, got))
                    continue
                good = got == "ok"
                bad += not good
                print("  %-4s %-45s %s" % ("PASS" if good else "FAIL", host, got))
    note = " (%d unreachable)" % unreachable if unreachable else ""
    print("tls_check: %s%s" % ("all passed" if not bad else "%d failed" % bad, note))
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main())
