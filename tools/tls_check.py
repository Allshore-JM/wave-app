"""Check which TLS trust list this Python environment uses (plan section 57).

Usage:  python tools/tls_check.py [--real]

Connects to the two hosts Render named for eMudhra's compromised certificates (both must be REFUSED as
unverifiable) through every trust path the site and the jobs use:
  * certifi    -- requests (the site) and botocore when certifi is installed (the jobs' uploads)
  * default    -- ssl.create_default_context(): urllib, i.e. the jobs' downloads (fetch.py, ecmwf_fetch.py)
  * botocore   -- the CA file botocore would pass (its own cacert.pem when certifi is missing)
With --real it also connects to the hosts the site and the jobs read (all must verify).

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
)


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
    """'ok', 'refused' (certificate not trusted) or 'error: ...' (could not tell)."""
    ctx = ssl.create_default_context(cafile=cafile) if cafile else ssl.create_default_context()
    try:
        with socket.create_connection((host, 443), timeout=timeout) as sock:
            with ctx.wrap_socket(sock, server_hostname=host):
                return "ok"
    except ssl.SSLCertVerificationError as exc:
        return "refused" if exc.verify_code in (19, 20, 21, 2) else "refused (code %s)" % exc.verify_code
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


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--real", action="store_true", help="also check that the real hosts verify")
    args = ap.parse_args(argv)
    print(versions())
    bad = 0
    for name, cafile in contexts().items():
        print("trust path %s: %s" % (name, cafile or "system default"))
        for host in COMPROMISED:
            got = connect(host, cafile)
            good = got.startswith("refused")
            bad += not good
            print("  %-4s %-45s %s (must be refused)" % ("PASS" if good else "FAIL", host, got))
        if args.real:
            for host in REAL:
                got = connect(host, cafile)
                good = got == "ok"
                bad += not good
                print("  %-4s %-45s %s" % ("PASS" if good else "FAIL", host, got))
    print("tls_check: %s" % ("all passed" if not bad else "%d failed" % bad))
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main())
