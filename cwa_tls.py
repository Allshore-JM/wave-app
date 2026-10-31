"""Taiwan CWA's TLS (plan section 69, owner decision 2026-10-31).

opendata.cwa.gov.tw's chain ends at "TWCA Global Root CA", a root (in certifi's list) without the Subject Key Identifier
extension. Python 3.13 turns OpenSSL's strict X.509 checks on by default (ssl.VERIFY_X509_STRICT), and they refuse that
chain ("Missing Subject Key Identifier"). For that one host the site's sessions use a context that is the default
context in every other respect: certifi's list, the chain verified (CERT_REQUIRED), the hostname checked. Only the
strict flag is off. tests/test_swan_cert.py lets this file, and no other site module, set a context, and pins what it
does.
"""
import ssl

import certifi
from requests.adapters import HTTPAdapter

HOST = "https://opendata.cwa.gov.tw/"


def context():
    """The default verifying context on certifi's list, without the strict X.509 flag."""
    ctx = ssl.create_default_context(cafile=certifi.where())
    ctx.verify_flags &= ~getattr(ssl, "VERIFY_X509_STRICT", 0)
    return ctx


class CwaAdapter(HTTPAdapter):
    """requests' adapter with context() for its connection pools."""

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = context()
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        kwargs["ssl_context"] = context()
        return super().proxy_manager_for(*args, **kwargs)


def mount(session, **adapter_kwargs):
    """session with CwaAdapter(**adapter_kwargs) for CWA's host only (every other host keeps its adapter)."""
    session.mount(HOST, CwaAdapter(**adapter_kwargs))
    return session
