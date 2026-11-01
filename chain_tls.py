"""Two WIS2 nodes' incomplete certificate chains (plan section 73, owner decision 2026-11-01).

wis.weathersa.co.za (South African Weather Service) sends its leaf and a cross-signed root but not the intermediate that
issued the leaf; wis2box.imd.gov.in (India Meteorological Department) sends its leaf only. Browsers fetch the missing
intermediate themselves (the leaf's "CA Issuers" address); Python does not, and refuses both hosts. For those two hosts
the site's sessions use a context that is requests' own (urllib3's default on certifi's list: the chain verified,
CERT_REQUIRED, the hostname checked, the strict X.509 flag kept) with the CAs' published intermediates added, so the
chain is built up to a root in certifi's list: GoDaddy TLS Intermediate CA DV - R1v1 with GoDaddy TLS Root CA - R1 (its
cross-sign by Go Daddy Root Certificate Authority - G2), and Sectigo Public Server Authentication CA DV R36 (issued by
Sectigo Public Server Authentication Root R46). Nothing else changes; every other host keeps its adapter.
tests/test_swan_cert.py pins the certificates by SHA-256 and lets this file and cwa_tls.py, no other site module, set a
context. When a node's CA changes its intermediate the node fails again (its flags go grey), as before this file.
"""
import hashlib
import ssl

import certifi
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

HOSTS = ("https://wis.weathersa.co.za/", "https://wis2box.imd.gov.in/")
# subject -> (PEM, SHA-256 of the DER), as published at the leaves' "CA Issuers" addresses (2026-11-01)
INTERMEDIATES = {
    "GoDaddy TLS Intermediate CA DV - R1v1": ("""-----BEGIN CERTIFICATE-----
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
-----END CERTIFICATE-----""",
        "7a43bc7747d0633fbd90ff900c9242417c027dbdca05af72da9a70e3518dbe2e"),
    "GoDaddy TLS Root CA - R1 (cross-signed by Go Daddy Root Certificate Authority - G2)": ("""-----BEGIN CERTIFICATE-----
MIIFhjCCBG6gAwIBAgIRAJDebH+ztQs8BhdyT8E0Aq0wDQYJKoZIhvcNAQELBQAw
gYMxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdBcml6b25hMRMwEQYDVQQHEwpTY290
dHNkYWxlMRowGAYDVQQKExFHb0RhZGR5LmNvbSwgSW5jLjExMC8GA1UEAxMoR28g
RGFkZHkgUm9vdCBDZXJ0aWZpY2F0ZSBBdXRob3JpdHkgLSBHMjAeFw0yNTA5MjQx
MjAwMDBaFw0zNzEyMzEyMzU5NTlaMEYxCzAJBgNVBAYTAlVTMRQwEgYDVQQKEwtH
b0RhZGR5LmNvbTEhMB8GA1UEAxMYR29EYWRkeSBUTFMgUm9vdCBDQSAtIFIxMIIC
IjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAwutyndeeSeRO9j0jUjOxADwr
++nlMlGJiwJlYyK1fEYf7IPX50+4LmdXO7oEuVdZnx/sTtyzuUYFR7KB5vYKysRL
HW4W0HxHclHhQ4Aw0T8quVgXAcHJ0XUmFy20wQSSiMGq30h/4qzwajYF7isgCPj3
nIXMCsA9zwDHaTIaCp/1KXRLVJRvGS681Giaxve42D6txJaDjNRWLFCKMhmbYAgH
E1b2DNqCGzp2eiDquCrAFgqTtY2Fk58BOSr8YdT2CWeaKIwx4U8raSqj1g1aTXs3
MRiasGnoQsnAzCG66lcuJ+ag4AvZrG+Q7VyLrpYoHE8Hk4+DLlZ92FinuXYDPx30
wMpYJjA9OlGPB/jUCAYqGvAyMQn4f+0b7C7u9YfQGT4F+XaAG03IaO85sYkcyAm6
w8BINtZOEFrDg+sDqwpM09pAJi1UJiLqH8lcHf0/aKVOLF4L2GIQWY+TCmWCN/+3
/sK8XpegJEgWRTa9Rg3lZD26kthFMxkhWsc7Ec5zF6+9sPaNK4jPZnJ1ZZ3BLOhB
vyDn9uaF+tfAs+UIBmGHH4TmWMz84uWRrJKiLfmYYTgxU5+V33gnubXVR5jvq3jd
EgZbQcXt/CbnnoLe2F3vRFncpbBKI4ElAbMMzONj5HneQmdSWM9KVKoN6ZQTV+wb
t684+CtlJdzys/ZZMDECAwEAAaOCAS8wggErMA4GA1UdDwEB/wQEAwIBhjATBgNV
HSUEDDAKBggrBgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MB0GA1UdDgQWBBTsUhGV
cHMZyN7KSEOXSxw1JCJDUDAfBgNVHSMEGDAWgBQ6moUHEGcotu/2vQVBbiDBlNoP
3jBUBggrBgEFBQcBAQRIMEYwRAYIKwYBBQUHMAKGOGh0dHA6Ly9jZXJ0aWZpY2F0
ZXMuZ29kYWRkeS5jb20vcmVwb3NpdG9yeS9nZHJvb3QtZzIuY3J0MCYGA1UdIAQf
MB0wCAYGZ4EMAQIBMAgGBmeBDAECAjAHBgVngQwBATA1BgNVHR8ELjAsMCqgKKAm
hiRodHRwOi8vY3JsLmdvZGFkZHkuY29tL2dkcm9vdC1nMi5jcmwwDQYJKoZIhvcN
AQELBQADggEBAEhbCwX1uaC5L9mJ+D6X4P/VH6Q2+zfX+dXnEBNNoE/xqboBmdFg
4SPi0+cRl1DBGELmn7L3wXdzuTh+e4bHIgvbzoZL8UJjQbTfE5w/EUU9tn0vsPxh
MgN2uWpme0gEGNTwUoX9D36vh1cDardevHaSbNYXRPR40Hrv5dHgoP0FVeaCZ09u
q2mtRVg6PSIQE5ycT2gPrbFxfo+lF/zxCy6BXkf46toy8AVz30rAYkVBNSqGANO0
rAeYLisohAJC59e+sjjxyFL9i8iYuD1PjUUYTNtQPPeOQKAhAmEphJ0iWPt98lrI
NaewsVVCcxYIgvifrn/Ptta4PPW7Z35c+Yc=
-----END CERTIFICATE-----""",
        "7bcb0f2f2d1031a6af8d61baa835d2835a3b8bcc26d94a3b048b1655fb81298c"),
    "Sectigo Public Server Authentication CA DV R36": ("""-----BEGIN CERTIFICATE-----
MIIGTDCCBDSgAwIBAgIQOXpmzCdWNi4NqofKbqvjsTANBgkqhkiG9w0BAQwFADBf
MQswCQYDVQQGEwJHQjEYMBYGA1UEChMPU2VjdGlnbyBMaW1pdGVkMTYwNAYDVQQD
Ey1TZWN0aWdvIFB1YmxpYyBTZXJ2ZXIgQXV0aGVudGljYXRpb24gUm9vdCBSNDYw
HhcNMjEwMzIyMDAwMDAwWhcNMzYwMzIxMjM1OTU5WjBgMQswCQYDVQQGEwJHQjEY
MBYGA1UEChMPU2VjdGlnbyBMaW1pdGVkMTcwNQYDVQQDEy5TZWN0aWdvIFB1Ymxp
YyBTZXJ2ZXIgQXV0aGVudGljYXRpb24gQ0EgRFYgUjM2MIIBojANBgkqhkiG9w0B
AQEFAAOCAY8AMIIBigKCAYEAljZf2HIz7+SPUPQCQObZYcrxLTHYdf1ZtMRe7Yeq
RPSwygz16qJ9cAWtWNTcuICc++p8Dct7zNGxCpqmEtqifO7NvuB5dEVexXn9RFFH
12Hm+NtPRQgXIFjx6MSJcNWuVO3XGE57L1mHlcQYj+g4hny90aFh2SCZCDEVkAja
EMMfYPKuCjHuuF+bzHFb/9gV8P9+ekcHENF2nR1efGWSKwnfG5RawlkaQDpRtZTm
M64TIsv/r7cyFO4nSjs1jLdXYdz5q3a4L0NoabZfbdxVb+CUEHfB0bpulZQtH1Rv
38e/lIdP7OTTIlZh6OYL6NhxP8So0/sht/4J9mqIGxRFc0/pC8suja+wcIUna0HB
pXKfXTKpzgis+zmXDL06ASJf5E4A2/m+Hp6b84sfPAwQ766rI65mh50S0Di9E3Pn
2WcaJc+PILsBmYpgtmgWTR9eV9otfKRUBfzHUHcVgarub/XluEpRlTtZudU5xbFN
xx/DgMrXLUAPaI60fZ6wA+PTAgMBAAGjggGBMIIBfTAfBgNVHSMEGDAWgBRWc1hk
lfmSGrASKgRieaFAFYghSTAdBgNVHQ4EFgQUaMASFhgOr872h6YyV6NGUV3LBycw
DgYDVR0PAQH/BAQDAgGGMBIGA1UdEwEB/wQIMAYBAf8CAQAwHQYDVR0lBBYwFAYI
KwYBBQUHAwEGCCsGAQUFBwMCMBsGA1UdIAQUMBIwBgYEVR0gADAIBgZngQwBAgEw
VAYDVR0fBE0wSzBJoEegRYZDaHR0cDovL2NybC5zZWN0aWdvLmNvbS9TZWN0aWdv
UHVibGljU2VydmVyQXV0aGVudGljYXRpb25Sb290UjQ2LmNybDCBhAYIKwYBBQUH
AQEEeDB2ME8GCCsGAQUFBzAChkNodHRwOi8vY3J0LnNlY3RpZ28uY29tL1NlY3Rp
Z29QdWJsaWNTZXJ2ZXJBdXRoZW50aWNhdGlvblJvb3RSNDYucDdjMCMGCCsGAQUF
BzABhhdodHRwOi8vb2NzcC5zZWN0aWdvLmNvbTANBgkqhkiG9w0BAQwFAAOCAgEA
YtOC9Fy+TqECFw40IospI92kLGgoSZGPOSQXMBqmsGWZUQ7rux7cj1du6d9rD6C8
ze1B2eQjkrGkIL/OF1s7vSmgYVafsRoZd/IHUrkoQvX8FZwUsmPu7amgBfaY3g+d
q1x0jNGKb6I6Bzdl6LgMD9qxp+3i7GQOnd9J8LFSietY6Z4jUBzVoOoz8iAU84OF
h2HhAuiPw1ai0VnY38RTI+8kepGWVfGxfBWzwH9uIjeooIeaosVFvE8cmYUB4TSH
5dUyD0jHct2+8ceKEtIoFU/FfHq/mDaVnvcDCZXtIgitdMFQdMZaVehmObyhRdDD
4NQCs0gaI9AAgFj4L9QtkARzhQLNyRf87Kln+YU0lgCGr9HLg3rGO8q+Y4ppLsOd
unQZ6ZxPNGIfOApbPVf5hCe58EZwiWdHIMn9lPP6+F404y8NNugbQixBber+x536
WrZhFZLjEkhp7fFXf9r32rNPfb74X/U90Bdy4lzp3+X1ukh1BuMxA/EEhDoTOS3l
7ABvc7BYSQubQ2490OcdkIzUh3ZwDrakMVrbaTxUM2p24N6dB+ns2zptWCva6jzW
r8IWKIMxzxLPv5Kt3ePKcUdvkBU/smqujSczTzzSjIoR5QqQA6lN1ZRSnuHIWCvh
JEltkYnTAH41QJ6SAWO66GrrUESwN/cgZzL4JLEqz1Y=
-----END CERTIFICATE-----""",
        "8c54c334b66ba4e426772af4a3f9136c19a1aec729fdb28c535c07a5a4ef22e0"),
}


def pinned():
    """The intermediates whose DER matches its pin (a changed one is left out: its host then fails as before)."""
    out = []
    for _name, (text, sha) in INTERMEDIATES.items():
        if hashlib.sha256(ssl.PEM_cert_to_DER_cert(text)).hexdigest() == sha:
            out.append(text)
    return out


def context():
    """requests' own context (urllib3's default on certifi's list) with the pinned intermediates added. The partial-
    chain flag (requests sets it: an intermediate in the store counts as a trust anchor) is cleared here, so a chain
    through the added intermediates must still reach a root in certifi's list (G47 F3)."""
    ctx = create_urllib3_context()
    ctx.load_verify_locations(certifi.where())
    ctx.verify_flags &= ~getattr(ssl, "VERIFY_X509_PARTIAL_CHAIN", 0)
    for text in pinned():
        ctx.load_verify_locations(cadata=text)
    return ctx


class ChainAdapter(HTTPAdapter):
    """requests' adapter with context() for its connection pools."""

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = context()
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        kwargs["ssl_context"] = context()
        return super().proxy_manager_for(*args, **kwargs)


def mount(session, **adapter_kwargs):
    """session with ChainAdapter(**adapter_kwargs) for the two hosts only."""
    for host in HOSTS:
        session.mount(host, ChainAdapter(**adapter_kwargs))
    return session
