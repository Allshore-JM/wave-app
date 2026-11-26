"""Capture the plan-section-110 fixtures once from the real feeds (python -I tests/capture_buoys_pacific_fixtures.py).

Writes tests/fixtures/buoys_pacific/:
  gw_sites.xml              Greater Wellington's Hilltop SiteList, cut to its wave / buoy sites and two others
  gw_meas_southb.xml        MeasurementList of "Wellington Harbour at Baring Head Wave Buoy (South (B))"
  gw_meas_old.xml           MeasurementList of "Wellington Harbour at Baring Head Wave Buoy" (quiet since 2020)
  gw_<field>.xml            South (B)'s last day: Hsig, Tsig, Wave Direction, Hmax, water temperature (P1D)
  gw_old_hs.xml             the 2020 series' last day (a quiet site)
  tdc_sites.xml             Tasman's SiteList cut the same way
  tdc_meas_tuatea.xml       Tuatea's MeasurementList; tdc_<field>.xml its last day (Height3, period, direction, max)
  ecan.json                 Environment Canterbury's 1_Week wave data
  epa_wave-height.json, epa_wave-direction.json, epa_status.json   Puerto Arica's buoy (24 h)
  spc_list.xml              the WIS2 global cache's listing of SPC's wave-buoy files
  spc_<name>.bufr4          the three newest SPC files; spc_expected.json their eccodes decoding (when eccodes is here)
"""
import json
import os
import re
import time
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "fixtures", "buoys_pacific")
UA = {"User-Agent": "allshoresurf.com test fixtures"}
GW = "https://hilltop.gw.govt.nz/data.hts"
TDC = "https://envdata.tasman.govt.nz/data.hts"
SOUTHB = "Wellington Harbour at Baring Head Wave Buoy (South (B))"
OLD = "Wellington Harbour at Baring Head Wave Buoy"
TUATEA = "HY Tuatea Buoy at Tasman Bay"
SPC = ("https://wis2globalcache.s3.amazonaws.com/?list-type=2&max-keys=1000&prefix="
       "data/int-spc/data/core/ocean/surface-based-observations/wave-buoys/")


def get(url, headers=None):
    h = dict(UA)
    h.update(headers or {})
    with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=120) as r:
        return r.read()


def hilltop(base, **params):
    return get(base + "?Service=Hilltop&" + urllib.parse.urlencode(params, quote_via=urllib.parse.quote))


def cut_sites(xml, keep_other=2):
    """Keep the wave / buoy <Site> blocks and a couple of others (the parser's filter is under test)."""
    head, rest = xml.split("<Site ", 1)
    blocks = ["<Site " + b for b in rest.split("<Site ")]
    tail = ""
    if "</HilltopServer>" in blocks[-1]:
        last, tail = blocks[-1].split("</HilltopServer>", 1)
        blocks[-1], tail = last, "</HilltopServer>" + tail
    kept, others = [], 0
    for b in blocks:
        if re.search(r'Name="[^"]*(wave|buoy)', b, re.I):
            kept.append(b)
        elif others < keep_other:
            kept.append(b)
            others += 1
    return head + "".join(kept) + tail


def main():
    os.makedirs(OUT, exist_ok=True)

    def w(name, data):
        open(os.path.join(OUT, name), "wb").write(data if isinstance(data, bytes) else data.encode("utf-8"))

    w("gw_sites.xml", cut_sites(hilltop(GW, Request="SiteList", Location="LatLong").decode("utf-8")))
    w("gw_meas_southb.xml", hilltop(GW, Request="MeasurementList", Site=SOUTHB))
    w("gw_meas_old.xml", hilltop(GW, Request="MeasurementList", Site=OLD))
    for f, m in (("hs", "Significant Wave Height (Hsig)"), ("tsig", "Significant Wave Period (Tsig)"),
                 ("dir", "Wave Direction"), ("hmax", "Maximum Wave Height (Hmax)"), ("sst", "Water Temperature")):
        w("gw_%s.xml" % f, hilltop(GW, Request="GetData", Site=SOUTHB, Measurement=m, TimeInterval="P1D"))
        time.sleep(0.5)
    w("gw_old_hs.xml", hilltop(GW, Request="GetData", Site=OLD, Measurement="Significant Wave Height (Hsig)",
                               TimeInterval="P1D"))
    w("tdc_sites.xml", cut_sites(hilltop(TDC, Request="SiteList", Location="LatLong").decode("utf-8")))
    w("tdc_meas_tuatea.xml", hilltop(TDC, Request="MeasurementList", Site=TUATEA))
    for f, m in (("hs", "Height3"), ("period", "Average Wave Period"), ("dir", "Average Wave Direction"), ("hmax", "Max Wave Height")):
        w("tdc_%s.xml" % f, hilltop(TDC, Request="GetData", Site=TUATEA, Measurement=m, TimeInterval="P1D"))
        time.sleep(0.5)

    w("ecan.json", get("https://www.ecan.govt.nz/data/current-wave-data/WaveDirectionData/1_Week",
                       {"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"}))
    for a in ("wave-height", "wave-direction", "status"):
        w("epa_%s.json" % a, get("https://puertoarica.cl/simar_api.php?action=%s&range=24h" % a))
        time.sleep(0.5)

    listing = get(SPC)
    w("spc_list.xml", listing)
    keys = sorted(re.findall(rb"<Key>([^<]+)</Key>", listing))[-3:]
    expected = {}
    for k in keys:
        k = k.decode()
        name = k.rsplit("/", 1)[-1]
        body = get("https://wis2globalcache.s3.amazonaws.com/" + k)
        w("spc_" + name, body)
        try:
            import eccodes
            h = eccodes.codes_new_from_message(body)
            eccodes.codes_set(h, "unpack", 1)

            def one(key):
                try:
                    v = eccodes.codes_get(h, key)
                except Exception:
                    return None
                return None if v in (eccodes.CODES_MISSING_LONG, eccodes.CODES_MISSING_DOUBLE) else v
            expected[name] = {k2: one(k2) for k2 in (
                "#1#significantWaveHeight", "#1#spectralPeakWavePeriod", "#1#directionFromWhichDominantWavesAreComing",
                "latitude", "longitude", "year", "month", "day", "hour", "minute", "wigosLocalIdentifierCharacter")}
            eccodes.codes_release(h)
        except ImportError:
            pass
    if expected:
        json.dump(expected, open(os.path.join(OUT, "spc_expected.json"), "w", encoding="utf-8"), indent=1)
    print("wrote", OUT)


if __name__ == "__main__":
    main()
