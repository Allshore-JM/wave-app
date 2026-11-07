"""Capture the Japan and Taiwan radar fixtures (plan section 86) from the real sources (network, read-only, paced):

    python tests/capture_radar2_fixtures.py

tests/fixtures/radar/jma_tile_rain.png     a real JMA tile holding rain, byte for byte
tests/fixtures/radar/jma_tile_dry.png      the 334-byte transparent picture JMA answers for a dry tile
tests/fixtures/radar/jma_outline.geojson.gz  JMA's data-area file, as it comes on the wire
tests/fixtures/radar/cwa_head.xml          a real CWA file with its 811,401 values left out (the tests put their own in)
"""
import json
import os
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "fixtures", "radar")
BASE = "https://www.jma.go.jp/bosai/jmatile/data/nowc"
CWA = "https://cwaopendata.s3.ap-northeast-1.amazonaws.com"
UA = {"User-Agent": "allshoresurf.com radar (https://allshoresurf.com)"}


def get(url):
    return urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60).read()


def save(name, body):
    with open(os.path.join(OUT, name), "wb") as fh:
        fh.write(body)
    print(name, len(body))


def main():
    frames = json.loads(get(BASE + "/targetTimes_N1.json"))
    stamp = max(f["validtime"] for f in frames if f["validtime"] == f["basetime"])
    rain = dry = None
    for x in range(53, 59):
        for y in range(22, 29):
            body = get(f"{BASE}/{stamp}/none/{stamp}/surf/hrpns/6/{x}/{y}.png")
            time.sleep(0.5)
            if len(body) == 334:
                dry = dry or body
            elif rain is None or len(body) > len(rain):
                rain = body
    save("jma_tile_rain.png", rain)
    save("jma_tile_dry.png", dry)
    save("jma_outline.geojson.gz", get(f"{BASE}/{stamp}/none/{stamp}/surf/hrpns_nd/data.geojson"))
    key = f"{int(stamp[:12]) // 10 * 10}"
    from datetime import datetime, timedelta
    when = datetime.strptime(key, "%Y%m%d%H%M") + timedelta(hours=8) - timedelta(minutes=30)
    body = get(f"{CWA}/history/Observation/{when:%Y%m%d%H%M}compref_mosaic.xml")
    i, j = body.index(b"<content>") + 9, body.index(b"</content>")
    save("cwa_head.xml", body[:i] + body[j:])


if __name__ == "__main__":
    main()
