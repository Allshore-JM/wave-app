"""Capture the plan-section-72 fixtures once from the real feeds (python -I tests/capture_buoys_world_fixtures.py).

Writes tests/fixtures/buoys_world/:
  cmems_index.txt           index_latest.txt rows of a few platforms: NIOT 2300453 (VGHS), an INCOIS drifter, Italy's
                            RON 6100217 (VGHS), Meteo-France's relay 6200001 (VGHS, broken VTPK), a Saildrone, a
                            Pacific platform outside every box, a national VHM0 platform (6200064)
  GL_TS_MO_2300453_*.nc     NIOT's file for the newest day and the day before
  GL_TS_MO_6200001.nc       the Meteo-France relay's newest file
  GL_TS_DB_7810896.nc       the drifter's newest file
  simcosta_list.json        SiMCosta's platform list (whole)
  simcosta_12.json          RJ-4's last 24 h; simcosta_518.json CDRJ-1's last 24 h (sea / swell split)
  kma_<var>.json            KMA's WIS2 node: each wave variable for three stations over 26 h
"""
import json
import os
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "fixtures", "buoys_world")
UA = {"User-Agent": "allshoresurf.com test fixtures"}
S3 = "https://s3.waw3-1.cloudferro.com/mdl-native-01/native/"
DS = "INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/"
KMA = ("https://wis2box.kma.go.kr/oapi/collections/"
       "urn:wmo:md:kr-kma:core.surface-based-observations.moored-buoys-and-moorings/items")
PLATFORMS = ("GL_TS_MO_2300453", "GL_TS_DB_7810896", "GL_TS_MO_6100217", "GL_TS_MO_6200001", "GL_TS_SD_1801600",
             "GL_TS_MO_52215", "GL_TS_MO_6200064")
KMA_STATIONS = ("0-410-0-21229", "0-22000-0-2200307", "0-410-0-22101")


def get(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=120) as r:
        return r.read()


def main():
    os.makedirs(OUT, exist_ok=True)
    index = get(S3 + DS + "index_latest.txt").decode("utf-8").splitlines()
    keep = [ln for ln in index if ln.startswith("#")]
    newest = {}
    for ln in index:
        if ln.startswith("#"):
            continue
        name = ln.split(",")[1].split("/")[-1]
        pid = name.rsplit("_", 1)[0]
        if pid in PLATFORMS:
            keep.append(ln)
            if ln.split(",")[7] > newest.get(pid, ("", ""))[0]:
                newest[pid] = (ln.split(",")[7], ln.split(",")[1])
    open(os.path.join(OUT, "cmems_index.txt"), "w", encoding="utf-8", newline="\n").write("\n".join(keep) + "\n")
    for pid in ("GL_TS_MO_2300453", "GL_TS_MO_6200001", "GL_TS_DB_7810896"):
        fn = newest[pid][1]
        open(os.path.join(OUT, fn.split("/")[-1]), "wb").write(get(S3 + fn))
        time.sleep(0.5)
    fn = newest["GL_TS_MO_2300453"][1]
    day = fn.split("/")[-2]
    prev = time.strftime("%Y%m%d", time.gmtime(time.mktime(time.strptime(day, "%Y%m%d")) - 43200))
    prev_fn = fn.replace("/%s/" % day, "/%s/" % prev).replace("_%s.nc" % day, "_%s.nc" % prev)
    open(os.path.join(OUT, prev_fn.split("/")[-1]), "wb").write(get(S3 + prev_fn))

    open(os.path.join(OUT, "simcosta_list.json"), "wb").write(get("https://simcosta.furg.br/api/equipamentos/"))
    now = int(time.time())
    fields = "Hsig,HM0,Tp,Avg_Wv_Dir_N,Hsig_sea,Tp_sea,Dp_sea,Hsig_swell,Tp_swell,Dp_swell"
    for bid in (12, 518):
        url = ("https://simcosta.furg.br/api/intrans_data?boiaID=%d&type=json&time1=%d&time2=%d&params=%s"
               % (bid, now - 86400, now, fields))
        open(os.path.join(OUT, "simcosta_%d.json" % bid), "wb").write(get(url))
        time.sleep(1)

    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 26 * 3600))
    for var in ("significant_wave_height", "spectral_peak_wave_period", "average_wave_period",
                "direction_from_which_dominant_waves_are_coming", "maximum_wave_height", "sea_surface_temperature"):
        doc = json.loads(get("%s?f=json&name=%s&datetime=%s/..&limit=5000" % (KMA, var, since)))
        doc["features"] = [f for f in doc["features"]
                           if f["properties"].get("wigos_station_identifier") in KMA_STATIONS]
        doc["numberMatched"] = doc["numberReturned"] = len(doc["features"])
        doc["links"] = []
        json.dump(doc, open(os.path.join(OUT, "kma_%s.json" % var), "w", encoding="utf-8"))
        time.sleep(1)
    print("wrote", OUT)


if __name__ == "__main__":
    main()
