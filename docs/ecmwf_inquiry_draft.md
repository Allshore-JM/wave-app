# ECMWF inquiry draft — swell-partition parameters for a non-commercial website

**Where to send:** ECMWF Support Portal — https://support.ecmwf.int (create a "Data" request;
a free ECMWF web account is needed to open a ticket). Alternative: servicedesk@ecmwf.int.

**Suggested subject:** Real-time access to IFS/WAM swell-partition parameters (140121–140129)
for a free non-commercial website

---

Hello,

I run allshoresurf.com, a free, non-commercial surf-forecast website (no ads, subscriptions,
or paid products). It currently displays NOAA GFS-Wave and PacIOOS SWAN point forecasts with
per-swell-partition detail (height, period, direction for each swell train), and I would like
to add the ECMWF wave model (IFS/WAM) as an additional option in the same format.

I understand that as of 1 October 2025 the entire ECMWF Real-time Catalogue is licensed
CC-BY-4.0 with no information cost. However, the parameters I need — the swell-partition set,
GRIB parameter IDs 140121–140129 (swh1/mwd1/mwp1, swh2/mwd2/mwp2, swh3/mwd3/mwp3) — do not
appear in the anonymous open-data distribution (data.ecmwf.int and its cloud mirrors currently
carry the 0.25° wave stream with combined statistics and period-band heights only).

My questions:

1. Is there a published timeline for adding the swell-partition parameters (140121–140129) to
   the open-data distribution on data.ecmwf.int — for example as part of the planned 0.1°
   open-data expansion later in 2026?

2. If they will not be on the open portal, what would be the recommended way for a small
   non-commercial user to receive them in real time — e.g. an ECPDS dissemination feed — and
   what delivery/service charges (if any) would apply? My requirement is modest: the 9
   partition parameters (plus combined significant wave height), global, 0.25° is fine,
   deterministic HRES only, from the 00z/12z cycles.

3. If a dissemination feed is the route, what is the application process and typical setup
   timeline for a non-Member-State individual/small organisation?

Attribution to ECMWF (CC-BY-4.0) would of course be displayed alongside the data.

Thank you,
Josh Murphree
joshmurphree@gmail.com — allshoresurf.com

---

**Context notes for future reference (2026-07-03 research):**
- Full Real-time Catalogue open (CC-BY-4.0, no information cost) since 2025-10-01.
- Anonymous portal (data.ecmwf.int) today: `ifs/0p25` only; wave stream params =
  swh, mwd, mwp, pp1d, mp2, wmb, cdww + h1012/h1214/h1417/h1721/h2125/h2530 (height-only
  period bands). No per-partition direction/period. AIFS wave stream identical; AWS mirror
  identical; no opencharts data API.
- 0.1° open tranche announced for "later in 2026" (param list unannounced).
- The live canary test `test_openmeteo_ecmwf_bulk_only_live` (RUN_LIVE=1) fails the day
  Open-Meteo's ecmwf_wam025 starts serving swell components — the automatic signal that
  partitions have reached the open data and ECMWF can be added as a fourth model.
