# G79 adversarial review: Taiwan CWA tide forecasts (plan section 113 B)

Change: commit d3eab0e on feat/tides-113 (parent d47f90b). Reviewed 2026-11-27.

Tests: `python -m pytest -q tests/test_tide*.py` passes both without a key (221) and with a dummy `CWA_API_KEY` (230).

Evidence: probes ran against CWA's public daily file (no key; `Sent` 2026-11-28T00:07:03+08:00, 9,918,242 bytes, 266
places, 31,560 events). CWA's public API spec (`/apidoc/v1`) confirms that `LocationId`, `timeFrom` and `timeTo` exist
for F-A0021-001. The spec gives no schema for the 200 answer.

## Findings, most severe first

### G79-1 (P1): every CWA window starts with a blank stretch and a false "incomplete" note; after CWA's midnight rebuild, the current height is missing for up to ~6.5 h

- Where: `tide_providers/cwa.py:154-182` (fetch: CWA's month starts at today 00:00 Taipei);
  `tide_global.py:364-402` (one payload per UTC day); `static_ui/tides.js:298-300, 368-369` (hasGap and the note).
- Scenario, checked with the real file and the page's own `hasGap` / `heightAt` in node:
  - The day strip starts at local midnight (Asia/Taipei). CWA's first event comes 0 min to 6 h 32 min after midnight,
    and the half-cosine has no value before it.
  - So `hasGap` is true for all 266 places at all times, and the window says "The curve has gaps where the list of
    highs and lows is incomplete." That note is false here: nothing is missing, CWA simply starts at midnight. Each
    morning is also blank up to the first event.
  - A place first opened in a UTC day after CWA's rebuild (16:07 UTC onward) is fetched from the new file. For example,
    莒光 (09007030) fetched at 17:00 UTC: its curve starts at 23:00 UTC and `heightAt(now)` is null, so there is no
    now-dot and no current height.
  - That payload is kept until 00:00 UTC, so every visitor to that place sees the same gap. In Hawaii that is
    06:00-12:30 HST.
  - The payload also has 59 h of `None` at its start (curve start 12:00 UTC two days back).
- Fix:
  - Keep each CWA station's last good highs and lows across UTC days (a small dict in the service, or in the module
    keyed by code). Merge in the events that come before the new answer's first event, so the previous day's file
    covers the morning.
  - On a cold start, either draw the leading stretch from one mirrored extreme (drawing only, not listed in `hilo`), or
    have the client leave out the "incomplete" note for a leading gap of an official-cosine source.
  - Add a test: a window fetched at 16:30 UTC must have a height at now.

### G79-2 (P2): no switch of its own: merging turns CWA on in production, and the REST answer it relies on has not been seen live

- Where: `tide_providers/cwa.py:52-58` (`available()` = the shared `CWA_API_KEY`); `tide_global.py:70-95`.
- Scenario:
  - `CWA_API_KEY` is already set on both Render services for the CWA buoys and winds (`wind_keyed.py:4`). So the
    first deploy lists all 266 places.
  - Inside OFFICIAL_KM those places hide the TICON gauges `ticon:kaohsiung-340-twn-uhslc-fd` (67 m from 64000020) and
    `ticon:keelung-341-twn-uhslc-fd` (50 m from 10017050). Verified with `load()` run with and without the key.
  - The REST shape `records.TideForecasts[].Location` comes from third-party code, and `tests/fixtures/.../rest-I03800.json`
    is built by hand (it has an empty `note`).
  - If CWA's real answer is shaped differently, every Taiwan window answers "could not be read", and Taiwan's two real
    gauges are gone too.
  - Other features have a kill switch (`COASTAL_ALERTS=0`, `JET_OVERLAY=1`). This one can be turned off only by
    removing the key, which also stops the CWA buoys and winds.
- Fix:
  - Make `available()` need `CWA_TIDES=1` as well as the key.
  - Before setting it, make one live REST call with the key and check three things: the shape, that `LocationId`
    filters (the unfiltered answer is ~9.9 MB, over `TIDE_AGENCY_MAX_BYTES`), and that the answer carries no key.
  - Commit that answer, trimmed, as the REST fixture.

### G79-3 (P2): at three places `AboveChartDatum` does not match CWA's other two heights (up to 64 cm off)

- Where: `tide_providers/cwa.py:87-110` (`parse_events` reads `AboveChartDatum` only).
- Scenario:
  - At 10014070 (臺東縣東河鄉), I00900 (漁港金樽) and O01300 (金樽 coast), the gap `AboveChartDatum − AboveLocalMSL`
    should be fixed for a place. It is not: it runs from 83 to 147 cm.
  - `AboveTWVD − AboveLocalMSL` stays about 15 cm, so the MSL and TWVD fields agree with each other.
  - Same events, two answers: chart datum gives a range of 0.42-1.74 m (1.32 m), MSL gives −1.00 to +0.77 m (1.77 m).
  - The site shows a tide range about 25 % smaller than CWA's own MSL / TWVD numbers. The other 263 places have a fixed
    gap (spread of 3 cm or less).
- Fix:
  - In `build_station_list`, and again in `fetch`, flag any place where the spread of (CD − MSL) is over a few cm.
  - Then either use `AboveLocalMSL` plus that place's median CD − MSL gap, or leave the place out. Record the decision
    in the module's docstring.

### G79-4 (P2): wrong or unhelpful English names

- Where: `tools/tides/build_national.py:57-78` (cwa_names).
- Scenario: the 266 places have only 55 different English parts:
  - 50 are just "Harbour (…)", 17 "Beach (…)", 16 "Dive spot (…)", 15 "Fishing spot (…)" and 11 "Coast (…)".
  - Townships carry only their county: "Pingtung County (…)" ×11, "Kaohsiung City (…)" ×11, "Taitung County (…)" ×10.
  - A visitor who does not read Chinese cannot tell them apart.
- The GeoNames "town within 6 km" rule also picks the wrong place (checked with the cached cities1000):

  | Shown | Actual place | Why it is wrong |
  |---|---|---|
  | "Shenzhen harbour (永安)" | I04000, Yong'an fishing harbour, Xinwu, Taoyuan | GeoNames village "Shenzhen" 3.7 km away; reads as the mainland city |
  | "Jiufen harbour (望海巷)" | I02800, in Keelung City | Jiufen at 5.3 km beat Keelung at 6.2 km |
  | "Jiufen fishing spot (深澳)" | B01200, Shen'ao, Ruifang | Not Jiufen |
  | "Toufen beach (崎頂)" | A00600, Qiding, Zhunan, Miaoli | Not Toufen |
  | "Jieshoucun harbour (福澳)" | I06800, Fu'ao, Nangan's main harbour | Jieshoucun at 1.06 km beat Nangan at 1.32 km |

- Fix:
  - Townships: the 8-digit LocationId is the official township code, so take the English township name from the
    Ministry of the Interior's township code table ("Donggang Township, Pingtung County (屏東縣東港鎮)").
  - Other places: name the township they lie in instead of a GeoNames village, or use a short hand-checked table of
    the 160 romanised names.
  - Add a unit test of cwa_names with a fake Grid.

### G79-5 (P3): the "never surf" rule is enforced only after the fact

- Where: `tools/tides/build_national.py:69-73`; `tests/test_tide_cwa.py:168-177`.
- Scenario:
  - cwa_names removes 衝浪 only when a name starts with it. Any other place of CWA's whose name contains 衝浪 anywhere
    else keeps it (say, a new "金樽衝浪區"), and the builder still writes the snapshot.
  - Only `test_the_committed_snapshot_lists_taiwan` catches it, after the rebuild.
  - cwa_names has no unit test of its own.
- Fix: the builder raises (SystemExit) on any CWA name containing 衝浪 or "surf" (case-insensitive) after naming. Add
  the unit test from G79-4. The fixtures themselves are clean: no 衝浪 and no surf.

### G79-6 (P3): "once a place per UTC day" is not a bound; the 20,000-a-day quota is shared with the CWA buoys and winds

- Where: `tide_global.py:282` (`max_entries=256`); `app.py:1243-1244`; `tide_providers/cwa.py:20`.
- Scenario:
  - The window cache is one LRU of 256 entries for every worldwide station. The 266 CWA places alone exceed it, so
    evicted places are fetched again.
  - A failing place is asked again every 60 s while a window retries.
  - The only bound is the bucket, 30 a minute (≈43,200 a day) per service. Both services use one key, and the key
    also carries O-A0003-001 every 10 min and the buoy calls.
  - Going over CWA's daily limit would stop the Taiwan buoys and winds for the rest of the day, not just the tides.
- Fix: put a daily cap on the cwa bucket (for example 4,000 calls a day per service), or count calls in the module,
  and correct the docstring and README.

### G79-7 (P3): the datum label suggests heights never go below zero

- Where: `tide_providers/cwa.py:39`.
- Scenario: 424 of 31,560 events read below chart datum (lowest −0.20 m, 八里 65000230), while the window says
  "chart datum (lowest low water)".
- Fix: say so, as JMA's label does: "chart datum (CWA; the lowest tides can read below zero)".

### G79-8 (P3): the key stays in the suppressed exception context

- Where: `tide_providers/cwa.py:166-169`; `tests/test_tide_cwa.py:132`.
- Scenario:
  - `raise TideFetchError(...) from None` clears `__cause__` but leaves the original exception, with the URL and the
    key, in `__context__` (verified).
  - It stays hidden only because traceback printing respects `__suppress_context__`, and `_official` keeps only the
    type name.
  - A future handler or error reporter that walks `__context__` would print the key. The test checks `__cause__` only.
- Fix: keep the type name inside the `except`, then raise after the block, so `__context__` is None. Assert
  `e.value.__context__ is None` in the test.

### G79-9 (P3): places stacked on the same spot

- Where: `tide_national.json` (cwa rows).
- Scenario: four pairs lie less than 50 m apart, and one map icon hides the other:
  - 10014010 / I05900
  - 10015080 / I06200
  - A01000 / B01100 (identical coordinates)
  - N00100 / O00200 (identical coordinates)
- Fix: in the builder, merge places with identical coordinates and identical series, keeping the more specific name.

## Checked and found sound

- **Key handling:** the key appears only in the request URL. Errors carry only a type name or a place code. The
  window link is CWA's data-set page (it loads, 200). The credit carries no key. The station list is built from CWA's
  public file with no key. The snapshot holds no key. urllib3 logs are not at DEBUG.
- **Without a key:** the CWA places are not listed. TICON dedup then uses only the listed agencies, so the Kaohsiung
  and Keelung gauges stay. The FES model points lose nothing: no forecast point depends on a CWA place for its 10-km
  test. The station list is revalidated on every load (no-cache), so an old list does not outlive a restart.
- **Parsing, checked against the real file:** every LocationId is a string. There are no unsorted or duplicate times
  and no two highs or two lows in a row. No gap between events exceeds 20 h. Each place has 97-124 events, against
  `MIN_EVENTS` = 20. A wrong place in the answer, HTML, `success: false` and non-dict answers are all refused.
- **Time:** Taipei time has no daylight saving. `Asia/Taipei` is right for Kinmen and Matsu. The window's end is
  covered by CWA's 32 days.
- **Other providers and the snapshot loader** are unaffected. `tz` from a provider is used only by CWA. The token
  bucket for cwa is created from PROVIDERS. `MAX_REQUESTS = 1` holds.

## Fix round 1 (2026-11-27)

| Finding | Fix | Test |
|---|---|---|
| G79-1 | `cwa._with_held`: each place's earlier highs and lows (kept 3 days, in memory) are put in front of CWA's new month, so after the first fetch the window starts filled and has a height at now; the window (static_ui/tides.js `edges`) tells a late start / early end apart from a gap in the list ("The predictions start partway through the first day shown."), so the false "incomplete" note is gone. Cold start after a restart: the curve starts at CWA's first extreme (no invented curve). UI 1.32.1. | `test_g79_1_...` (17:00 UTC after the rebuild: a height at now), tests/ui/tides.test.js "section 113 B (G79-1)" |
| G79-2 | `CWA_TIDES=0` turns the tides off alone (default on). The live REST shape was confirmed on the test site with the real key before this round: 5 places' windows parsed, values equal CWA's own daily file (09007010, 10016010, A00200, I03800). The TICON gauges at Kaohsiung / Keelung give way to CWA's places by the existing official-over-gauge rule (3 km), as for every agency. | `test_g79_2_...` |
| G79-3 | Places whose AboveChartDatum - AboveLocalMSL spreads more than 5 cm (10014070, I00900, O01300) get `msl_gap_cm` (the median, 114) in the snapshot; their heights are AboveLocalMSL + that gap. | `test_g79_3_...` |
| G79-4 | No GeoNames guess: townships by their county's English name; other places "Type (own name), <county of the nearest CWA township>", e.g. "Harbour (永安), Taoyuan City", "Beach (崎頂), Miaoli County", "Harbour (福澳), Lienchiang County". | `test_g79_4_5_...` |
| G79-5 | The builder stops on any name with 衝浪 or "surf". | `test_g79_4_5_...` |
| G79-6 | At most 2,000 CWA requests a UTC day (`_spend`), checked before the request. | `test_g79_6_...` |
| G79-7 | DATUM "chart datum (lowest low water; the lowest tides can read below zero)". | `test_the_window_...` |
| G79-8 | The error is raised after the `except` block: no `__cause__`, no `__context__`. | `test_fetch_needs_the_key_...` |
| G79-9 | Places under 50 m apart: the more official one (township, harbour, beach, fishing, dive, coast): 266 -> 262. | `test_g79_9_one_place_per_spot`, `test_g79_9_the_station_list_...` |

Mutants (scratchpad mut79.py): 9 of 9 caught. Tide tests 236 + UI 658 + page/overlay/UI-module pass.
