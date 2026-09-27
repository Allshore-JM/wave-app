# G19 — wind gaps, resize from every side, wave-height and wind range (plan section 27)

Two fresh-context reviewers at HIGH effort, 2026-09-27.

- **Reviewer A** covered the site release `e6b7710..f965af2` on `feat/wind-resize-range`. That is the SWAN wind back-fill, the
  blank-wind hardening, the resize edges and the client scale for a 0-60 ft legend. A checked the code, ran 14 mutants,
  used the test site at 1580x900 and the phone preset, and compared the result with production.
- **Reviewer B** covered the frame job on `feat/range-job` (`fb74849` + `0cddd27`): wave height stored to 75 ft with a
  0-60 ft legend, and wind stored to 120 kt. B re-encoded the live run's f000 and f213 from the same NOAA records.

**Result: 0 P0, 1 P1, 2 P2, 10 P3.** The P1 and one P2 are fixed. The other P2 is a trade-off for the owner.

## Reviewer A: site

| # | Sev | Finding | Outcome |
|---|---|---|---|
| A P2-1 | P2 | The server shortened its cache only when every row lacked wind. A SWAN forecast whose back-fill failed kept its early rows blank for the full 30 min, so the client's 60 s retry got the same answer each time. | **Fixed** @ bf66ce6: any row without wind gets the short TTL, which is the same test as `wind_complete`. |
| A P3-1 | P3 | When maximised in Table view, a 10 px white strip sat beside the table. The body margin was dropped in max mode but the width cap still counted it. | **Fixed** @ bf66ce6: the margin stays in max mode. |
| A P3-2 | P3 | At the viewport edge, the keyboard grip pushed the window left, while the pointer grip stopped at the edge. | **Fixed** @ bf66ce6: the keyboard grip goes through `resizeGeometry` like the pointer. |
| A P3-3 | P3 | The `render=full` seed did not carry `wind_complete`, so a gappy inline forecast stayed 10 min in the client cache. | **Fixed** @ bf66ce6. |
| A P3-4 | P3 | The wind cache cap of 64 evicted the 6 h back-fill entries first. | **Fixed** @ bf66ce6: the cap is now 128. |
| A P3-5 | P3 | `_swan_first_row_utc` reads the first token rather than the parser's Time column. There is no fallback when the older cycle is missing. | **Accepted.** Time is column 0 today. A missing cycle leaves the old fail-soft behaviour. |

A also confirmed the following:
- The cycle arithmetic and NOMADS retention are correct. So are the singleflight and the merge order, where the newest run wins.
- 51201 SWAN has 0 blank wind rows, against 24 on production. The 26 Sep 12Z row matches the gfs.20260926/12 spec.
- GFS payloads are byte-identical to production apart from the new key.
- All seven edges and the grip on both windows are the top element under the pointer.
- On phones there are no edges.
- With today's runs the legend, colour table and ticks are identical to production. A simulated switch to a [0, 18.288] manifest shows the 60 ft scale.
- The 5 surviving mutants are equivalent in practice.

## Reviewer B: frame job

| # | Sev | Finding | Outcome |
|---|---|---|---|
| B P1 | P1 | Nothing stopped a run built under the old range from being rebuilt under the new one. This could happen with `--force`, with a lost pointer, or with a hand-moved pointer. The frames are cached for a year, so they would decode 1.524x too high, or 0.656x too low. | **Fixed** @ 713b0a6. `fill_guard` also compares each field's (lo, hi, circular) from the manifest's `fields` block. It repairs the pointer when the old manifest is usable, and otherwise refuses, `--force` included. A manifest without a `fields` block keeps the fill-only rule. There is a test for this. |
| B P2 | P2 | The 2-ft contour lines wander about twice as often in flat seas, because a storage step of 0.09 m replaces 0.059 m. Line cells shifted by more than half a cell rose from 3.6 % to 6.1 % at f000, and the p99 shift from 0.9 to 1.5 cells. | **Accepted, reported to the owner.** This is the price of storing heights up to 75 ft in 8 bits. The mitigation would be client-side, with wider smoothing where the gradient is low, and can be a later small release if the owner sees it on the map. |
| B P3-1 | P3 | Colour terraces are coarser: 11.7 RGB per colour-table entry over 0-3 m, up from 7.7. | **Accepted.** This is faint at 0.65 opacity. |
| B P3-2 | P3 | The live client 2.12.2 shows a new-range run with its colours stretched by 1.524. Readouts stay correct. | **Handled by the order:** the site release goes first, then the job-only merge. |
| B P3-3 | P3 | The run summary printed SI values without units. | **Fixed** @ 713b0a6: peaks are shown in ft / s / kt. |
| B P3-4 | P3 | `clamped_high` counts values above `hi`, but code 255 also covers the last half step. | **Accepted.** This predates the change and is informational. |
| B P3-5 | P3 | The new summary line has no test. | **Accepted.** The line is informational only. |

B also confirmed the following:
- The step is exactly 0.09 m, which is 9 GRIB units. The decode error is at most 0.04 m.
- The run's 19.09 m peak is stored, and clamped cells drop from 75 to 0.
- The wind error over 0-20 m/s is at most 0.27 mph, which the whole-mph readout hides.
- 18.288 and 22.86 serialise exactly and match the client's `SCALES.hs.top`.
- The manifest stays under 20 KB.
- Old runs keep their own scale in client 2.13.0.
- 12 of 12 unit-slip mutants were caught.

## Afterwards

- The owner's logo replacement landed on the site branch @ 4ad8a89 (UI 1.9.7) before the merge. Its golden recapture is @ e32fb9b.
- Order of the releases: first the site merge (tag `prod-pre-resize` @ e6b7710), then the job-only merge (tag `prod-pre-range`).
- After the job merge, the first new run's manifest must show hs hi 22.86 with legend [0, 18.288], and wind hi 61.73 m/s.
