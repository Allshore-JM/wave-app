# Overlays — G7 adversarial review (smooth peak period) — 2026-09-25

Gate G7 of plan section 20, before the job-only merge. Owner request: the peak-period overlay should be smooth like
wave height, at the same resolution. Both fields already use the same 0.25-degree frames and the same half/full
switching; the job's manifest declared tp `interpolation: "nearest"`, so the browser drew one flat block per ~28 km
cell. Commit eb40481 on `feat/overlays-tp-smooth` (one commit on production a0f4afb) declares tp `bilinear`
(`tools/model_frames/encode.py`); the live client (asset 2.6.7) is unchanged and draws tp through the same sampler as
wave height. The coastal fill (tp keeps nearest-model values at filled nodes) and the re-publish guard are unchanged.

Author's local check with the live client (run 2026092418 at +9 h, filled frames, bilinear manifest): tp leaves the
same water pixels uncoloured as wave height (0 in the Hawaii, San Francisco and Tokyo views; 4,406 in Bergen's inner
fjords), colours no land, readout = drawn pixel at 133,128 samples, full redraw 111 ms against 105 ms for wave height.

One fresh-context reviewer at HIGH effort (Opus 5.5), no access to the author's reasoning.

## Findings

Report: scratch `g7-report.md`. **0 P0, 0 P1, 1 P2, 3 P3**; verdict: fit for the job-only merge, with the owner's
sign-off on the regime-edge look.

| # | Sev | Finding | Outcome |
|---|---|---|---|
| P2-1 | P2 | Bilinear peak period invents in-between periods at swell-regime edges: 1.5–1.6 % of drawn samples are more than 1 s from every surrounding model node (3.6–4.0 % along coasts; worst about 10 s, e.g. nodes of 21.9 s and 1.6 s shown as 11.5 s). Because the Tp ramp is multi-hue, each edge gets a thin outline in the middle colours, about one cell wide. The plan already accepted the in-between values. | Owner decision (below). The alternative is regime-aware blending in the client (blend only where the neighbours are within about 3 s; otherwise nearest), which needs a client release and its own gate. |
| P3-1 | P3 | No client test drives tp through the bilinear branch from a manifest hint, or across an Update from a nearest-hint run to a bilinear one (the reviewer ran that flow by hand: correct). | With the next client release. |
| P3-2 | P3 | Stale wording: the sampler comment in `overlay.js` still says Tp is nearest; the README tied the change to a date, but it follows the run (older runs keep `nearest` until they age out, about a day). | README fixed (6a9c21c); the client comment with the next client release. |
| P3-3 | P3 | The nearest coastal fill for Tp leaves more sharp seams through a bilinear display than a mean fill would (pairs over 8 s apart: 0.76–0.94 % against 0.12–0.31 %). | Keep nearest: a mean fill would spread invented periods up to 4 rings (the G6 P2) and need `FILL_VERSION` 3; the two look nearly the same through the display. |

Verified correct by the reviewer: the hint reaches the manifest (6,017 B) and the live client accepts it; `_nearest` is
recomputed on every frame, so nothing carries across fields, runs or Update; hs and tp missing masks identical in all 81
steps of run 2026092418 and in sampled frames of three other runs; the live client drew the same pixels for hs and tp in
7 views (0 mismatches in 11.3 M); smooth coverage is a superset of the old; readout = drawn at 455,175 samples; the
"≤ lo"/"≥ hi" labels cannot fire (codes 1 and 255 never occur); legend unchanged; half frames and wind unaffected; the
fill guard and `fill_key` unaffected; a `--force` rebuild writes only a new manifest; tp costs what hs costs per tile
(0.94 against 1.06 ms; nearest was 0.72); 49 job tests and 46 client tests pass; `git revert` of eb40481 is safe.

## Owner decision

2026-09-25 ~02:30 UTC, after seeing the reviewer's regime-edge sheet: **merge as is** — peak period smooth everywhere
like wave height, thin outlines at swell-regime edges accepted. **G7 closed.**
