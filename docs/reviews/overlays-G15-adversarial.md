# Overlays — G15 adversarial review (the simpler panel) — 2026-09-26

Owner request: no opacity slider (wave height and period fixed around 65-70 %, wind 100 %), the animation on for every
overlay and the contours on for height and period with the checkboxes removed, and no +/- zoom buttons ("keeps it simple
with less clutter").

- Asset 2.12.0 @ f2639b0 on `feat/overlays-simplify` (off production 294819d): `FIXED_OPACITY` hs/tp 0.65, wind 1;
  contours and animation always on (old tab saves ignored, nothing written); the Opacity / Contours / Animation row and its
  CSS removed; the gated template block removes `map.zoomControl` (flag-off page unchanged; wheel, pinch, double-click and
  keyboard still zoom, the Home button stays); reduced motion still animates nothing.
- Fixes: asset 2.12.1 @ 0b45b07 and 2.12.2 @ 3bd3b49 (the phone legend); test @ e6b4ad5.

## G15 — one fresh-context reviewer at high effort (Opus 5.5), code + test site: 0 P0, 0 P1, 1 P2, 4 P3

| # | Sev | Finding | Outcome |
|---|---|---|---|
| P2-1 | P2 | With the animation always on, every first picture (mount, field switch) waited for its direction frame: first wind frame 126 KB + wdir 152 KB sharing the link (~0.6 s -> ~1.4 s at 1.6 Mbps); a stalled direction held it up to the watchdog. | The first picture of a field (nothing of it drawn yet) is fetched alone and drawn at once; its direction follows and the particles join when it lands; later steps still change picture and particles together. Five scheduler tests updated to the rule. Test site: `half/wind/f008.png` alone, then `half/wdir/f008.png` after the landing; drawn at 984 ms from the pick (incl. module and coast load), particles 28 ms later. |
| P3-1 | P3 | "Nothing is saved" not pinned (re-adding `save()` to the setters survived). | The test calls the setters with true / false / true and checks the stored old values are untouched. |
| P3-2 | P3 | Dead CSS, an empty test loop, a stale "opacity" comment; old tabs keep their old keys (harmless, ignored). | Removed; old keys left (ignored). |
| P3-3 | P3 | README data figures described the loop without direction frames. | Updated: direction frames now always add to the loop (about 53 / 85 MB desktop, 30-62 MB phones, 59 / 119 MB wind per 209-frame loop; frames immutable). |
| P3-4 | P3 | Phones: the 66-px details box showed the transport, the timeline and a Valid line repeating the header; the legend below the fold. | The legend sits right under the timeline with a tighter bar in the sheet (asset 2.12.2): 43-64 px of the 66-px box, labels included. |

Verified by the reviewer: no leftover references (setOpacity, opacityWind, the row's `ui` fields); the panel clamp,
`_layoutSheet`, `_stackWidth`/`_stackHeight` work with the Home-only corner (43x43); fixed opacity live (hs/tp 0.65 over
the imagery, wind 1 multiplied into the relief; once per field change); always-on contours and particles on all fields;
old "off" saves ignored; Metric refresh; in-flight <= 2; reduced motion and runs without direction data covered by
existing tests; z3 playback with contours and particles 59-61 fps (long tasks ~51 ms, the accepted cost); no zoom control
left, `setPosition` runs before the removal, no CSS targets it, keyboard/wheel/pinch/double-click zoom work, Home menu
works, the phone sheet (x 62-362) clear of Home (x 23-56); desktop details 127/127 px, no dead space; no console errors;
mutations (contours / anim honouring saves, wind 0.9, tp 1, the template removal) all caught.

After the fixes: Node 133, pytest (flag) 10; asset 2.12.2 on the test site.
