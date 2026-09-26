# Overlays — G13 adversarial review (the full model horizon, plan section 22) — 2026-09-26

The owner asked for the overlays to cover the models' full output. NOAA's GFS-Wave 0.25 and GFS pgrb2 0.25 run hourly to
+120 h, then every 3 h to +384 h. The site's forecast table already shows all 385 hourly rows. Owner decisions: hourly
frames to +120 h and 3-hourly to +384 h (209 frames, was 81 to +240 h); a run goes live only when all 16 days are
published; every frame is played; the timeline is laid out by time; the panel's run line says when the run went live and
when the next update is expected, in the viewer's own time zone.

- Part 1 (job) @ 067c003 on `feat/overlays-384`: `fetch.STEPS` hourly 0–120 + 3-hourly 123–384; the manifest's
  `frame_schedule` (the unread `frame_hours` removed); tests incl. the LIVE client (2.9.5) playing a 209-frame run.
- Part 2 (client, asset 2.10.0) @ c1634b3 on `feat/overlays-timeline`: the timeline in hours with direction-aware
  snapping; the run line "live since … · next update about …". Reviewed as G13b on the test site once a real 209-frame
  run is live.

## G13a — the job (one fresh-context reviewer at high effort, Opus 5.5): 0 P0, 0 P1, 0 P2, 10 P3

Nothing lets a partial or wrong run go live, stops a run from publishing, or changes published bytes.

| # | Finding | Outcome |
|---|---|---|
| P3-1 | A build killed by the 60-min timeout leaves no failure record, so every tick would rebuild and be killed again (a 209-step build is estimated at 10–15 min, ~29 min worst case). | `timeout-minutes: 120`. |
| P3-2 | A listed object that cannot be fetched (NotReady) makes the next tick re-upload every earlier frame with no backoff (~1,880 writes per tick near f384; pre-existing, now 2.6× larger). | Accepted for now (rare; within the free tier); a skip-stored-keys follow-up if it is ever seen. |
| P3-3 | Pointer repair never accepts a complete 81-frame manifest, so a lost pointer plus a fill change leaves no pointer until the next cycle. | Accepted (very narrow); pinned by a test. |
| P3-4 | No transition test in the repo. | The reviewer's four probes added as tests: a live 81-frame run of the same cycle is left alone; a lost pointer over an 81-frame run of the same fill is rebuilt at 209; another fill is refused; `--force` rebuilds all 209. |
| P3-5 | The plan's optional forced rebuild of the live 81-frame cycle rewrites 729 year-cached immutable keys (safe only if byte-identical). | Not done: the natural switch takes at most one cycle. |
| P3-6 | Stale texts: the workflow's completion window, publish.py's r2.dev comment, the README's date. | Fixed. |
| P3-7 | A partial manifest's `frame_schedule` / `expected_frames` describe the full schedule. | Accepted (partial manifests are dry-run artefacts, never pointed to). |
| P3-8 | Nothing checks consecutive indices across +120 → +123 while playing. | Covered by Part 2's review (G13b). |
| P3-9 | The upload-failure test's bound became weak; a comment overstated the final-drain coverage. | Bound tightened (< 30 steps). |
| P3-10 | A newest cycle that never completes only shows as the stale banner. | Accepted (the banner is the signal). |

Verified by the reviewer: 8 complete cycles have all 836 needed keys (418 per product, one listing page each); f121 is
absent everywhere and nothing exists outside `STEPS`; every one of the 209 `.idx` files in 4 cycles has exactly one record
for each of the five fields; `check_identity` / `check_geometry` pass on real records at f001, f002, f119, f120, f123 and
f384; 783 MB of ranged downloads per run; all 209 steps complete at +5.29–5.52 h (27–47 min after f240; the hourly steps
by +4.3 h); the gap between publishes stays ~6 h, so the 9-h stale banner keeps ≥ 2.2 h of margin; a run queued during an
11.5-min build ran after it and short-circuited (nothing piles up, nothing is cancelled); ~309 MB per run, ≤ 1.9 GB kept,
~226k R2 writes a month (free tier); a 209-frame manifest is 13,019 B raw, served brotli-compressed; the served 2.9.5
client works by frame index and valid time throughout; the job tests take 32 s on the CI runner. Not verifiable here:
the pipelined build time on the runner (the first pipelined run had not published), the wave values at hourly steps
(JPEG2000), long-term reliability of NOAA's f241–f384 tail.

After the fixes: job tests 66.

## G13b — the client on the test site with the first 209-frame run (2026092600): 0 P0, 1 P1, 2 P2, 5 P3

One fresh-context reviewer at high effort (Opus 5.5) on asset 2.10.0 (test site = 32eae6f byte for byte).

| # | Sev | Finding | Outcome |
|---|---|---|---|
| P1-1 | P1 | A slow drag snapped backwards: each input was compared with the frame just snapped to, so a forward drag read as backward (thumb 120 123 120 123 126 123 …; a drag ending at 140 left the map on +138); dragging left jumped forward the same way; also on 81-frame runs. Keyboard fine. | Fixed @ 9dabe76 (asset 2.10.1): `TimelineState` compares a drag's input with the pointer's previous raw value, keys and clicks with the shown thumb (test: steady drags never go against their direction). Verified on the test site: 118 → 140 gives 118 119 120 123 … 141, monotonic, ends on +141. |
| P2-1 | P2 | After a drag the slider keeps focus, the thumb stopped following playback, and arrow keys then compared with the playing frame instead of the frozen thumb (the map jumped back). | The thumb follows playback unless a drag is in progress; keys start from what is shown. Verified. |
| P2-2 | P2 | The 30-min run check rebuilt the whole panel: on collapsed panels every time (the status was only set when expanded), on expanded ones at the switch to "expected shortly" (focus lost; the phone sheet's scroll reset). | The run line is a text node updated in place; its status is known when collapsed; no rebuild for it. Verified in place. |
| P3 | P3 | No weekday after local midnight until a re-render; "expected shortly" up to 30 min late; the estimate (publish + 6 h) shifts with a late or repaired run; zone names / 12-24 h follow the browser locale; no handler / run-check tests. | The run line refreshes at most once a minute while the panel is used and on every run check; the estimate and the locale behaviour accepted (00Z went live at +5.55 h → the 06Z estimate 11:35 UTC against ~11:25–11:55); the handler test added via `TimelineState`. |

Verified by the reviewer: the slider 0–384 on the 209-frame run (+120 h at 31 % of the track); playback across +120 h
plays frames 116…120 then +123…+144 at ~500 ms per frame with no skip or repeat, the thumb, label and frame always
agreeing; mouse drags keep in-flight ≤ 2 and end on the thumb's frame; seeking while loading snaps the thumb at once with
the loading hint; an 81-frame run and Update (+180 h kept); the run line "live since 7:32 PM HST · next update about Sat,
1:35 AM HST" for the live run; zones in Node (Honolulu, New York across the November DST change, London, Kolkata, UTC,
Lord Howe, Chatham, St Johns) incl. the weekday rule, "shortly", "newer", a missing publish time; the phone sheet (G12
layout kept); no hot path (hours map 0.17 ms, `_syncUI` 0.8 ms); Node 120, flag 10.

After the fixes: Node 121, flag 10; asset 2.10.1 verified on the test site.
