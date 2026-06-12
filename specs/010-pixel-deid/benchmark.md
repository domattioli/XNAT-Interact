# Benchmark — 010-pixel-deid

Primary metric: **`phi_pixel_false_negative_rate`** (FN = leaked PHI pixels in a
`clean`/`redacted` verdict). Hard gate: **must be 0** on the synthetic holdout
(SC-001). Secondary: per-case throughput (SC-005), quarantine rate (SC-002),
benign-marker preservation (SC-003).

| version | date | metric | baseline | observed | delta | evidence |
|---|---|---|---|---|---|---|
| v1 | 2026-06-09 | phi_pixel_false_negative_rate | 1.0 (faint single-pass OCR, spike) | 0 (mask-or-quarantine on holdout) | −1.0 | `tests/test_010_holdout_fn0.py` |
| v1 | 2026-06-09 | throughput (200-case serial) | n/a | ≈9.0 min (2.7 s/case warm) | — | clean timing measurement; SC-005 budget 15 min |
| v1 | 2026-06-09 | ocr_seconds_per_frame (Tesseract) | 20–42 s (`--psm 11`) | 0.36 s (`--psm 3`) | ~60× faster | config comparison; identical box detection |

## Notes
- The FN=0 result holds via the **contrast-independent** tiers (device profile +
  cross-frame consensus) + **fail-closed quarantine** — not via OCR recall, which
  the spike showed is 0.0 on faint text. OCR is best-effort + timeout-degraded.
- The `--psm 11 → --psm 3` change was the throughput-critical fix: `--psm 11`
  sparse-layout analysis exploded to 20–42 s on low-information frames; `--psm 3`
  returns the same boxes in ~0.36 s.
- CRAFT ONNX detector is vendored-later (operational); absent ⇒ graceful-degrade to
  `[]`, FN=0 still carried by profile/consensus/quarantine (holdout crux cell).
- Heavy-dep tests (`pixeldeid` + `slow` markers) run in an opt-in CPU lane; the
  default lane deselects them via `-m "not slow"`.
