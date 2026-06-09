# Implementation Plan: Burned-in PHI Pixel De-identification (upload-time)

**Branch**: `development` (feature `010-pixel-deid`) | **Date**: 2026-06-09 | **Spec**: [`spec.md`](spec.md)
**Input**: Feature specification from `specs/010-pixel-deid/spec.md`

## Summary

Replace the always-`True` `needs_pixel_review` placeholder with a **tiered, CPU-only
burned-in-PHI pipeline** that returns a per-case verdict `clean | redacted | quarantine`.
Tiers, cheapest→heaviest:

0. **Device-profile blind mask** (contrast-independent trust anchor) — registry of
   device → overlay boxes; blind-mask profiled devices.
0'. **Cross-frame variance consensus** (contrast-independent) — static bright overlay
    regions pop out across a multi-frame case regardless of text contrast.
2. **Multipass detector** — Tesseract/Presidio OCR over {orig, invert, stretch, CLAHE}
   **+ a learned ONNX-CPU text-region detector (CRAFT)** for faint/stylized text; union boxes.
3/4. **Presidio PHI-NER** over OCR'd text → PHI-vs-benign so laterality/view markers survive.

Routing: union all maskable regions → redact (dilated). **Fail closed** → `quarantine`
(blocked from upload, held with evidence) when device unprofiled + low detector confidence,
or a PHI pattern lands outside a maskable region, or profile/detector disagree beyond tolerance.

Spike (2026-06-09) baseline: crisp PHI single-pass recall 1.0, **faint 0.0** → the
contrast-independent tiers (0, 0') + the learned detector are what carry SC-001 (FN=0).

## Technical Context

**Language/Version**: Python 3.11 (repo standard).
**Primary Dependencies**: `presidio-image-redactor` + `presidio-analyzer` + `pytesseract`
(Tesseract 5 binary) for OCR + PHI-NER; `opencv-python` (already vendored) for preprocessing
+ variance mask; **`onnxruntime` (CPU) + a vendored CRAFT detector model** for the learned
detector tier; `pydicom`/`numpy` (already present). `spaCy en_core_web_lg` for the analyzer.
**Storage**: device-profile registry = a versioned JSON/YAML data file in-repo; quarantine
store = a local directory of held cases + a sidecar evidence JSON (no raw PHI). De-id audit
entries route through the existing registry/audit-log seam (009 `src/services/registry.py`).
**Testing**: pytest, offline, synthetic-only (`tests/synthetic_data.py` already has
`make_burned_in_phi_pixel_array`); new SC-001 holdout = labeled synthetic frames; CI CPU lane.
**Target Platform**: Linux/macOS/Windows CPU-only (no GPU, no CUDA).
**Project Type**: single project (library + services + CLI/GUI seam) — Option 1.
**Performance Goals**: 200 image-sized cases ≤15 min wall-clock on multi-core CPU (SC-005),
via `multiprocessing` fan-out across cases.
**Constraints**: CPU-only; FN=0 on the SC-001 holdout (hard gate); no raw PHI in logs/audit/
tests/repo; integrate as an upload gate without breaking the 992-offline + live dual-run suites.
**Scale/Scope**: image-sized cases (small per-case frame counts), batches ≤200.

## Constitution Check

*GATE: must pass before build. This feature IS Principle I.*

- **I — PHI Safety First**: ✅ This feature closes the known burned-in-pixel gap. Adds automated
  pixel de-id + fail-closed quarantine; ships the SC-001 FN=0 test proving PHI removal. Audit
  stores categories only, never raw PHI. **Directly satisfies the principle's open gap.**
- **II — Fail Softly**: ✅ No tracebacks. Unprofiled/low-confidence/ambiguous → friendly
  `quarantine` verdict with next step (operator review), via the existing `FriendlyError`/
  `ReviewDecision` seam. One bad frame doesn't drop the batch.
- **III — Skill Floor**: ✅ Default path is automatic (no per-image human). Human only at the
  rare quarantine boundary, surfaced through the existing GUI upload confirmer; no new terminal
  step for the student.
- **IV — Testable Offline**: ✅ Fully offline + synthetic. No server, no VPN, no PHI. New CRAFT
  model runs under onnxruntime-CPU locally; holdout fixtures are generated, not real.
- **V — Config over Hardcoding**: ✅ Device-profile registry + quarantine path + thresholds in
  config/data files, not source. No endpoints/credentials touched.
- **VI — Data Integrity**: ✅ Audit entries through the 009 SQLite registry (transactional).
  Redaction is irreversible-on-copy; verdict gating is explicit. No shared-state lost-update risk
  (per-case writes).

No violations → Complexity Tracking empty.

## Approach (staged, dependency-ordered)

```
Stage 1  Detector + preprocessing primitives  (services/pixel_deid/detect.py)
         - multipass image variants (orig/invert/stretch/CLAHE)         [FR-004]
         - Tesseract sparse detect → boxes
         - CRAFT ONNX-CPU detector wrapper (vendored model, lazy-load)  [FR-004]
         - box union + dilation                                         [FR-007]
              ▼ depends on nothing repo-side; pure-CPU; spike code is the seed
Stage 2  Contrast-independent masks
         - device-profile registry schema + 1–2 starter profiles        [FR-002]
           (authored from device tags / 0x0019 private-block in fixtures)
         - cross-frame variance consensus mask                          [FR-003]
              ▼ depends on S1 box types
Stage 3  PHI classification + verdict engine  (services/pixel_deid/verdict.py)
         - Presidio analyzer over OCR text → PHI vs benign              [FR-005,009]
         - union all maskable regions; redact (irreversible-on-copy)    [FR-008]
         - fail-closed routing → clean|redacted|quarantine             [FR-001,006]
         - audit entry (categories only, no raw PHI) via 009 registry   [FR-008]
              ▼ depends on S1+S2
Stage 4  Quarantine store + upload-gate integration
         - quarantine dir + evidence sidecar (no raw PHI)               [FR-015]
         - replace needs_pixel_review; wire verdict into the PHI gate   [FR-001,012,013]
           in xnat_experiment_data.py (additive; default path preserved)
              ▼ depends on S3
Stage 5  Batch throughput
         - multiprocessing fan-out across cases; ≤15 min / 200          [FR-011, SC-005]
Stage 6  Validation
         - SC-001 labeled synthetic holdout (crisp/faint × profiled/unprofiled
           × single/multi-frame); assert FN=0 (mask-or-quarantine)      [SC-001]
         - CPU CI lane; full offline suite green; benchmark row          [SC-006]
```

### Detector-tier decision (CRAFT vs docTR)

- **CRAFT** chosen for v1: it's a pure text-**region** detector (we only need *where*, not
  *what*, to mask), small, ONNX-exportable, runs well on CPU; available as a standalone model
  (also the detector inside EasyOCR). docTR bundles detection+recognition (heavier, more deps).
- Wrapper is model-agnostic (`detect_text_regions(img) -> boxes`) so docTR can swap in later
  without touching callers. Final choice confirmed at Stage 1 against the SC-005 15-min budget;
  if CRAFT-ONNX deps prove heavy on CPU, fall back to Tesseract-multipass + the contrast-
  independent tiers (which already carry FN=0 via mask-or-quarantine) and log CRAFT as deferred.

## Project Structure

```text
specs/010-pixel-deid/
├── spec.md          # done
├── plan.md          # this file
├── research.md      # spike findings + detector decision (distilled here; optional file)
└── tasks.md         # /speckit-tasks output (next)

src/services/pixel_deid/            # NEW package (keeps deidentify.py tag-scrub separate)
├── __init__.py
├── detect.py        # S1 multipass + Tesseract + CRAFT-ONNX wrapper + union/dilate
├── profiles.py      # S2 device-profile registry (schema + loader)
├── consensus.py     # S2 cross-frame variance mask
├── verdict.py       # S3 PHI-NER + redaction + fail-closed routing → verdict
└── quarantine.py    # S4 quarantine store + evidence sidecar

src/services/deidentify.py          # MODIFIED: needs_pixel_review delegates to verdict engine
src/xnat_experiment_data.py         # MODIFIED: PHI gate consumes verdict (additive, opt-in)
data/device_profiles/               # NEW: starter profile data files (config, not source)
models/craft/                       # NEW: vendored CRAFT ONNX model (git-tracked or fetched)

tests/
├── synthetic_data.py               # EXTEND: faint/static-overlay/multi-frame generators
├── test_010_detect.py             # S1
├── test_010_profiles_consensus.py # S2
├── test_010_verdict.py            # S3 (clean/redacted/quarantine routing)
├── test_010_quarantine_gate.py    # S4
└── test_010_holdout_fn0.py        # S6 SC-001 hard gate (FN=0)
```

**Structure Decision**: New `src/services/pixel_deid/` package — keeps the multi-tier pixel
pipeline cohesive and separate from the existing tag-scrub `deidentify.py`, which only delegates
its `needs_pixel_review` to the new verdict engine. Additive wiring into the existing PHI gate
(`xnat_experiment_data.py`) preserves the 992-offline + live dual-run suites (same pattern that
kept 009 green).

## Risks / mitigations

- **CRAFT-ONNX dep/model-vendoring weight** → wrapper is swappable + optional; FN=0 is carried by
  the contrast-independent tiers + quarantine even if the learned detector is deferred. Vendored
  model is binary → push via **git CLI direct**, never MCP (per DomI #85), verify magic-bytes.
- **15-min budget under multipass×CRAFT×200** → fan-out across cores; cap passes; learned detector
  only on unprofiled/low-confidence frames, not every frame (profiled path is instant).
- **Over-masking benign markers** → PHI-NER gate (FR-009) + SC-003; FP is acceptable, FN is not.
- **Subagent narration drift** (recurring) → orchestrator diffs every subagent commit + `git
  status` after each stage; don't trust the prose.

## Complexity Tracking

*No constitution violations — section intentionally empty.*
