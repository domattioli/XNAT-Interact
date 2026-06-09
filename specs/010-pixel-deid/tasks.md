# Tasks: Burned-in PHI Pixel De-identification (010-pixel-deid)

**Input**: `specs/010-pixel-deid/{spec,plan}.md`
**Tests**: REQUIRED — Constitution Principle I + IV mandate a PHI-removal test and offline
synthetic coverage. SC-001 (FN=0 holdout) is the hard gate.

## Format: `[ID] [P?] [Story] Description`
- **[P]** = parallelizable (different files, no dep). `[Story]` ∈ US1..US4 or FOUND/POLISH.

## Dependency order
S1 detect → S2 profiles+consensus → S3 verdict → S4 quarantine+gate → S5 throughput → S6 validation.
US-coverage: US1=profiles(S2)+gate(S4); US2=detector(S1)+consensus(S2)+routing(S3); US3=PHI-NER(S3);
US4=throughput(S5). SC-001 holdout (S6) spans all.

---

## Phase 1: Setup

- [ ] T001 Create package skeleton `src/services/pixel_deid/__init__.py` and empty modules
  (`detect.py`, `profiles.py`, `consensus.py`, `verdict.py`, `quarantine.py`); add
  `data/device_profiles/` and `models/craft/` dirs with `.gitkeep`.
- [ ] T002 [P] Add deps to `requirements*.txt` / packaging: `presidio-image-redactor`,
  `presidio-analyzer`, `pytesseract`, `onnxruntime` (CPU), spaCy `en_core_web_lg`; document the
  Tesseract-5 system binary prereq in `docs/` install notes.

## Phase 2: Foundational (synthetic fixtures — blocks all stories)

- [ ] T003 Extend `tests/synthetic_data.py`: add `make_faint_burned_in_phi_pixel_array`
  (low-contrast text near background) and `make_multiframe_phi_case` (N frames, static overlay +
  moving anatomy) and a `make_unprofiled_device_dataset`. Synthetic only, fake PHI strings. [FR-014]

---

## Phase 3: US2 — Detector primitives (Priority: P1) 🎯 catch-all core

**Goal**: find burned-in text regions including faint/stylized; the spike's 0.0-recall gap.
**Independent Test**: faint synthetic frame → union boxes cover the PHI bbox.

- [ ] T004 [US2] `detect.py`: multipass image variants `{orig, invert, contrast-stretch, CLAHE}`
  (OpenCV) + `_tesseract_boxes(img)` sparse-psm11 detection. Seed from the spike `multipass.py`. [FR-004]
- [ ] T005 [US2] `detect.py`: `detect_text_regions(img) -> boxes` CRAFT ONNX-CPU wrapper —
  lazy-load vendored model from `models/craft/`, onnxruntime CPUExecutionProvider, model-agnostic
  signature so docTR can swap later. Graceful degrade → Tesseract-only if model absent. [FR-004]
- [ ] T006 [US2] `detect.py`: `union_dilate(boxes, margin)` — merge overlapping boxes + dilate by
  configurable margin. [FR-007]
- [ ] T007 [P] [US2] `tests/test_010_detect.py`: crisp recall stays 1.0; faint recall improves vs
  single-pass; benign markers still detected as text (classification deferred to S3).

## Phase 4: US1 + US2 — Contrast-independent masks (Priority: P1)

**Goal**: zero-FN anchor (profiles) + multi-frame static-overlay catch (consensus).
**Independent Test**: profiled case → profile boxes mask PHI w/o detector; multi-frame faint case
→ consensus mask covers overlay even when single-frame OCR returns nothing.

- [ ] T008 [US1] `profiles.py`: device-profile registry — schema (device-id key → list of overlay
  boxes + tolerance), loader/validator, `boxes_for(dataset)` resolving device id from DICOM tags /
  0x0019 private block. [FR-002]
- [ ] T009 [US1] Author **1–2 starter profiles** in `data/device_profiles/` from the device
  tags/private-block patterns present in the synthetic/test fixtures (worked example + schema). [FR-002]
- [ ] T010 [US2] `consensus.py`: `variance_consensus_mask(frames)` — per-pixel variance across a
  case's frames; low-variance + bright → static-overlay box. Single-frame → empty (fall back). [FR-003]
- [ ] T011 [P] [US1/US2] `tests/test_010_profiles_consensus.py`: profiled case fully masked w/o
  detector; multi-frame consensus catches faint static overlay; single-frame degrades cleanly.

## Phase 5: US3 — PHI classification + verdict engine (Priority: P1/P2)

**Goal**: PHI-vs-benign + fail-closed routing → `clean|redacted|quarantine`.
**Independent Test**: frame w/ PHI + `L` marker → PHI masked, `L` preserved; unprofiled+low-conf
→ quarantine, never clean-with-residual-PHI.

- [ ] T012 [US3] `verdict.py`: Presidio analyzer over OCR'd box text → PHI category/confidence;
  classify benign markers (laterality/view/technique) as non-PHI → preserve. [FR-005, FR-009]
- [ ] T013 [US3] `verdict.py`: `assess_case(...)` — union profile + consensus + detector boxes,
  `redact()` irreversible-on-copy fill, dilation; return verdict + masked pixels. [FR-001, FR-008]
- [ ] T014 [US3] `verdict.py`: fail-closed routing — unprofiled & low detector confidence, OR
  PHI-pattern outside maskable region, OR profile/detector disagreement > tolerance → `quarantine`. [FR-006]
- [ ] T015 [US3] `verdict.py`: audit entry via 009 `src/services/registry.py` — regions masked,
  tiers fired, PHI categories; **no raw PHI text**. [FR-008, SC-004]
- [ ] T016 [P] [US3] `tests/test_010_verdict.py`: clean/redacted/quarantine routing; benign-marker
  preserve; audit has no raw PHI.

## Phase 6: US1 — Quarantine store + upload-gate integration (Priority: P1)

**Goal**: replace placeholder; wire verdict into the existing PHI gate, additive.
**Independent Test**: quarantined case is blocked + held w/ evidence; clean/redacted upload proceeds;
existing suites stay green.

- [ ] T017 [US1] `quarantine.py`: quarantine store — write held case + evidence sidecar JSON
  (flagged regions, tiers, categories; no raw PHI); held, never auto-uploaded/auto-over-masked. [FR-015]
- [ ] T018 [US1] `deidentify.py`: `needs_pixel_review` delegates to `verdict.assess_case` (verdict
  != clean ⇒ review path); keep signature back-compat. [FR-001]
- [ ] T019 [US1] `xnat_experiment_data.py`: PHI gate consumes the verdict — `clean|redacted`
  proceed, `quarantine` blocks upload (additive/opt-in param, default path preserved like 009). [FR-012, FR-013]
- [ ] T020 [P] [US1] `tests/test_010_quarantine_gate.py`: quarantine blocks upload + evidence
  written; clean/redacted proceed; assert 992-offline suite unaffected (default path).

## Phase 7: US4 — Batch throughput (Priority: P2)

- [ ] T021 [US4] Batch driver: `multiprocessing` fan-out across cases; learned detector only on
  unprofiled/low-confidence frames (profiled = instant). [FR-011]
- [ ] T022 [US4] `tests/test_010_throughput.py` (marked `slow`): 200 synthetic image-sized cases
  complete ≤15 min CPU-only; assert no GPU provider required. [SC-005]

## Phase 8: Validation / Polish (SC-001 hard gate)

- [ ] T023 [POLISH] `tests/test_010_holdout_fn0.py`: labeled synthetic holdout — crisp/faint ×
  profiled/unprofiled × single/multi-frame. Assert **FN=0**: every residual-PHI case is fully
  masked OR quarantined; no clean/redacted verdict leaks PHI pixels. [SC-001]
- [ ] T024 [POLISH] Quarantine-rate check on profiled in-distribution holdout < 10% (SC-002);
  benign-marker preservation ≥ 90% (SC-003).
- [ ] T025 [POLISH] CI CPU lane runs the holdout (no GPU); full offline suite green; add the
  pixel-deid benchmark row (metric `phi_pixel_false_negative_rate`, baseline = spike 0.0 faint
  single-pass). [SC-006]
- [ ] T026 [POLISH] Vendored CRAFT model push via **git CLI direct** (binary; never MCP per
  DomI #85) + magic-byte/size verify; docs note model provenance + license.
- [ ] T027 [POLISH] Update `specs/README.md` (row for 010) + `docs/DATA_MODEL.md §4.2` (replace the
  always-True `needs_pixel_review` placeholder note with the shipped pipeline).

---

## Parallelization notes
- `[P]` test tasks (T007, T011, T016, T020) run alongside their stage's impl once the module exists.
- T008/T010 are independent files → parallel. T004→T005→T006 are same-file, sequential.
- Stages are gated: S3 needs S1+S2 box types; S4 needs S3 verdict; S6 needs all.

## Definition of done
- All FR-001..FR-015 covered; SC-001 FN=0 holdout green (hard gate); SC-005 ≤15 min; offline suite
  green + CPU CI lane; no raw PHI anywhere; default upload path unbroken (additive wiring).
