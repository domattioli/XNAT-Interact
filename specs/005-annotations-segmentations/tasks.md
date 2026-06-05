# Tasks: Annotations & Segmentations (Phase 5)

**Input**: [spec.md](spec.md), [plan.md](plan.md). Clarify-adjusted: custom
RLE+manifest store (SEG = adapter), 4 P1 types, aggregator **seam only**,
keep-all versioned. Tests REQUIRED, offline.

## Format `[ID] [P?] [Story] desc`

## Phase A — Core model + storage (foundational, BLOCKS rest)
- [ ] T001 [FN] `src/annotations/__init__.py`; `model.py`: `Annotation`
  (annotator_id, tool, annotation_type, created_at, version, derived, payload,
  blob, codec), `AnnotationSet` (image_ref, list, manifest), `ConsensusResult`
  (payload, per_annotator, method).
- [ ] T002 [FN] `codecs.py`: `RLECodec` (lossless binary/label mask ↔ bytes via
  run-length + npz), `JsonScalarCodec` (landmark/bbox/scalar ↔ json bytes);
  `encode`/`decode`; round-trip exact.
- [ ] T003 [FN] `validate.py`: annotator-id PHI guard (opaque token only; reject
  patient-name-like), mask shape/grid checks, type-payload validators.
- [ ] T004 [FN] `registry.py`: `register(type, codec, aggregator, validator)` +
  `get(type)`; register 4 builtins — `binary_segmentation`(RLE),
  `label_map`(RLE), `landmark`(JsonScalar), `bbox`(JsonScalar).
- [ ] T005 [FN] tests `tests/test_annotations_core.py`: codec round-trip exact +
  blob smaller than raw; registry returns builtins; PHI annotator-id rejected;
  shape-mismatch rejected.

## Phase B — Aggregator seam (per Clarification 3: seam, not algorithms)
- [ ] T006 [US2] `aggregate/__init__.py`: `Aggregator` Protocol
  (`aggregate(list[payload]) -> ConsensusResult`) + registry (register/get).
- [ ] T007 [US2] `aggregate/reference.py`: trivial reference aggregator
  (pixel-majority for masks / mean for scalars) registered through the seam —
  labeled reference-only.
- [ ] T008 [US2] `aggregate/staple.py`: STAPLE **stub** raising NotImplementedError
  with a docstring describing the EM + exactly where to plug it in (extension
  slot, deferred).
- [ ] T009 [US2] tests `tests/test_annotations_aggregate.py`: reference aggregator
  runs on M masks → ConsensusResult; **register a brand-new aggregator and run the
  full pipeline with ZERO core edits** (SC-004); STAPLE stub raises clearly.

## Phase C — Tool-agnostic importers
- [ ] T010 [P][US1] `importers/generic.py`: ndarray mask → Annotation.
- [ ] T011 [P][US1] `importers/mturk.py`: MTurk PNG/base64 row → Annotation
  (annotator_id=WorkerId, tool='mturk'); reuse the dormant
  `MTurkSemanticSegmentation` intent.
- [ ] T012 [P][US1] `importers/dicom_seg.py`: DICOM SEG ↔ Annotations
  (one segment per annotator) — import + export adapter.
- [ ] T013 [US1] tests `tests/test_annotations_importers.py`: each importer →
  canonical Annotation; unknown format → FriendlyError no-raise; SEG round-trip.

## Phase D — XNAT io (manifest + blobs, versioned, keep-all)
- [ ] T014 [US1] `io_xnat.py`: `upload_annotation_set(server, image_ref, set)` →
  writes manifest.json + per-annotation RLE blobs to the image's `ANNOTATIONS`
  resource (image NOT re-uploaded); keep-all versioning (new submit = new version,
  nothing overwritten).
- [ ] T015 [US3] `io_xnat.download_annotation_set(server, image_ref, dest_dir)` →
  fetch manifest + blobs, decode, cross-platform paths, fail-soft.
- [ ] T016 [US1/US3] tests `tests/test_annotations_io.py` (FakeXNAT): K users' masks
  → one set, K annotations, image referenced not duplicated (SC-003); re-submit
  by same user → new version kept; download round-trips arrays; manifest PHI-free
  (SC-006).

## Phase E — GUI + stub wiring
- [ ] T017 [US1] `app/logic/annotations.py` (no streamlit): list image annotations,
  import, run a (registered) aggregator, prep up/download — fail-soft.
- [ ] T018 [US1] `app/pages/annotations.py` + nav (main.py/state.py): pick image →
  see per-user annotations → run consensus (registered methods) → up/download.
- [ ] T019 [US4] Wire `MTurkSemanticSegmentation` (xnat_scan_data.py) to the new
  model (or thin-adapter) so the stub is live, not dead-commented.
- [ ] T020 [US1] tests `tests/test_app_annotations_logic.py` (offline, no streamlit).

## Phase F — Simulation + docs
- [ ] T021 Extend `scripts/simulate_e2e.py`: ANNOTATIONS section — 3 users' masks
  for one image (2 tools) → store → register+run reference aggregator → up/download
  → verify round-trip + versioning + PHI-free manifest.
- [ ] T022 Update specs/README index (Phase 5 row) + note STAPLE deferred to the
  aggregator seam.

## Dependencies
- A blocks B–F. B (seam) before E (GUI runs aggregator). C importers before D/E.
- `[P]` importers (T010-T012) parallel (separate files).
