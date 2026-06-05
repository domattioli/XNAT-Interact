# Implementation Plan: Annotations & Segmentations (Phase 5)

**Branch**: `005-annotations-segmentations` | **Date**: 2026-06-05 | **Spec**: [spec.md](spec.md)

## Summary

New `src/annotations/` layer: canonical `Annotation`/`AnnotationSet` model, a
type **registry** (codec+aggregator+validator), lossless **codecs** (RLE / PNG /
JSON-scalar), **aggregators** (STAPLE, majority-vote, scalar mean), tool-agnostic
**importers** (MTurk, DICOM SEG, generic), and **XNAT io** (upload/download an
annotation set via the injected server seam). GUI gets an Annotations page. Wires
the dormant `MTurkSemanticSegmentation` stub into the real model. Offline-testable,
data-efficient, extensible.

## Technical Context

**Language**: Python 3.11. **Deps**: numpy, pydicom, opencv/Pillow (already
present); STAPLE in pure numpy — NO new heavy dep (SimpleITK optional). **Storage**:
compressed blobs + JSON manifest under the image's `ANNOTATIONS` XNAT resource.
**Testing**: pytest, offline, FakeXNAT + synthetic masks. **Reuses**: Phase-1
`services/{errors,deidentify}`, the gateway/FakeXNAT seam, Phase-2 app/logic
pattern.

## Constitution Check

- **I PHI**: masks rarely hold burned-in PHI but flow the normal pipeline; new
  PHI surface = `annotator_id` → FR-010 validator rejects patient-identifier-like
  ids (opaque tokens only). Manifest PHI-free (SC-006). ✓
- **II Fail-soft**: bad file / shape mismatch / unknown tool → FriendlyError,
  no traceback (FR-011). ✓
- **III Skill floor**: GUI Annotations page; tool-agnostic import hides
  per-tool format details. ✓
- **IV Offline-testable**: all paths via injected server + synthetic masks;
  STAPLE deterministic toy tests (FR-011, SC-005). ✓
- **V Config/secrets**: none new; no creds. ✓
- **VI Integrity/efficiency**: lossless round-trip + compression + no image
  duplication + self-describing manifest (FR-002,003,012; SC-001,003). ✓

## Project Structure

```text
src/annotations/
├── __init__.py
├── model.py          # Annotation, AnnotationSet, ConsensusResult (dataclasses)
├── registry.py       # type -> {codec, aggregator, validator}; register()/get()
├── codecs.py         # RLECodec, PngMaskCodec, JsonScalarCodec (encode/decode bytes)
├── validate.py       # annotator-id PHI check, shape/grid checks
├── aggregate/
│   ├── __init__.py   # Aggregator Protocol + registry (register/get) — THE seam
│   ├── reference.py  # trivial majority(mask)/mean(scalar) reference aggregator
│   └── staple.py     # STAPLE extension slot: stub raising NotImplementedError +
│                     # docstring showing where/how to implement (deferred)
├── importers/
│   ├── __init__.py
│   ├── mturk.py      # MTurk PNG/base64 -> Annotation
│   ├── dicom_seg.py  # DICOM SEG (1 segment/annotator) <-> Annotation
│   └── generic.py    # generic 2-D mask ndarray -> Annotation
└── io_xnat.py        # upload_annotation_set / download_annotation_set (injected server)

app/logic/annotations.py     # list/import/aggregate/download logic (no streamlit)
app/pages/annotations.py     # streamlit page
tests/test_annotations_*.py  # model/codecs/registry, staple/vote/scalar, importers, io, app-logic
scripts/simulate_e2e.py      # extend: ANNOTATIONS section
```

## Approach

1. **Core** (model+codecs+registry+validate): the canonical types + lossless
   blobs + extensibility seam. Register builtins: `semantic_segmentation`
   (RLE+STAPLE), `label_map` (RLE+multilabel-STAPLE/vote), `landmark`
   (JSON-scalar+mean), `bbox` (JSON-scalar+mean).
2. **Aggregator seam** (per Clarification 3): `Aggregator` Protocol + registry +
   a trivial reference aggregator to prove end-to-end; STAPLE/vote/mean are
   registerable add-ons (STAPLE = documented stub slot, not built this phase).
3. **Importers**: generic + mturk + dicom_seg → canonical model.
4. **XNAT io**: manifest + blobs to/from the `ANNOTATIONS` resource via FakeXNAT.
5. **GUI**: annotations page + nav; wire `MTurkSemanticSegmentation` to the model.
6. **Sim**: ANNOTATIONS section (multi-user import → STAPLE → up/download).

## Risks & Mitigations
- *Aggregator seam vs deferred algorithms*: ship the interface + registry + a
  trivial reference aggregator only; STAPLE/vote/mean register later. Prove the
  seam with a test that registers a brand-new aggregator with zero core edits.
- *DICOM SEG complexity*: keep the importer to the segment-per-annotator subset
  the sandbox notebook already sketched; document unsupported corners.
- *Extensibility proof*: a test registers a brand-new type and runs the full
  pipeline with zero core edits (SC-004).
