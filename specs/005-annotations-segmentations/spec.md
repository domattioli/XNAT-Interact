# Feature Specification: Annotations & Segmentations (Phase 5)

**Feature Branch**: `005-annotations-segmentations`
**Created**: 2026-06-05
**Status**: Draft
**Input**: Goal — speckit + sim for upload/download of segmentation + other annotation data; multi-user per image; per-attribute averages (STAPLE); future annotation types expand; tool-agnostic; data-efficient storage. Existing stub: `MTurkSemanticSegmentation` (`xnat_scan_data.py:307`) + `sandbox-mturk.ipynb` (DICOM SEG, one segment per worker).

## Overview

Add a first-class **annotation layer** on top of the existing image data model: each
image can carry **many annotations from many users (annotators), produced by
different tools**. The system stores them **data-efficiently** (compressed
masks, no image duplication), computes **per-image consensus/averages**
(STAPLE for label masks; majority-vote; scalar mean for point/scalar
attributes), and is **extensible** — new annotation types register without
touching core. Upload/download reuse the Phase-1 publish/fetch path + FakeXNAT
seam; the GUI gets an Annotations screen. All offline-testable, no PHI.

## Clarifications (2026-06-05, speckit-clarify)

1. **Storage** (operator deferred → maintainer call): primary store = **custom
   run-length-encoded mask blobs + a JSON manifest** per image under an
   `ANNOTATIONS` resource (lightest, simplest, offline-testable). **DICOM SEG** is
   supported as an **import/export adapter** for interoperability, not the
   canonical store.
2. **P1 annotation types** = **all four**: binary segmentation, multi-label
   segmentation, landmarks/points, bounding boxes. Each ships codec + validator.
3. **Consensus/averaging** = **seam only this phase**. Build the pluggable
   **aggregator interface + registry** so methods (STAPLE, majority-vote, scalar
   mean) can be registered/added later WITHOUT core edits. Do **not** implement
   the algorithms now; ship a minimal **reference aggregator** (trivial mean /
   majority) only to prove the seam end-to-end, plus a clearly-marked STAPLE
   extension slot.
4. **Annotator model** = **keep-all, versioned**: a user may submit more than once
   per (image, type); all kept + timestamped; default consumers use each
   annotator's latest. `annotator_id` = opaque token (XNAT username / worker id),
   PHI-validated — never a patient identifier.

> These supersede any conflicting wording below (notably: STAPLE is a
> registerable add-on via the aggregator seam, not a built-in deliverable this
> phase).

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Many users annotate one image; stored efficiently (Priority: P1)

A reviewer collects N users' segmentations of the same image (from MTurk, a
desktop tool, a NIfTI export — different tools) and uploads them as one
annotation set for that image. Masks are stored compressed (RLE/PNG), the
reference image is NOT re-uploaded, and each annotation records who/what/when
(annotator id, tool, type, timestamp).

**Why first**: This is the core capability — multi-user-per-image annotation
ingest with efficient storage. Everything else (consensus, download) needs it.

**Independent Test**: Import K masks for one image via the tool-agnostic
importer; assert one `AnnotationSet` with K `Annotation`s, each with
annotator/tool/type, stored as compressed blobs whose decoded arrays
round-trip to the originals; assert the reference image is referenced, not
duplicated. Offline, FakeXNAT.

**Acceptance Scenarios**:
1. **Given** 3 users' binary masks for image X from 2 different tools, **When**
   imported, **Then** one AnnotationSet(image=X) holds 3 annotations tagged with
   their annotator id + tool + `semantic_segmentation` type.
2. **Given** a stored mask blob, **When** decoded, **Then** it equals the
   original array bit-for-bit (lossless codec).
3. **Given** upload, **When** published, **Then** annotations go to the image's
   `ANNOTATIONS` resource and the source image is not re-sent.

---

### User Story 2 — Per-image consensus / averages (STAPLE) (Priority: P1)

For an image with multiple users' label masks, compute a **consensus**
(STAPLE) and per-annotator performance; for scalar/point attributes compute the
mean. The consensus is stored as a **derived** annotation (clearly not a user's).

**Why this priority**: The averaging (STAPLE especially) is an explicit goal and
the main analytic payoff of collecting multiple annotations.

**Independent Test**: Feed known toy masks; assert STAPLE consensus matches the
expected result (all-agree → that mask; known-disagreement → documented
outcome) and that sensitivity/specificity per annotator are returned; assert a
scalar attribute's mean is correct. Offline, deterministic.

**Acceptance Scenarios**:
1. **Given** M identical binary masks, **When** STAPLE runs, **Then** consensus
   == that mask and every annotator's sensitivity/specificity ≈ 1.
2. **Given** mixed masks, **When** STAPLE runs, **Then** it returns a
   probabilistic consensus + per-annotator performance, marked
   `aggregation=staple`, `derived=True`.
3. **Given** point annotations (e.g. a landmark x,y per user), **When**
   averaged, **Then** the mean point is returned.

---

### User Story 3 — Download annotations + consensus (Priority: P2)

A user downloads an image's annotation set — all users' masks and/or the
consensus — into a folder, in a documented format, cross-platform.

**Why this priority**: Round-trips the data back out for analysis; depends on
P1 storage.

**Independent Test**: Seed FakeXNAT with a stored AnnotationSet; download to
tmp; assert per-annotator masks + consensus land as files, decode correctly,
forward-slash/pathlib paths.

**Acceptance Scenarios**:
1. **Given** a stored set, **When** downloaded, **Then** each annotation + the
   consensus is written and decodes to the original arrays.

---

### User Story 4 — New annotation type without core change (Priority: P2)

A future annotation type (e.g. bounding boxes, landmarks, free-text labels,
multi-class label maps) is added by registering a codec + aggregator +
validator — no edits to the upload/download/storage core.

**Why this priority**: "Future annotation types may expand" is an explicit goal;
extensibility must be designed in, proven now.

**Independent Test**: Register a new toy type via the registry; import,
store/round-trip, aggregate, download it through the SAME core paths used by
segmentation, with zero core edits.

**Acceptance Scenarios**:
1. **Given** a newly registered `landmark` type, **When** the same import →
   store → aggregate → download pipeline runs, **Then** it works without
   modifying core modules.

---

### Edge Cases
- Masks of mismatched shape for the same image → reject with a clear error
  (cannot aggregate different grids).
- A single annotator (N=1) → consensus = that annotation; performance undefined,
  reported as such.
- Empty mask / all-background → handled, no divide-by-zero in STAPLE.
- An unknown tool format on import → friendly error, never a raw traceback.
- Annotator id that looks like PHI (a real name) → rejected/pseudonymized
  (annotator ids must be opaque, not patient identifiers).

## Requirements *(mandatory)*

### Functional Requirements
- **FR-001**: System MUST model an image as having many `Annotation`s from many
  annotators, each carrying `annotator_id`, `tool`, `annotation_type`,
  `created_at`, and a typed payload.
- **FR-002**: System MUST store mask payloads **losslessly compressed**
  (run-length encoding and/or PNG/packbits), never as raw uncompressed arrays,
  and MUST NOT duplicate the reference image.
- **FR-003**: Stored payloads MUST round-trip (decode == original array).
- **FR-004**: System MUST provide a pluggable **aggregator seam** — an
  `Aggregator` interface + registry keyed by name — so consensus methods
  (STAPLE, majority-vote, scalar mean) can be registered and selected per
  annotation type **without core edits**. The algorithms themselves are
  out-of-scope this phase (per Clarification 3); STAPLE has a clearly-marked
  extension slot.
- **FR-005**: System MUST ship a minimal **reference aggregator** (trivial
  majority for masks / mean for scalars) registered through the seam, solely to
  demonstrate the end-to-end pipeline; it is explicitly a reference example, not
  a validated consensus method.
- **FR-013**: System MUST keep **all** submissions (versioned, timestamped); a
  user MAY submit more than once per (image, type); default consumers use each
  annotator's latest version (per Clarification 4).
- **FR-006**: Consensus/aggregated results MUST be stored as `derived=True`
  annotations distinguishable from user-submitted ones.
- **FR-007**: System MUST provide a **registry** mapping `annotation_type` →
  {codec, aggregator, validator} so new types are added without core edits.
- **FR-008**: System MUST provide **tool-agnostic importers** normalizing at
  least: MTurk PNG/base64 masks, DICOM SEG (one segment per annotator), and a
  generic 2-D mask array, into the canonical `Annotation` model.
- **FR-009**: Upload MUST publish an image's annotations to the image's
  `ANNOTATIONS` resource via the Phase-1 path; download MUST fetch them back —
  both testable offline against FakeXNAT.
- **FR-010**: `annotator_id` MUST be opaque (worker/user token), never a patient
  identifier; importers MUST reject/replace PHI-like annotator ids.
- **FR-011**: All annotation operations MUST be offline-testable (no server, no
  PHI) and MUST fail soft (FriendlyError, no raw traceback) on foreseeable
  problems (bad file, shape mismatch, unknown tool).
- **FR-012**: A storage **manifest** (JSON) MUST index an AnnotationSet
  (image ref, list of annotations w/ metadata + blob refs + codec) so the set is
  self-describing on download.

### Key Entities
- **Annotation**: one user's annotation of one image — `annotator_id`, `tool`,
  `annotation_type`, `created_at`, `payload` (decoded), `blob`+`codec` (stored),
  `derived` flag.
- **AnnotationSet**: all annotations for one image + a `manifest`.
- **AnnotationType registry entry**: `{codec, aggregator, validator}` keyed by
  type name.
- **Codec**: encode/decode a payload to/from a compact byte blob (RLE, PNG,
  JSON-scalar).
- **Aggregator**: `aggregate(list[payload]) -> ConsensusResult` (STAPLE,
  majority-vote, mean).
- **ConsensusResult**: consensus payload + per-annotator performance + method.

### Success Criteria *(mandatory)*
- **SC-001**: A mask stored then loaded equals the original (0 differing pixels)
  while the blob is strictly smaller than the raw array for typical masks.
- **SC-002**: A consensus aggregator registered through the seam runs on an
  image's M annotations and returns a `ConsensusResult`; registering a NEW
  aggregator (e.g. a real STAPLE) requires zero edits to the import/store/io
  core (seam proven, algorithm deferred).
- **SC-003**: One image with K users' masks → one AnnotationSet with K
  annotations; reference image stored once (by reference), not K+1 times.
- **SC-004**: A newly registered annotation type runs the full import → store →
  aggregate → download pipeline with **zero** edits to core modules.
- **SC-005**: Every annotation path has an offline test (no network, no PHI);
  the e2e sim gains an annotations section that passes.
- **SC-006**: No `annotator_id` in any stored manifest is a patient identifier
  (enforced by validation + test).

### Assumptions
- STAPLE implemented in numpy (binary EM; multi-label via one-vs-rest) — no
  heavy new dependency; SimpleITK optional, not required.
- Masks for one image share a grid/shape; differing shapes are an error, not a
  resize.
- "Data-efficient" = lossless compression + no image duplication + shared
  manifest; not lossy.
- De-identification: masks rarely carry burned-in PHI, but annotation upload
  still flows through the existing pipeline; the new PHI surface is the
  `annotator_id`, handled by FR-010.
