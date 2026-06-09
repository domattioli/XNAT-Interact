# Feature Specification: Burned-in PHI Pixel De-identification (upload-time)

**Feature Branch**: `010-pixel-deid`
**Created**: 2026-06-09
**Status**: Draft
**Input**: User description: "SOTA CPU-only methods to robustly scan and de-id up to 200 image-sized cases at upload time; replace the always-True `needs_pixel_review` placeholder."

## Context

Source images are fluoroscopy / OR C-arm frames (modality XA/RF). DICOM **tag**
scrubbing is already solid (`src/services/deidentify.py`). The open gap is
**PHI burned into the pixels** at acquisition: patient name, MRN, DOB, study
date, hospital/room — rendered as overlay text in fixed-ish regions per device.

`needs_pixel_review` currently returns `True` for every image (conservative
placeholder, forces a human look at every frame). The operator requirement is:
**robustly de-id burned-in PHI at upload time, CPU-only, for batches up to 200
image-sized cases, WITHOUT per-image or per-case human review** — human review
must be rare and happen at the quarantine boundary, not as the default gate.

### Spike evidence (2026-06-09, synthetic burned-in frames, CPU)

| Condition | Presidio/Tesseract single-pass | + contrast multipass |
|---|---|---|
| High-contrast PHI (white-on-dark) | 1.00 box-recall | 1.00 |
| Low-contrast PHI (faint overlay) | **0.00 (total leak)** | 0.25 |

Conclusions driving this spec:
1. OCR-only de-id is **blind to low-contrast text** → unacceptable false-negative
   (FN = leaked PHI) when used alone.
2. Intensity tricks (invert/stretch/CLAHE) help but do not close the gap.
3. **Contrast-independent** layers — device-profile blind masking and cross-frame
   static-overlay consensus — are mandatory, not optional.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Profiled device auto-redaction (Priority: P1)

A case arrives from a known C-arm model. The system masks all regions that the
device's profile marks as text-bearing overlay, with zero per-image human input,
and the case proceeds to upload de-identified.

**Why this priority**: Profiled devices are the common path. A deterministic,
contrast-independent blind mask is the zero-FN trust anchor; without it nothing
else is safe to auto-upload.

**Independent Test**: Feed a synthetic case tagged with a registered device
profile; assert every ground-truth PHI box is fully covered by the redaction and
no human-review flag is raised.

**Acceptance Scenarios**:

1. **Given** a case from a device with a registered profile, **When** pixel de-id
   runs, **Then** all profile-defined overlay regions are masked and the case is
   marked clean for upload with no human-review flag.
2. **Given** a profiled case where OCR additionally finds text outside the profile
   boxes, **When** de-id runs, **Then** those extra regions are also masked (union),
   never ignored.

---

### User Story 2 - Unprofiled / faint-text catch-all (Priority: P1)

A case arrives from an **unknown** device, or with **low-contrast** burned-in
text that single-pass OCR misses. The system still detects and masks the PHI, or —
if it cannot do so with confidence — routes the case to quarantine rather than
silently uploading leaked PHI.

**Why this priority**: This is exactly where out-of-box tools fail (0.00 recall in
the spike). Fail-closed behavior here is the difference between a safe system and
a PHI leak.

**Independent Test**: Feed low-contrast and unprofiled synthetic cases; assert
either full PHI coverage OR a quarantine routing — never a clean-for-upload verdict
with residual PHI pixels.

**Acceptance Scenarios**:

1. **Given** a case from an unprofiled device, **When** de-id runs, **Then** the
   multipass detector + cross-frame consensus mask is applied and, if confidence is
   below threshold, the case is quarantined (not uploaded).
2. **Given** a multi-frame case with a static low-contrast overlay, **When** de-id
   runs, **Then** the cross-frame variance consensus identifies the overlay region
   even though single-frame OCR returns nothing.
3. **Given** any case where detected text matches an MRN/date/name pattern outside
   a maskable region, **When** de-id runs, **Then** the case is quarantined.

---

### User Story 3 - Benign-marker preservation (Priority: P2)

Laterality and view markers (`L`, `R`, `PA`, `AP`, kVp readouts) are **not** PHI
and must survive de-id so the image stays clinically usable.

**Why this priority**: Over-masking degrades research value. FN=0 is mandatory;
FP should be minimized where it can be done without risking a miss.

**Independent Test**: Feed a frame with both PHI strings and benign markers; assert
PHI boxes masked, benign markers preserved.

**Acceptance Scenarios**:

1. **Given** a frame containing both `DOE^JOHN MRN 00471123` and a `L` laterality
   marker, **When** de-id runs, **Then** the PHI string is masked and the `L` marker
   is preserved.

---

### User Story 4 - Batch throughput (Priority: P2)

An operator uploads a batch of up to 200 image-sized cases. Pixel de-id completes
for the whole batch at upload time on a CPU-only machine within an acceptable
wall-clock window, parallelized across cores.

**Why this priority**: Upload-time, not interactive — but must finish in minutes,
not hours, or it blocks the workflow.

**Independent Test**: Time a 200-case synthetic batch end-to-end on CPU; assert it
completes within the SC-005 budget.

**Acceptance Scenarios**:

1. **Given** a 200-case batch, **When** pixel de-id runs on a multi-core CPU,
   **Then** it completes within the SC-005 wall-clock budget with no GPU.

---

### Edge Cases

- Single-frame case (no cross-frame consensus available) → fall back to
  profile + multipass detector; quarantine on low confidence.
- Color/inverted (MONOCHROME1) frames → normalize photometric before detection.
- Text overlapping anatomy (not on a flat banner) → detector + dilation; quarantine
  if the maskable region would destroy diagnostic content (flag for human).
- Device profile present but firmware moved the overlay → union with detector
  catches drift; profile-vs-detector disagreement beyond tolerance → quarantine.
- Empty / corrupt pixel data → skip pixel de-id, do not mark clean, route to error.
- Multi-frame case where the overlay is NOT static (rare burned-in timestamp ticking
  per frame) → variance consensus misses it → detector multipass + quarantine.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST replace the always-`True` `needs_pixel_review` placeholder
  with a tiered pixel-PHI assessment returning one of: `clean`, `redacted`, or
  `quarantine`.
- **FR-002**: System MUST maintain a **device-profile registry** mapping device
  identity (model / private-block profile) to text-bearing overlay regions, and
  blind-mask those regions for profiled devices (contrast-independent).
- **FR-003**: System MUST, for multi-frame cases, compute a **cross-frame variance
  consensus mask** that identifies static bright overlay regions independent of text
  contrast, and add it to the redaction union.
- **FR-004**: System MUST run a **multipass text detector** (original + inverted +
  contrast-stretched + CLAHE) and union all detected text boxes into the redaction set.
- **FR-005**: System MUST classify detected text as PHI vs benign using a PHI
  recognizer (names, MRN, dates, DOB, facility) so benign markers (laterality/view)
  are preserved (FR-009).
- **FR-006**: System MUST **fail closed** — route a case to **quarantine** (not
  upload) when: device unprofiled AND detector confidence below threshold; OR a
  PHI-pattern match falls outside a maskable region; OR profile/detector disagreement
  exceeds tolerance.
- **FR-007**: System MUST dilate every redaction box by a configurable margin to
  cover anti-aliasing / descenders before masking.
- **FR-008**: System MUST apply redaction as an irreversible pixel fill on a copy,
  writing the de-identified pixels back to the DICOM, and MUST record an audit-log
  entry (regions masked, method tier, PHI categories found) WITHOUT storing the PHI
  text itself.
- **FR-009**: System MUST preserve benign laterality/view/technique markers that the
  PHI recognizer classifies as non-PHI.
- **FR-010**: System MUST be **CPU-only** — no GPU dependency in any tier.
- **FR-011**: System MUST process batches of up to 200 image-sized cases at upload
  time, parallelized across CPU cores.
- **FR-012**: Human review MUST occur only at the **quarantine boundary**, never as a
  per-image or per-case default gate; the clean/redacted paths require no human input.
- **FR-013**: System MUST integrate with the existing upload flow as a gate: a case
  may not upload until its pixel-de-id verdict is `clean` or `redacted`; `quarantine`
  blocks upload pending operator action.
- **FR-014**: All test fixtures MUST be **synthetic** burned-in PHI; no real PHI,
  no production endpoints.

### Key Entities *(include if feature involves data)*

- **Device Profile**: device identity → list of overlay redaction regions + tolerance;
  the contrast-independent trust anchor.
- **Redaction Set**: union of profile boxes + variance-consensus mask + detector boxes,
  dilated; the regions actually masked on a frame/case.
- **PHI Finding**: a detected text region + its recognizer classification (category,
  confidence); drives mask-vs-preserve and quarantine decisions. PHI text is not persisted.
- **De-id Verdict**: per-case `clean | redacted | quarantine` + audit metadata
  (tiers fired, region count, categories, no raw PHI).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: On a labeled synthetic holdout (high- and low-contrast, profiled and
  unprofiled, single- and multi-frame), **PHI-pixel false-negative rate = 0** — every
  case with residual PHI is either fully masked or quarantined; **no clean/redacted
  verdict leaks PHI pixels**. (This is the hard gate; the spike's 0.00 low-contrast
  single-pass recall is the baseline this must beat.)
- **SC-002**: Quarantine rate on profiled, in-distribution cases is low (target under
  10%) — most cases auto-resolve without human review (FR-012).
- **SC-003**: Benign laterality/view markers preserved in at least 90% of frames where
  they appear (FP-control, subordinate to SC-001).
- **SC-004**: The de-id verdict and audit entry contain no raw PHI text (privacy of the
  pipeline itself).
- **SC-005**: A 200-case synthetic batch completes pixel de-id on a CPU-only multi-core
  machine within an acceptable upload-time budget [NEEDS CLARIFICATION: exact wall-clock
  ceiling — minutes? target value?].
- **SC-006**: Offline suite green + the SC-001 holdout runs as a CI regression gate
  (CPU lane, no GPU).

## Assumptions

- Fixtures are synthetic burned-in PHI; the validated-zero-FN claim is measured on a
  curated synthetic holdout, refreshed as new device profiles are added.
- Device identity is derivable from DICOM tags / the existing private 0x0019 block for
  the profiled path; unprofiled devices fall to the catch-all + quarantine.
- Plug-and-play CPU tooling is acceptable: Microsoft Presidio (Image Redactor +
  Analyzer) supplies the OCR + PHI-NER tiers; Tesseract is the OCR engine; OpenCV
  provides the classical preprocessing. The novel glue (device registry, cross-frame
  consensus, fail-closed routing) is built in-repo.
- "Image-sized case" means a small per-case frame count (image-scale, not full OR
  video); large-video de-id is out of scope for v1.
- Advanced learned detectors (CRAFT/DBNet/docTR via ONNX-CPU) are a pluggable upgrade
  to the detector tier, not required for v1 if Presidio+multipass meets SC-001 on the
  holdout.
- Quarantine UX / operator review tooling reuses the existing upload-flow confirmation
  gate; building a new review UI is out of scope here.
