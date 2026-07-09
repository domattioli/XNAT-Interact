# Feature Specification: Real XNAT Surgical Integration Testing

**Feature Branch**: `015-xnat-surgical-integration-test`  
**Created**: 2026-07-08  
**Status**: Draft  
**Input**: User description: "Build comprehensive spec for real-XNAT integration testing with synthetic surgical data. Validate end-to-end pipeline against live XNAT server."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Ingest and De-identify Real Surgical Case (Priority: P1)

A quality assurance engineer needs to verify that surgical DICOM images with burned-in PHI (patient names, MRNs) are correctly uploaded to a live XNAT instance and de-identified without data loss. The engineer must confirm that PHI is removed from both metadata tags and pixel regions, while preserving all clinically relevant DICOM information needed for surgical analysis.

**Why this priority**: Data security (PHI redaction) is critical for regulatory compliance and patient privacy. This is the foundation all downstream phases depend on. Without verified de-identification, downstream workflows cannot proceed safely.

**Independent Test**: Full test of Phase 1 — upload raw surgical data with synthetic PHI, verify de-identification against live XNAT database, download processed data and inspect for PHI absence. Delivers: verified de-identification pipeline.

**Acceptance Scenarios**:

1. **Given** a DICOM file with burned-in PHI text ("DOE^JOHN MRN 00471123") in pixel array and metadata tags, **When** uploaded via pyxnat REST API to live XNAT, **Then** downloaded file has no PHI in metadata, pixel regions show redaction masks, and clinical landmarks remain intact.
2. **Given** multiframe DICOM sequences with variable metadata fields (some missing), **When** de-identified, **Then** all private tags removed, public tags sanitized (date shifted, UID regenerated), and frame count preserved.
3. **Given** concurrent uploads of 3 surgical cases to live XNAT, **When** de-identification pipeline runs, **Then** all 3 cases complete without data loss or cross-contamination.

---

### User Story 2 - Multi-annotator Annotation Workflow (Priority: P2)

A surgical researcher needs to collect annotations from multiple reviewers (landmarks, bounding boxes, free-form notes) on de-identified images. Annotations must be stored durably in the live XNAT instance, linked to source images, and accessible for downstream segmentation and consensus workflows.

**Why this priority**: Multi-annotator annotation enables quality assessment and consensus-based truth labels. This feeds directly into segmentation and STAPLE consolidation. Enables measurement of inter-observer variability.

**Independent Test**: Full test of Phase 2 — generate 3 sets of synthetic annotations per surgical case (simulated human reviewers), upload via XNAT annotation API, query live database to verify linkage to images, download and verify manifest integrity. Delivers: verified annotation storage and retrieval.

**Acceptance Scenarios**:

1. **Given** 3 independent annotators reviewing same surgical image, **When** each submits landmarks and bounding boxes via annotation API, **Then** all 3 annotations stored in XNAT with distinct reviewer IDs, timestamps, and confidence scores.
2. **Given** annotation manifest referencing 42 fluoroscopy frames, **When** queried against live XNAT database, **Then** 100% of referenced frames exist and annotations are retrievable via REST API.
3. **Given** concurrent annotation submissions from 3 reviewers on overlapping images, **When** submissions complete, **Then** no annotations are lost or corrupted (verified by checksum).

---

### User Story 3 - Multi-segmenter Segmentation and STAPLE Consolidation (Priority: P2)

A surgical AI researcher needs to collect segmentation masks from 3 independent segmentation models (rule-based, ML, hybrid), upload to live XNAT, and consolidate via STAPLE algorithm to produce a consensus mask with per-model performance scores. This enables assessment of segmentation quality and inter-model agreement.

**Why this priority**: STAPLE consolidation is the centerpiece of derived-data validation. Tests multi-resource upload, STAPLE computation, and result persistence. Enables measurement of model agreement and uncertainty quantification.

**Independent Test**: Full test of Phase 3 & 4 — generate 3 synthetic segmentation masks per surgical case (varied quality), upload to XNAT as separate derived resources, run STAPLE consolidation, store consensus result, query database to verify linkage. Delivers: verified segmentation pipeline and STAPLE integration.

**Acceptance Scenarios**:

1. **Given** 3 binary segmentation masks for same surgical image (from 3 models), **When** uploaded to XNAT and STAPLE consolidation runs, **Then** consensus mask generated with per-model sensitivity/specificity scores and stored as derived resource.
2. **Given** STAPLE output referencing original images and input masks, **When** queried via XNAT REST API, **Then** all links traversable and original data unchanged.
3. **Given** segmentation masks with variable quality (one model 95% accurate, one 60%, one 85%), **When** STAPLE runs, **Then** high-quality model weighted higher in consensus and performance scores reflect accuracy differences (verifiable by inspection).

---

### User Story 4 - Round-trip Integrity and Dedup Validation (Priority: P1)

A system integrator needs to verify that data survives the complete pipeline (upload → de-identify → annotate → segment → STAPLE → download) without loss or corruption. Checksums must match pre/post, annotations and segmentations must remain linked to images, and dedup identity resolution must prevent spurious UID collisions.

**Why this priority**: End-to-end integrity is critical. Ensures no silent data loss or corruption during complex multi-phase workflow. Without this, downstream analysis cannot be trusted.

**Independent Test**: Full test of Phase 5 — execute all 5 phases on 3 surgical cases, download final results, compute checksums, verify all linkages (image→annotation→segmentation→STAPLE), query dedup database for identity consistency. Delivers: verified round-trip integrity and dedup resolution.

**Acceptance Scenarios**:

1. **Given** raw DICOM file with SHA256 checksum, **When** uploaded and processed through full pipeline, **Then** downloaded de-identified file has different checksum (due to de-identification changes), but original checksum recoverable from metadata audit trail.
2. **Given** annotation on image A and segmentation on image A referencing same original UID, **When** queried post-STAPLE, **Then** both annotation and segmentation still reference image A and each other (links preserved).
3. **Given** UID collision scenario (RADIOFLUORO_2026 case with synthetic UID collisions), **When** dedup resolution runs against live database, **Then** correct identity determined and no spurious UID reuse (verified via pHash advisory).

---

### Edge Cases

- What happens when DICOM file has malformed headers or truncated pixel data? (HIP_2024 multimodality stress case)
- How does system handle faint/low-contrast burned-in PHI during pixel redaction? (HIP_2024 edge case)
- What happens when concurrent uploads modify XNAT state mid-download? (RADIOFLUORO_2026 concurrent stress)
- How does system handle missing metadata fields (variable frame rates, incomplete tags)? (HIP_2024 real-world variability)
- What happens when segmentation masks have mismatched dimensions? (Recovery + error reporting)

## Requirements *(mandatory)*

### Functional Requirements

**FR-001 (Setup)**: System MUST boot a real XNAT instance with SQL database backend, REST API responding to version endpoint, and TEST_PROJ project pre-created.

**FR-002 (Data Generation)**: System MUST generate 3 synthetic surgical cases with realistic medical imaging data:
- KNEE_2025: 42 fluoroscopy frames + 5 video sequences + 18 pre-op imaging files
- HIP_2024: 18 fluoroscopy frames + multi-modality imaging + variable metadata
- RADIOFLUORO_2026: 5+ imaging instances with identity collision scenarios + corrupted data

**FR-003 (Ingestion)**: System MUST upload synthetic surgical data to live XNAT server without data loss. All metadata fields must be preserved and queryable.

**FR-004 (De-identification)**: System MUST remove PHI from uploaded data:
- Metadata: sanitize identifiers (patient names, medical record numbers, dates)
- Pixel regions: redact burned-in text overlays (bright and faint cases)
- Clinical information (frame count, imaging modality, anatomy references) MUST be preserved

**FR-005 (De-id Verification)**: System MUST verify de-identification:
- No PHI in metadata (database inspection)
- Pixel redaction confirmed (visual inspection post-download)
- No identity duplicates (verified by querying database)

**FR-006 (Annotation Storage)**: System MUST store multi-annotator annotations:
- 3 independent reviewers per case
- Annotation types: landmarks, regions of interest, clinical notes
- Each annotation linked to source image with reviewer identity + timestamp + confidence

**FR-007 (Annotation Retrieval)**: System MUST retrieve stored annotations:
- Query and verify all annotations present
- Verify linkage to source images (100% present)
- Validate manifest integrity

**FR-008 (Segmentation Upload)**: System MUST upload 3 segmentation masks per surgical case:
- Diverse segmentation approaches (rule-based, learning-based, hybrid)
- Format: binary masks + labeled regions + confidence maps
- Store as separate resources linked to source images

**FR-009 (Segmentation Consolidation)**: System MUST consolidate 3 segmentation masks:
- Compute consensus representation
- Calculate per-method performance metrics
- Quantify agreement/uncertainty
- Store consolidated result linked to source images and input masks

**FR-010 (Consolidation Verification)**: System MUST verify consolidated segmentation:
- Consensus result downloadable and queryable
- Original masks + consolidated result linked and retrievable
- Performance metrics reflect input mask quality differences

**FR-011 (Round-trip Integrity)**: System MUST verify end-to-end data integrity:
- Checksum validation pre/post pipeline (accounting for de-identification changes)
- Annotation/segmentation linkages preserved through all phases
- Identity consistency verified (no spurious duplicates post-consolidation)
- Database state consistency confirmed

**FR-012 (Identity Resolution)**: System MUST resolve identity using multiple signals:
- Cryptographic hash (authoritative)
- Study identity correlation (corroborating)
- Perceptual hash advisory (confidence check)
- Verified against database

**FR-013 (Error Handling)**: System MUST gracefully handle:
- Malformed imaging files
- Truncated or corrupted data
- Concurrent state modifications
- Missing or variable metadata

**FR-014 (Concurrency)**: System MUST handle concurrent operations:
- 3 concurrent uploads without cross-contamination
- 3 concurrent annotation submissions without loss
- 3 concurrent segmentation uploads without collision

### Key Entities

- **Surgical Case**: Represents a single surgical procedure (KNEE_2025, HIP_2024, RADIOFLUORO_2026). Contains subject ID, experiment ID, date, modality, file set.
- **DICOM Image**: Individual DICOM frame or multiframe sequence. Has metadata tags (patient, study, series UIDs), pixel array with optional burned-in PHI, frame count.
- **Annotation**: Reviewer-supplied label (landmark, bounding box, note). Linked to image, reviewer ID, timestamp, confidence score.
- **Segmentation Mask**: Binary or label-map representation of anatomical regions. Generated by segmentation model, has model ID, confidence map, label vocabulary.
- **STAPLE Result**: Consensus segmentation produced by STAPLE algorithm. References 3 input masks, stores per-model performance scores, uncertainty map.
- **De-id Audit Trail**: Record of de-identification operations (metadata changes, PHI redaction, UID regeneration). Enables verification and rollback if needed.
- **Dedup Identity**: SHA256 + StudyUID + pHash for image identity resolution. Used to detect and resolve UID collisions.

## Success Criteria *(mandatory)*

### Measurable Outcomes

**SC-001 (De-identification Success)**: All 3 surgical cases successfully de-identified without data loss.
- Metric: 100% of DICOM files have PHI removed from metadata (verified by database query + tag inspection)
- Metric: 100% of burned-in PHI regions redacted in pixels (verified by visual inspection post-download)
- Metric: 0 UID collisions post-de-id (verified by querying live database)

**SC-002 (Annotation Completeness)**: All annotations stored and retrievable from live XNAT.
- Metric: 3 annotations per case = 9 total annotations stored successfully
- Metric: 100% of annotations linkable to source images (verified via REST API queries)
- Metric: 100% of annotation manifests pass checksum validation

**SC-003 (Segmentation Upload Success)**: All segmentation masks uploaded and consolidated via STAPLE.
- Metric: 3 masks per case × 3 cases = 9 masks uploaded without loss
- Metric: STAPLE consolidation produces consensus mask for all 3 cases
- Metric: Per-model performance scores computed and stored (0 failed STAPLE runs)

**SC-004 (Round-trip Integrity)**: End-to-end pipeline completes without data loss or corruption.
- Metric: Pre/post checksums match (after de-id changes accounted for)
- Metric: 100% of annotation/segmentation linkages preserved post-STAPLE (verified by database queries)
- Metric: Dedup identity consistent across all phases (no spurious UID reuse)

**SC-005 (Concurrency Stress)**: System handles concurrent operations without errors.
- Metric: 3 concurrent case uploads complete without cross-contamination (all 3 cases recoverable post-upload)
- Metric: 3 concurrent annotation submissions → 9 total annotations stored (0 lost/corrupted)
- Metric: 3 concurrent segmentation uploads + STAPLE → 3 consensus masks (0 failed operations)

**SC-006 (Edge Case Recovery)**: System gracefully handles malformed/corrupted data.
- Metric: HIP_2024 corrupted headers + truncated pixel data → system continues with recovery logging (0 silent failures)
- Metric: RADIOFLUORO_2026 UID collisions → dedup resolution succeeds (correct identity determined)
- Metric: Faint/low-contrast PHI (HIP_2024) → redaction succeeds (PHI unreadable post-redaction)

**SC-007 (Feature Completeness)**: All core features exercised through real XNAT integration.
- Metric: De-identification tested with bright + faint burned-in text, metadata sanitization, identity regeneration ✓
- Metric: Upload/Download tested with multiframe data, multi-modality, concurrent updates, corrupted files ✓
- Metric: Identity resolution tested with collision scenarios, hash-based detection, confidence checks ✓
- Metric: Annotation storage/retrieval tested with multi-reviewer workflows, manifest integrity ✓
- Metric: Segmentation storage/retrieval tested with diverse mask formats, linked resources ✓
- Metric: Consolidation tested with multi-input consensus computation, performance scoring, uncertainty quantification ✓
- Metric: Data discovery tested across all resource types (surgeries, scans, annotations, segmentations, consolidated results) ✓
- Metric: Link preservation tested end-to-end (annotations → images, segmentations → images, consolidated → input masks) ✓

## Assumptions

- **Real XNAT Availability**: A real XNAT instance (not mocked/simulated) is available and can be booted on localhost or a local network during testing. FakeXNAT is explicitly NOT used.

- **PostgreSQL Persistence**: PostgreSQL backend persists state across test phases. Session teardown: XNAT state saved to disk and reproducible on next run.

- **Network Access**: Local machine has unrestricted network access (no proxy blocking real XNAT downloads or REST API calls). Docker or local JVM + PostgreSQL can run without sandbox network restrictions.

- **Synthetic Data Realism**: Synthetic surgical data generators in `tests/synthetic_data.py` produce realistic DICOM/video with authentic metadata and burned-in PHI patterns (representative of real surgical cases).

- **Concurrency Model**: Real XNAT HTTP REST API and PostgreSQL support concurrent requests without race conditions or data corruption (tested via concurrent uploads/annotations/segmentations).

- **Identity Resolution**: Dedup identity (SHA256 + StudyUID + pHash) is authoritative for detecting and resolving UID collisions. No external authority needed.

- **STAPLE Algorithm**: STAPLE consolidation is available and correct (can be implemented via scikit-image or external library). Implementation details out of scope.

- **Error Messages**: Real XNAT REST API and database provide sufficient error messages for debugging (no silent failures). System logs all failures for inspection.

- **Scope Boundaries**: Multi-annotator/multi-segmenter simulation uses synthetic labels (not real human reviewers). Real human annotation workflows are future work.

- **Testing Environment**: All testing occurs on a single local machine (no cloud sandbox, no external dependencies, no network policy restrictions).
