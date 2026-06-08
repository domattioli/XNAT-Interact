# Feature Specification: Data Identity & Deduplication (009)

**Feature Branch**: `claude/codebase-improvement-plan-sGjxz`
**Created**: 2026-06-08
**Status**: Draft (spec only — not yet planned/built)
**Input**: [`docs/DATA_MODEL.md`](../../docs/DATA_MODEL.md) (the scientific + privacy model) and
[`docs/METADATA.md`](../../docs/METADATA.md). Implements the deferred identity/dedup/registry work
those documents specify. Related: [#32](https://github.com/domattioli/XNAT-Interact/issues/32)
(dedup/identity grill), [#33](https://github.com/domattioli/XNAT-Interact/issues/33) (audit),
[#34](https://github.com/domattioli/XNAT-Interact/issues/34) (future Postgres migration).

## Overview

The ingest pipeline currently mis-handles identity in ways that **invalidate the science**:
all DICOM UIDs are collapsed to one value (`xnat_experiment_data.py:510/513/516`), the case key
(`StudyInstanceUID`) is destroyed, surgeon identity is stored in cleartext, and duplicate
detection runs on a brittle normalize+resize pixel hash backed by an O(n) JSON list. Per
`DATA_MODEL.md`, these break **surgeon learning curves** (primary goal) and **ML train/test
leakage avoidance** (secondary goal), and leak re-identifying data.

This feature implements the corrected model: **content-authoritative identity + layered
deduplication**, backed by a **SQLite registry** that replaces the `ConfigTables` JSON
pseudo-database. It is behavior-correcting (fixes wrong identity handling) and substrate-changing
(registry), not a new user feature.

Out of scope here (tracked separately): the **advanced automated pixel-PHI de-identification**
upgrade (`DATA_MODEL.md` §4.2 — its own feature), the **Postgres** migration (#34), and the
**arthroscopy / simulation** sibling tracks.

## Clarifications (resolved in the #32 grill; see DATA_MODEL §3–§8)

1. **Authoritative keys are content, not UIDs.** Source DICOM UIDs cannot be trusted
   (re-export/anonymizers regenerate them) but are kept as **corroborating** evidence. Image
   identity = `sha256(raw PixelData)`; case identity = the set of image content-hashes.
2. **Patient identity is destroyed; surgeon identity is a keyed-HMAC pseudonym** with a
   librarian-only crosswalk. No patient linkage.
3. **Duplicates are never auto-removed.** Within-case near-dup shots are *kept* (shot count is a
   skill metric) with an informational flag. Case overlap produces an **evidence package for a
   human** (import both / combine / other) — never auto-merge/auto-reject. Empty Subject/
   Experiment/Scan shells are never created.
4. **Registry = SQLite sidecar now** (schema, `UNIQUE`-indexed dedup, transactions), replacing the
   ConfigTables JSON blob; Postgres deferred (#34).
5. **Dates are hashed for dedup, never stored readable** (date-of-service is a HIPAA identifier).

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Correct, unique DICOM identity (Priority: P1)

Every uploaded frame keeps a **unique** `SOPInstanceUID`; the original Study/Series/SOP UIDs are
**preserved** (as corroborating metadata), not clobbered; the re-identifying `Old_StudyDate`
private tag is **stripped** after its dedup value is captured as a hash.

**Why first**: identity correctness is the foundation; dedup and the science depend on it.

**Independent Test**: ingest a multi-frame session offline → each output frame has a distinct
`SOPInstanceUID`; original UIDs are recoverable as corroborating metadata; no readable
`Old_StudyDate` remains; frame ordering (InstanceNumber) is intact.

**Acceptance Scenarios**:
1. **Given** a session of N frames, **When** ingested, **Then** there are N distinct
   `SOPInstanceUID`s (not 1).
2. **Given** a source with original Study/Series UIDs, **When** ingested, **Then** those values
   are preserved as corroborating metadata and not overwritten with `intake_form.uid`.
3. **Given** a source `StudyDate`, **When** ingested, **Then** no readable original date persists
   on the uploaded object (only its dedup hash exists in the registry).

---

### User Story 2 — Layered content-based deduplication (Priority: P1)

Duplicate images and cases are detected on **image content** (authoritative), with UIDs
corroborating and a perceptual hash as an advisory flag; case overlap yields an evidence package
for a human decision; no data is ever silently dropped.

**Why**: protects learning-curve integrity (no double-counted cases) and ML split hygiene (no
leakage), without discarding legitimate near-identical skill-signal shots.

**Independent Test**: seed the registry with case A; ingest exact-dup / subset / superset /
partial / disjoint variants → exact/subset/superset/partial against a *different* subject produce
an evidence package (no auto-import); disjoint imports clean; within-case near-dups are flagged
but **kept**.

**Acceptance Scenarios**:
1. **Given** an image already in the index (by content hash), **When** re-ingested, **Then** it is
   detected as a duplicate (UID match raises confidence, is not required).
2. **Given** an incoming case whose image-hash set overlaps an existing case under a *different*
   subject, **When** ingested, **Then** an evidence package is produced (overlap ratio, matching
   shots, UID/date/device corroboration) and a human chooses the action — nothing auto-merges.
3. **Given** near-identical consecutive shots in one case, **When** ingested, **Then** they are
   flagged (`IS_QUESTIONABLE`) but **all retained** (count preserved).
4. **Given** any ingest that would create a Subject/Experiment/Scan with zero landed files,
   **When** processed, **Then** no empty shell is created.

---

### User Story 3 — SQLite registry replaces ConfigTables (Priority: P1)

The surgeon/rater registry, dedup index, and audit trail live in a **single-file SQLite database**
with schema, foreign keys, a `UNIQUE`-indexed content-hash column, and transactional writes,
replacing the JSON-blob-on-XNAT `ConfigTables`.

**Why**: the JSON blob is O(n) membership + whole-blob rewrite (scale wall) with a TOCTOU
lost-update guard (#33 H4); SQLite gives O(1) dedup membership, atomic writes, and real queries.

**Independent Test**: a dedup-membership check is O(1) via the `UNIQUE` index; concurrent
in-session writes are transactional (no lost update within a writer); existing ConfigTables JSON
migrates into the schema without data loss.

**Acceptance Scenarios**:
1. **Given** the SQLite registry, **When** a content hash is checked, **Then** membership resolves
   via the `UNIQUE` index (no full-table scan, no blob rewrite).
2. **Given** an existing ConfigTables JSON, **When** migrated, **Then** `surgeons`/`subjects`/
   image-hashes land in the schema and are queryable.
3. **Given** a write, **When** it fails mid-operation, **Then** the transaction rolls back (no
   partial/lost update).

---

### User Story 4 — Keyed-pseudonym identity for surgeon; patient destroyed (Priority: P2)

Surgeon identity in the dataset is `HMAC(salt, hawkid)` (stable, irreversible without the salt);
the `pseudonym ↔ HawkID` crosswalk + salt live in a **separate, access-controlled store**; patient
identifiers are destroyed (no pseudonym).

**Why**: enables longitudinal learning curves without exposing professionally-sensitive named
skill scores; strongest patient de-id.

**Independent Test**: the same HawkID yields the same pseudonym across cases; the operational
registry contains no real names; patient identifiers are absent from all outputs; the crosswalk is
not in the operational DB or source.

**Acceptance Scenarios**:
1. **Given** two cases by the same surgeon, **When** ingested, **Then** both carry the same
   pseudonym; the real HawkID appears only in the librarian-only crosswalk.
2. **Given** any uploaded object or registry row, **When** inspected, **Then** no patient
   identifier is present.

---

### User Story 5 — Versioning & run flexibility (Priority: P3)

Derived/assessor data uses keep-all monotonic versioning (`v(n+1)` never overwrites `vn`); `scan`
is a user-selectable parameter (default `'0'`) and the original `SeriesInstanceUID` is preserved
so run structure is reconstructible.

**Independent Test**: re-uploading a revised segmentation creates a new version without destroying
the prior; an uploader can direct data into a second scan; original SeriesInstanceUID survives.

**Acceptance Scenarios**:
1. **Given** an existing derived `vn`, **When** a revision is uploaded, **Then** `v(n+1)` is added
   and `vn` is retained.
2. **Given** an uploader choosing a non-default scan, **When** they upload, **Then** data lands in
   that scan; default remains `'0'`.

---

### Edge Cases
- Re-exported source with regenerated UIDs → content hash still detects the duplicate (UIDs
  disagree but don't override the content match).
- Same content hash legitimately recurring across genuinely different cases → surfaced in the
  evidence package for human adjudication, never auto-merged.
- Migration of a ConfigTables JSON containing the legacy normalize+resize hashes → carried as
  legacy corroborating data, superseded by raw-content hashes going forward.
- Registry file absent/locked → fail-soft with a friendly error; never silently skip dedup.

## Requirements *(mandatory)*

### Functional Requirements
- **FR-001**: Each uploaded instance MUST retain a unique `SOPInstanceUID`; the per-frame
  collapse (`xnat_experiment_data.py:510/513/516`) MUST be removed.
- **FR-002**: Original `StudyInstanceUID` / `SeriesInstanceUID` / `SOPInstanceUID` MUST be
  preserved as corroborating metadata, not overwritten with `intake_form.uid`.
- **FR-003**: The original `StudyDate`/`StudyTime` MUST be captured as a dedup **hash**
  (`HMAC(salt, date+device)`) and MUST NOT persist as a readable value on the uploaded object.
- **FR-004**: Image identity MUST be `sha256(raw PixelData)` (pre-processing); case identity MUST
  be the set of image content-hashes. Original UIDs MUST be used only as corroborating signals.
- **FR-005**: A perceptual hash MAY flag near-duplicates (advisory) but MUST NOT auto-reject.
- **FR-006**: Within-case near-duplicate shots MUST be retained and flagged
  (`IS_QUESTIONABLE` → private tag), never removed.
- **FR-007**: Case-level overlap MUST produce an evidence package (overlap ratio, matching shots,
  UID/date/device corroboration) for a human decision (import both / combine / other); the system
  MUST NOT auto-merge or auto-reject.
- **FR-008**: The pipeline MUST NOT create empty Subject/Experiment/Scan objects (filter first,
  create only when ≥1 file lands).
- **FR-009**: A SQLite registry MUST hold `surgeons`, `raters`, `cases`, `image_hashes`
  (`content_hash` UNIQUE), and an append-only `audit_log`, with foreign keys and transactional
  writes, replacing the ConfigTables JSON.
- **FR-010**: A one-time migration MUST import existing ConfigTables JSON into the SQLite schema
  without data loss.
- **FR-011**: Surgeon identity MUST be stored as `HMAC(salt, hawkid)`; the `pseudonym ↔ HawkID`
  crosswalk + salt MUST live in a separate, access-controlled store, never in the operational
  registry or source.
- **FR-012**: Patient identifiers MUST be destroyed (no pseudonym, no crosswalk).
- **FR-013**: Derived/assessor data MUST use keep-all monotonic versioning.
- **FR-014**: `scan` MUST be a user-selectable parameter (default `'0'`).
- **FR-015**: All paths MUST remain offline-testable and fail-soft; no creds/PHI in argv/logs.

### Key Entities
- **ImageIdentity**: `content_hash` (authoritative), `orig_sopuid` (corroborant), `instance_number`.
- **CaseIdentity**: content-hash set; `orig_studyuid` + `date_device_hash` (corroborants).
- **SurgeonPseudonym**: `HMAC(salt, hawkid)`; crosswalk held librarian-side.
- **Registry (SQLite)**: `surgeons`, `raters`, `cases`, `image_hashes`, `audit_log`.
- **DedupResult**: exact / subset / superset / partial / disjoint + evidence package.

### Success Criteria *(mandatory)*
- **SC-001**: An N-frame session yields N distinct `SOPInstanceUID`s (grep/parse test).
- **SC-002**: No readable original `StudyDate` persists on uploaded objects; its dedup hash exists
  in the registry.
- **SC-003**: Exact/subset/superset/partial duplicates (against a different subject) are detected
  on content hash and produce an evidence package; disjoint imports clean; within-case near-dups
  are retained. (Parameterized seed-set test, per DATA_MODEL §5.)
- **SC-004**: Dedup membership is `UNIQUE`-indexed (no full scan); ConfigTables JSON migrates with
  zero data loss.
- **SC-005**: The operational registry + uploaded data contain no real surgeon names and no
  patient identifiers; the same HawkID maps to a stable pseudonym.
- **SC-006**: Full existing offline suite stays green; new behavior covered by offline tests, with
  the data-integrity cases promotable to the `RUN_XNAT_DUAL=1` live lane.

### Assumptions
- The raw `PixelData` is available pre-processing for content hashing (true for source DICOM).
- The `ConfigTables` JSON is the only legacy registry to migrate.
- A salt-management mechanism (librarian-held) exists or is provisioned for the keyed pseudonyms.

## Out of Scope (this feature)
- Advanced automated pixel-PHI de-identification (DATA_MODEL §4.2) — separate feature.
- Postgres migration (#34).
- Arthroscopy and simulation sibling tracks.
- Modeling multi-run sessions as distinct scans (only the user-selectable param + SeriesUID
  preservation are in scope).
