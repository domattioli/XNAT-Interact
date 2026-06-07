# Contract Test Design: FakeXNAT ↔ Real XNAT Behavioral Parity

**Phase**: 006-xnat-alignment (support document)
**Date**: 2026-06-05
**Objective**: Ensure FakeXNAT and real XNAT produce identical outputs for realistic grad-student workflows. Serves as durable verification gate after Phase 7 offline fixes.

---

## Problem

Phase 7 fixed five defects offline (742 tests green) but T006/T020 (real-XNAT verification) were blocked on environment setup. Instead of one-off harness, we pivot to a **durable contract test** that locks in FakeXNAT ≈ real XNAT and prevents future regressions.

---

## Workflow Model

Grad-student simulation: realistic multi-step usage pattern, not just push-or-pull.

### Flow: Input → Analyze → Upload → Revise

```
1. INPUT CASE
   ├─ Librarian uploads de-identified RF session (raw DICOMs)
   └─ Server: Project → Subject → Experiment(xnat:rfSessionData) → Scan(0) → Resource(SOURCE_DATA) → files

2. DOWNLOAD CASE
   ├─ Analyst downloads the source dataset for analysis
   └─ Server: Resource enumeration → N actual files (count-verified)

3. UPLOAD DERIVED
   ├─ Analyst processes images, uploads consensus segmentation
   └─ Server: new Experiment(xnat:assessorData) OR Resource(SEGMENTATION_CONSENSUS) → files

4. REVISE AFTER RE-ANALYSIS
   ├─ Analyst re-downloads source, applies new parameters, overwrites derived
   └─ Server: existing Experiment/Resource updated (idempotent upsert) OR new versioned resource

5. CLEANUP
   └─ Verify orphaned/partial states don't exist; final state matches spec
```

---

## Synthetic Data Schema

### DICOM Factory

Generates a minimal but valid RF DICOM with configurable metadata.

**Parameters**:
- `num_frames`: number of slices (default 3)
- `instance_start`: InstanceNumber base (default 1)
- `modality`: 'RF' (default), or 'CT'/'US' for mixed-modality test
- `missing_tags`: set of tags to omit (e.g. `{'InstanceNumber'}` to test Phase 7 #30 guard)

**Produces**: bytes (valid pydicom DICOM), writable to `/tmp/*.dcm`

Example:
```python
dcm = synthetic_dcm(num_frames=5, modality='RF')
dcm_missing_instance = synthetic_dcm(num_frames=3, missing_tags={'InstanceNumber'})
```

### Session Metadata

Each test case seeds:
- **Subject**: `ITEST_SUBJ_{N:04d}`, label (display name)
- **Experiment**: `ITEST_EXP_{N:04d}`, xsiType (`xnat:rfSessionData`)
- **Scan**: ID `0`, xsiType `xnat:rfScanData`
- **Resource**: `SOURCE_DATA`, N real files

Derived:
- **Assessor**: `xnat:assessorData`, `SEGMENTATION_CONSENSUS` or `RECONSTRUCTION_V2`, versioned

---

## Test Harness

### Structure: Comparative Test

```
def test_workflow_[case](fake_xnat, real_xnat):
    """
    1. Setup: stage synthetic data (same seed → same DICOMs)
    2. Run workflow against fake_xnat
    3. Record all server state (exists, attrs, resources, files)
    4. Run same workflow against real_xnat
    5. Assert: outputs match exactly (server state, files, error types)
    6. Cleanup: real_xnat only
    """
```

### Test Cases

#### T001: Input Case (Source Upload)
- **Setup**: 1 synthetic RF DICOM (3 frames)
- **Workflow**: `publish_to_xnat(session)` → creates Subject, Experiment, Scan, uploads zip
- **Asserts**:
  - Server has exactly 1 Subject, 1 Experiment, 1 Scan
  - Scan Resource `SOURCE_DATA` holds 3 files
  - File count matches server's `# Files` endpoint
  - FakeXNAT and real XNAT server state match (attrs, resource metadata)

#### T002: Download Case (Source Retrieval)
- **Setup**: Pre-stage Subject/Experiment/Scan with 5-file Resource (T001 state)
- **Workflow**: `download_resource(scan='0', label='SOURCE_DATA')` → fetch all files
- **Asserts**:
  - Returns 5 files (not 1 synthesized)
  - File count matches server `# Files`
  - File bytes match originals (checksum)
  - FakeXNAT and real XNAT return identical file lists and content

#### T003: Upload Derived (Assessor or Resource)
- **Setup**: Subject/Experiment from T001; analyst has processed result (synthetic derived DICOM)
- **Workflow**: `publish_to_xnat(assessor=consensus_dicom, label='SEGMENTATION_CONSENSUS')`
- **Asserts**:
  - Creates new Assessor OR Resource with label `SEGMENTATION_CONSENSUS`
  - Derived file lands on server
  - FakeXNAT ≈ real XNAT (choose: assessor vs. resource based on gateway design)

#### T004: Revise After Re-analysis
- **Setup**: Pre-existing Experiment with SOURCE_DATA + SEGMENTATION_CONSENSUS from T001+T003
- **Workflow**:
  1. Download SOURCE_DATA
  2. Re-analyze (synthetic reprocess)
  3. Overwrite SEGMENTATION_CONSENSUS (idempotent upsert)
- **Asserts**:
  - Final state has 1 Experiment, 1 SEGMENTATION_CONSENSUS (no duplicate resources)
  - New file bytes differ from old (re-analysis visible)
  - FakeXNAT and real XNAT produce identical state

#### T005: Edge Case — Mixed Modality
- **Setup**: Project with RF + CT experiments
- **Workflow**: `list_downloadable(project)` → enumerate all; download each
- **Asserts**:
  - Both RF and CT appear in listing
  - Subject labels resolve for both
  - FakeXNAT ≈ real XNAT enumeration

#### T006: Edge Case — Orphaned/Partial Subject (Idempotent Upsert)
- **Setup**: Pre-seed orphaned Subject (exists but no Experiments)
- **Workflow**: `publish_to_xnat(session)` → reuse orphaned, add Experiment
- **Asserts**:
  - Final state has 1 Subject (not 2)
  - FakeXNAT ≈ real XNAT cleanup behavior

#### T007: Edge Case — Missing Tag Guard (#30)
- **Setup**: Synthetic DICOM with no `InstanceNumber`
- **Workflow**: `publish_to_xnat(session)` → create Scan
- **Asserts**:
  - No crash (Phase 7 #30 guard)
  - Session metadata mines a default instance index
  - FakeXNAT ≈ real XNAT error handling

---

## Fidelity Audit Matrix

Track divergences between FakeXNAT and real XNAT.

| Behavior | FakeXNAT | Real XNAT | Design Choice or Bug | Notes |
|----------|----------|-----------|----------------------|-------|
| Post-`create()` datatype cache | Filled (Phase 7 fix) | Empty until set | FakeXNAT reproduction | Phase 7 #27; verified T001 |
| Resource `# Files` endpoint | Reports count | Reports count | Aligned | Phase 7 #25; verified T002 |
| Subject label resolution | Returns label + internal ID | Returns both | Aligned | Phase 7 #29 |
| Experiment enumeration (type-agnostic) | Filters xsiType | Lists all types | Aligned | Phase 7 #29 |
| Idempotent `create()` on existing | Reuses handle | Reuses handle | Aligned | Phase 7 #27; verified T006 |
| Resource registry lookup (plural/singular QS) | Unified registry (Phase 6 Stage 1) | N/A (real XNAT) | FakeXNAT enhancement | Contract tests use both `/project/` and `/projects/` formats; FakeXNAT now normalizes both via _parse_resource_qs |
| Multi-file resource enumeration | list_files() returns all N | enumerate endpoint lists N | Aligned | Phase 7 #25; verified T002 |
| Assessor vs. Resource | Aligned (assessor, T003 verified, fake-side) | Real-side parity Stage 5 (dual-run) | Design choice — assessor path chosen per C001 | `gateway.create_assessor` wired; T003 contract tests live; Stage 5 dual-run discharges real-side |
| Versioning overwrite (no dupes) | put_zip(..., overwrite=True) → same resource | Same | Aligned | T004 verified idempotent overwrite |
| Missing-tag guard (InstanceNumber) | No crash, default used | No crash, default used | Aligned | Phase 7 #30; verified T007 |

---

## Implementation Roadmap

### Stage 0: Synthetic Data Factory
- [ ] `tests/conftest.py`: `synthetic_dcm(num_frames, modality, missing_tags)` generator
- [ ] `tests/fixtures/`: parameterized DICOM templates (RF, CT, US; with/without tags)
- [ ] Seed dataset factory: `stage_experiment(...)` → pre-creates Subject/Experiment/Scan/Resource

### Stage 1: Dual-XNAT Harness
- [ ] `tests/contract/`: pytest fixtures for both fake and real XNAT
- [ ] Real XNAT fixture: Docker container startup/teardown (reuse Phase 7's `xnat_local/` image)
- [ ] Comparative test decorator: `@contract_test` runs same test against both

### Stage 2: Test Cases (T001–T007)
- [ ] Each test case in `tests/contract/test_*.py`, parameterized `(fake_xnat, real_xnat)`
- [ ] Per-case: setup → workflow → dual-run → assert parity → cleanup

### Stage 3: Fidelity Gap Resolver
- [ ] Document findings in this audit matrix
- [ ] Gaps = either design choices (accept divergence) or fidelity bugs (patch FakeXNAT)
- [ ] No Phase 6 code shipped until all gaps resolved or documented as deferred

### Stage 4: Integration with Phase 6 Refactor
- [ ] After gateway ABC built, run contract test against new `XnatGateway` + `FakeGateway`
- [ ] Assert: existing behavior preserved (test cases still pass)

---

## Success Criteria

- **SC-001**: All seven test cases (T001–T007) pass against both FakeXNAT and real XNAT.
- **SC-002**: Fidelity audit matrix has zero undocumented divergences; gaps are either resolved or marked as design choice / deferred.
- **SC-003**: Contract test runs in CI (with real XNAT skipped if no Docker) and locally (full dual-run).
- **SC-004**: After Phase 6 gateway implementation, contract test still passes (behavior preserved).
- **SC-005**: Documentation: `docs/CONTRACT_TEST.md` explains the workflow model, synthetic data schema, and how to run the harness.

---

## Scope Limits (What This Is NOT)

- NOT a user-feature test (e.g. "analyst can download"). Tests internal parity, not UX.
- NOT a performance benchmark (though timing could be added later).
- NOT a migration test (existing XNAT layouts unchanged).
- NOT a gateway implementation (just the test framework).

---

## Timeline

- **Week 1**: Synthetic data factory + dual-harness fixture (Stage 0–1). Estimate: 2–3 hours.
- **Week 2**: Test cases T001–T007 (Stage 2). Estimate: 3–4 hours (8 tests × ~30 min each).
- **Week 3**: Fidelity gap resolution + docs (Stage 3–4). Estimate: 2–3 hours.
- **Integration**: Parallel with Phase 6 gateway build; no blocking path.

---

## Deferred / Out of Scope

- **Multi-scan sessions**: Phase 7 used single-scan (`0`) simplification. Future multi-series is Phase 6+ scope.
- **Assessor lifecycle**: Which assessor fields/versions to track. Clarified by gateway design.
- **Annotation round-trip**: Segmentations/markup stored as resources; assessor storage is Phase 5 (STAPLE) scope.

---

## Fidelity Divergence Audit Matrix

Gaps confirmed during dual-run against XNAT 1.9.3 (build 199, 2025-11-21).

| ID | Area | FakeXNAT behavior | Real XNAT behavior | Root cause | Resolution |
|---|---|---|---|---|---|
| GAP-001 | Assessor file upload (T003) | `create_assessor(files=[...])` stages files in `_staged_files`; `list_files()` returns them | XNAT 1.9.3 returns HTTP 404 on `file.put` to assessor resources regardless of resource creation order | XNAT 1.9.3 server version limitation — assessor file upload via `file.put` not supported; newer XNAT versions support it | T003 dual-run xfails on `pyxnat.DatabaseError` (catches 1.9.3 404). Fake-side assertions pass. Document here; revisit when testing on XNAT 1.9.4+. |
