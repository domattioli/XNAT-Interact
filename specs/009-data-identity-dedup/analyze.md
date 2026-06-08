# Analysis: Data Identity & Deduplication (009)

Cross-consistency check of [spec.md](spec.md) ↔ [plan.md](plan.md) ↔ [tasks.md](tasks.md) ↔
codebase, run before planning to build. Grounded against the current source.

## Coverage matrix (spec → tasks)

| Requirement | Task(s) | Status |
|---|---|---|
| FR-001 unique SOPInstanceUID | T011 | ✓ |
| FR-002 preserve Study/Series/SOP UIDs | T011 | ✓ |
| FR-003 strip `Old_StudyDate`, hash date | T001, T012 | ✓ |
| FR-004 content-hash image + case | T001, T007 | ✓ |
| FR-005 perceptual hash advisory-only | T007 | ✓ |
| FR-006 keep near-dup shots | T010 | ✓ |
| FR-007 evidence package, no auto-act | T008, T013 | ✓ |
| FR-008 no empty shells | T013 | ✓ |
| FR-009 SQLite registry schema | T003 | ✓ |
| FR-010 migration from ConfigTables JSON | T004 | ✓ |
| FR-011 surgeon pseudonym + separate crosswalk | T001, T015 | ⚠ (F2 — no salt source) |
| FR-012 patient destroyed | T015 | ⚠ (F4 — verify existing de-id) |
| FR-013 keep-all derived versioning | T016 | ✓ |
| FR-014 user-selectable scan | T014 | ✓ |
| FR-015 offline-testable / fail-soft | gates | ✓ |

| Success criterion | Task(s) | Status |
|---|---|---|
| SC-001 N distinct SOPInstanceUIDs | T011 + Stage-4 gate | ✓ |
| SC-002 no readable StudyDate | T012 + gate | ✓ |
| SC-003 seed-set dedup + evidence | T009, T010 | ✓ |
| SC-004 UNIQUE-index dedup + zero-loss migration | T005 | ✓ |
| SC-005 no names / no patient ids / stable pseudonym | T015 + gate | ✓ |
| SC-006 offline green + live-lane data-integrity | T017, T018 | ✓ |

All FRs/SCs are covered. Two are flagged (F2, F4). No orphan tasks; no orphan requirements.

## Findings

### F1 — HIGH: `ConfigTables` surface is large; `batch_upload.py` missing from the plan
`grep` shows **73 references across 5 files**: `xnat_experiment_data.py`, `xnat_scan_data.py`,
`utilities.py`, `xnat_resource_data.py`, **`batch_upload.py`**. The plan's T006 "thin read-path
shim" understates this, and `batch_upload.py` is **not** in the plan's modify list.
**Action:** treat T006 as "caller inventory + shim across all 5 files"; add `batch_upload.py` and
`xnat_scan_data.py` to the modify set; keep the `ConfigTables` public surface so the 73 call sites
resolve unchanged through the registry underneath. (Resolved below.)

### F2 — HIGH: no salt mechanism exists; FR-011's assumption is false
`grep` finds **no `salt`/`HMAC`/secret-key** mechanism in `src/` (only XNAT URL/project env vars).
The keyed pseudonym (FR-011) needs a librarian-held salt source — it is **unbuilt**, not a given.
**Action:** add an explicit salt-provisioning task before pseudonym use: source the salt from a
librarian-only path (env var `XNAT_IDENTITY_SALT` or a restricted file), never defaulted, never
logged; tests use a throwaway salt. (Resolved below — new T001b; Stage 1.)

### F3 — MED: crosswalk store is interface-only
FR-011's `pseudonym ↔ HawkID` crosswalk is specified as an *interface* (separate, librarian-only,
encrypted) but no concrete encrypted store is chosen. **Acceptable for 009** (the operational DB
holds only pseudonyms; the crosswalk can be a librarian-managed artifact), but the concrete
encrypted-store implementation is a **follow-up decision**, noted in scope. No task change.

### F4 — LOW: FR-012 may be partly satisfied already (verify-first)
`deidentify.py` already redacts PN-VR (PatientName) and tags. FR-012 (patient destroyed) may be
largely covered; T015 should **verify-first** and only add what's missing (e.g. confirm no
`PatientID`/derived patient field survives), not re-implement existing de-id.

### F5 — OK: structure sound
Dependency graph is acyclic; Stage 4/5 same-file serialization is correctly flagged; offline-first
+ live-lane promotion is consistent with the established test architecture; no new runtime deps.

## Resolutions applied
- **T006 scope expanded** to a 5-file caller inventory + shim (incl. `batch_upload.py`,
  `xnat_scan_data.py`).
- **New T001b** (Stage 1): salt provisioning (librarian-only source; throwaway in tests).
- **T015 marked verify-first** for FR-012 (don't re-implement existing PN-VR de-id).
- **F3** recorded as an in-scope note (crosswalk interface now; concrete encrypted store = follow-up).

## Verdict
**Build-ready** after the above resolutions. No blocking ambiguity; the two HIGH findings are
scoping corrections, not redesigns. Highest-risk work remains the Stage-4 metadata-block surgery
(mitigated by verify-first + the seed-set dedup tests + the live dual-run lane).
