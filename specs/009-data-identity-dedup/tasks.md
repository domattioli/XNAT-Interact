# Tasks: Data Identity & Deduplication (009)

**Input**: [spec.md](spec.md), [plan.md](plan.md). Suite green at EVERY commit. Offline-first;
data-integrity cases promotable to `RUN_XNAT_DUAL=1`. **Tier**: **[H]** haiku, **[S]** sonnet.
**Verify-first**: read current code before changing — several audit "bugs" were already fixed.

## Stage 1 — Identity primitives (BLOCKS 2–5)

- [ ] T001 [S] `src/services/identity.py`: `image_content_hash(ds)` = `sha256(raw PixelData)`
  pre-processing; `surgeon_pseudonym(hawkid, salt)` = `HMAC`; `case_date_hash(date, time, device,
  salt)`; UID-corroboration helpers. Pure, no side effects.
- [ ] T002 [S] `tests/test_identity.py`: content-hash determinism + sensitivity (same pixels→same,
  1-pixel diff→different); pseudonym stability (same hawkid→same) + irreversibility (no salt→can't
  recover) + salt-sensitivity; date-hash determinism.
- [ ] T001b [S] Salt provisioning (analyze F2): source the pseudonym salt from a librarian-only
  path (`XNAT_IDENTITY_SALT` env var or restricted file) — never defaulted, never logged; tests
  use a throwaway salt. `identity.py` reads it; missing salt → `FriendlyError`.
- [ ] GATE offline suite green; new module imported nowhere yet.

## Stage 2 — SQLite registry + migration (BLOCKS 3, 5)

- [ ] T003 [S] `src/services/registry.py`: SQLite schema — `surgeons(pseudonym PK, role)`,
  `raters(rater_id PK, expertise_tier, reliability_weight)`, `cases(case_key PK, surgeon FK,
  procedure, date_hash, device)`, `image_hashes(content_hash UNIQUE, case_key FK, orig_sopuid,
  instance_number)`, `audit_log(append-only)`. Transactional CRUD; `exists(content_hash)` via the
  UNIQUE index. Separate crosswalk-store interface (path injected; librarian-only; NOT in this DB).
- [ ] T004 [S] `registry.migrate_from_configtables(json)` importer: ConfigTables JSON →
  schema; assert row-for-row parity vs source before cutover.
- [ ] T005 [S] `tests/test_registry.py`: schema creation; O(1) `exists` via UNIQUE index;
  transaction rollback on mid-write failure (no lost update); JSON migration zero-loss.
- [ ] T006 [S] ConfigTables→registry shim (analyze F1 — 73 refs across 5 files:
  `utilities.py`, `xnat_experiment_data.py`, `xnat_scan_data.py`, `xnat_resource_data.py`,
  `batch_upload.py`). Inventory the callers; keep the `ConfigTables` PUBLIC surface unchanged so
  all 73 sites resolve through `registry` underneath; add the migration entry point. Keep JSON
  readable as one-release fallback.
- [ ] GATE offline suite green; migration parity test passes.

## Stage 3 — Dedup engine (BLOCKS 4)

- [ ] T007 [S] `src/services/dedup.py`: image-level (`content_hash` authoritative + `orig_sopuid`
  corroborant + perceptual-hash advisory flag) and case-level (content-hash **set algebra** →
  exact/subset/superset/partial/disjoint vs the registry index). Never auto-acts.
- [ ] T008 [S] `dedup.build_evidence_package(...)`: overlap ratio, matching shots, UID/date/device
  corroboration → structured result for a human decision (import both / combine / other).
- [ ] T009 [S] `tests/test_dedup.py`: seed-set fixtures (per DATA_MODEL §5) — exact/subset/
  superset/partial/disjoint against a *different* subject; disjoint imports clean; UID match raises
  confidence but isn't required; re-export (UIDs differ, pixels same) still detected.
- [ ] T010 [H] `tests/test_dedup_evidence.py`: overlap → evidence package, NO auto-merge/reject;
  within-case near-dups flagged (`IS_QUESTIONABLE`) but **kept**; no empty Subject/Exp/Scan shell.
- [ ] GATE offline suite green; seed-set algebra covered.

## Stage 4 — Wire identity into ingest (the H1/UID fix) — serialize, same files

- [ ] T011 [S] `src/xnat_experiment_data.py` metadata block (~505–540): REMOVE the per-frame UID
  collapse (`:510/:513/:516`); assign **unique** `SOPInstanceUID` per instance; **preserve**
  original Study/Series/SOP UIDs as corroborating metadata (capture, don't overwrite with
  `intake_form.uid`). (FR-001/002)
- [ ] T012 [S] `src/services/deidentify.py`: after the date dedup-hash is captured, **strip
  `Old_StudyDate`** (and any readable original date) from the object. (FR-003)
- [ ] T013 [S] `xnat_experiment_data.py`: route ingest through `dedup`; on case overlap surface the
  evidence package (no auto-act); enforce the **no-empty-shell** invariant (filter→create). (FR-007/008)
- [ ] T014 [H] `xnat_experiment_data.py` + `xnat_conventions`: make `scan` a user-selectable
  parameter (default `'0'`); preserve original `SeriesInstanceUID`. (FR-014, US5)
- [ ] GATE offline suite green; SC-001 (N distinct SOPInstanceUIDs) + SC-002 (no readable
  StudyDate) pass.

## Stage 5 — Pseudonym identity + versioning — serialize, same files

- [ ] T015 [S] `src/xnat_resource_data.py`: derive `surgeon_pseudonym` at intake; write pseudonym
  (not name) into experiment attribution + registry. **Verify-first (analyze F4):** `deidentify.py`
  already redacts PN-VR — confirm no `PatientID`/derived patient field survives and add only what's
  missing; do NOT re-implement existing de-id. (FR-011/012)
- [ ] T016 [H] keep-all monotonic versioning for derived/assessor data (extend the
  `ann__{annotator}__{type}__v{n}` pattern to the assessor path). (FR-013)
- [ ] GATE offline suite green; SC-005 (no real names / no patient ids; stable pseudonym) passes.

## Stage 6 — Verify + close

- [ ] T017 [S] Full offline suite + `scripts/simulate_e2e.py` green; byte-diff representative
  writes; verify SC-001..SC-006.
- [ ] T018 [S] Promote data-integrity dedup/identity cases to the `RUN_XNAT_DUAL=1` live lane;
  confirm green against live XNAT.
- [ ] T019 [H] Update `specs/README.md` (009 row), `docs/DATA_MODEL.md` status (implemented items),
  and cross-ref #32/#33. Session introspection.

## Dependencies
- Stage 1 BLOCKS 2–5. Stage 2 BLOCKS 3 + 5. Stage 3 BLOCKS 4.
- Stages 4 + 5 edit the SAME files (`xnat_experiment_data.py` / `xnat_resource_data.py`) →
  **serialize**, Stage 4 before Stage 5. Stage 6 last.

## Definition of done
SC-001..SC-006 met: N distinct SOPInstanceUIDs (T011), no readable StudyDate (T012), seed-set
dedup with evidence package + kept shots + no empty shells (T009/T010/T013), UNIQUE-indexed dedup +
zero-loss migration (T005), no real names / no patient ids / stable pseudonym (T015), offline suite
green + live-lane data-integrity cases green (T017/T018).

## Out of scope (this feature)
Advanced pixel de-id (DATA_MODEL §4.2) · Postgres (#34) · arthroscopy/simulation tracks ·
multi-run-as-distinct-scans modeling.
