# Tasks: Real-XNAT Round-Trip Correctness (Phase 7)

**Input**: [spec.md](spec.md), [plan.md](plan.md). Five targeted defect fixes
(#27, #28, #29, #25, #30). Each fix = "red test first" (must FAIL at parent
commit), then patch, then green. Offline suite green at EVERY commit; real-XNAT
checks `@real_xnat`-marked (synthetic/localhost only).
Dispatch tier per task: **[H]** haiku, **[S]** sonnet.

## Format `[ID] [P?] [Issue] [tier] desc`

## Stage 0 — Fidelity harness (BLOCKS the red-first tests)
- [X] T001 [#27] [S] `tests/fakes/fake_xnat.py`: reproduce pyxnat's post-`create()`
  empty datatype cache — a freshly `create()`d handle's `attrs._get_datatype()`
  returns `None` until set, so `attrs.mset` blows up exactly like real pyxnat.
  Gate behind a fidelity flag so existing tests are unaffected.
- [ ] T002 [#29] [S] `tests/fakes/fake_xnat.py`: subject enumeration returns
  internal IDs (`*_S#####`) distinct from labels; experiment listing carries an
  `xsiType` column incl. `xnat:rfSessionData`. Seeder helper to stage an RF exp.
- [ ] T003 [#25] [H] `tests/fakes/fake_xnat.py`: a scan resource holds N real files
  (byte round-trip) + reports `# Files`; `list_files`/get enumerates them.

## Stage 1 — #27 push blocker (P1 critical, BLOCKS round-trip)
- [X] T004 [#27] [S] `tests/test_publish_real_contract.py` (RED FIRST): scripted
  `publish_to_xnat` against the fidelity fake → assert today's code raises
  `TypeError: quote_from_bytes() expected bytes`; post-fix asserts exp+subj+scan+
  `SRC`+zip created, no orphaned empty subjects.
- [X] T005 [#27] [S] `src/xnat_experiment_data.py` (~L257): after each `create()`
  (exp/subj/scan) set the handle datatype so `_get_datatype()` returns the xsiType
  (e.g. `*_inst.attrs._datatype = f'xnat:{schema_prefix_str}SessionData'`) or pass
  attrs in the same `create(**{...})`. Comment rationale + pyxnat-version note.
  Make create **idempotent-upsert**: reuse an existing/orphaned subject/experiment,
  fill only missing children, never duplicate.
- [X] T005b [#27] [S] `tests/test_publish_real_contract.py`: add re-publish case —
  pre-seed an orphaned/partial subject, run publish → final state has exactly one
  subject/experiment with all children (SC-001 upsert).
- [ ] T006 [#27] [S] `@real_xnat` integration assert in `tests/integration/`: re-run
  `run_roundtrip_push.py` against local XNAT → push completes (SC-001).

## Stage 2 — #28 ConfigTables bootstrap (P1)
- [ ] T007 [#28] [H] `tests/test_configtables_bootstrap.py` (RED FIRST): fresh
  project (no `database_config.json`) → today propagates `pyxnat...DataError`;
  non-whitelisted user (`admin`) → today raises `PermissionError`.
- [ ] T008 [#28] [H] `src/utilities.py` (~L634): widen first-run `except` to include
  `pyxnat.core.errors.DataError` ("does not exist" → first run → self-initialize),
  alongside `FileNotFoundError`/`KeyError`/`ValueError`. Do NOT re-init over an
  existing-but-malformed file.
- [ ] T009 [#28] [H] `src/utilities.py` (~L704): replace hardcoded
  `['dmattioli','domattioli','stelong']` whitelist with project membership/owner
  lookup (reuse `_verify_login`'s users()/owner path) **OR** a config/env-listed
  allowlist (escape hatch for CI/admin service accounts). No identities in code.
  T007 covers member + non-member; add an allowlisted-non-member case. Green T007.

## Stage 3 — #29 browse labels + type-agnostic enum (P1)
- [ ] T010 [#29] [S] `tests/test_browse_labels.py` (RED FIRST): staged RF exp →
  today `list_downloadable` returns `[]`; Subject column shows internal ID.
- [ ] T011 [#29] [S] browse path (`_subject_names()` + enumeration): resolve subject
  **labels** not internal IDs; query downstream by label.
- [ ] T012 [#29] [S] experiment enumeration → type-agnostic (project experiments +
  `xsiType` column) so RF/CT/US surface. Green T010.

## Stage 4 — #25 full-series / whole-surgery download (P2)
- [ ] T013 [#25] [S] `tests/test_download_full_series.py` (RED FIRST): N-file scan →
  today yields 1 synthesized file; assert post-fix N intact; whole-surgery →
  every scan; count-mismatch → FriendlyError; empty resource → friendly no-op.
- [ ] T014 [#25] [S] `app/logic/download.py` (~L222): replace synthesized
  `{subject}_{exp}_{scan}.dcm` with real resource-file enumeration
  (`CObject`/`Resource.get`) + count-verify vs server `# Files`; empty no-op.
  Keep gateway-portable (Phase 6 re-homes this).
- [ ] T015 [#25] [H] `app/pages/download.py`: one-click whole-surgery selection
  (auto-expand experiment → all scans) + a **content-scope picker** (source-only /
  subset / all / + derived), delivering a **single zip** (default = full source
  set). No streamlit in logic. Green T013.
- [ ] T015b [#25] [S] `app/logic/download.py`: zip assembly from enumerated real
  files honoring the selected content scope; test in `test_download_full_series.py`
  asserts zip contents match each scope option (default = all source images).

## Stage 5 — #30 cleanup (P3)
- [X] T016 [#30] [H] `tests/test_session_metadata_guard.py` (RED FIRST): DICOM with
  no `InstanceNumber` → today `AttributeError` at `xnat_experiment_data.py:506`.
- [X] T017 [#30] [H] `src/xnat_experiment_data.py` (~L506): `hasattr`-guard
  `metadata.InstanceNumber` like its sibling tags; default/derive an index. Green
  T016.
- [ ] T018 [#30] [H] `src/initialize_basic_metatable_items.py`: fix
  `from Utilities import MetaTables` → `from src.utilities import ConfigTables`
  (+ API to current `ConfigTables`), OR remove the file if `ConfigTables`
  self-initializes. Add an import smoke test (or removal note).

## Stage 6 — Verify + close out
- [ ] T019 [S] Full offline suite + `scripts/simulate_e2e.py` green; confirm every
  RED-FIRST test (T004/T007/T010/T013/T016) fails at its parent commit (SC-006).
- [ ] T020 [S] `@real_xnat` end-to-end: `run_roundtrip_push.py` then
  `run_roundtrip_pull.py` against local XNAT → push completes + pull returns the
  full series (SC-001..SC-004). Synthetic/localhost only; tear down after.
- [ ] T021 [H] Update `specs/README.md` Phase-7 row → Planned/Built; annotate
  #25/#27/#28/#29/#30 with fix commits; note Phase 6 will re-home #25/#29 behind
  the gateway.

## Dependencies
- Stage 0 (fidelity fake) BLOCKS the red-first tests in Stages 1–4.
- Within each stage: RED test → patch → green (strict order).
- Stages 1–5 are otherwise **independent** (different files/issues) → can land in
  any order / parallel branches; #27 first by severity (unblocks the round-trip).
- T006/T020 need a local XNAT (PR #23 image / xnat4tests).

## Definition of done (phase)
SC-001..SC-006 met: real local push completes (T005/T006), fresh-project + non-
whitelisted bootstrap works (T008/T009), RF surfaces with labels (T011/T012),
whole-surgery download yields full series count-verified (T014/T015), missing-tag
DICOM + bootstrap script fixed (T017/T018), every fix proven by a test that was
RED at its parent commit (T019), offline suite + e2e sim green.
