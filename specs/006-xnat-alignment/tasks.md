# Tasks: XNAT Alignment (Phase 6)

**Input**: [spec.md](spec.md), [plan.md](plan.md). Behavior-preserving refactor +
#25 download fix. Tests REQUIRED, offline. Suite green at EVERY commit.
Dispatch tier noted per task: **[H]** haiku, **[S]** sonnet.

## Format `[ID] [P?] [Story] [tier] desc`

## Stage 1 — Gateway contract (BLOCKS all)
- [ ] T001 [US1] [S] `src/services/xnat_gateway.py`: define `XnatGateway` (ABC) —
  methods: `connect/disconnect`, `liveness`, `project_label`, `project_users`,
  `select(qs)`, `exists(qs)`, `create(qs, datatype)`, `set_attrs(qs, mapping)`,
  `put_zip(qs, ffn, *, content, format, tags)`, `put_file/insert_file/get_file_copy/
  delete_file`, `list_files(qs)`, `download_resource(qs, dest)`,
  `create_assessor(...)`. Docstring each = its `docs/XNAT_MODEL.md §3` origin.
- [ ] T002 [US1] [S] `PyxnatGateway(XnatGateway)`: move today's raw pyxnat calls in
  **verbatim** (no reformat). `build_gateway(url,user,password)` returns it;
  `build_server()` → thin shim returning `.server` for any not-yet-migrated caller.
- [ ] T003 [US1] [H] `tests/fakes/fake_xnat.py`: declare it implements `XnatGateway`;
  add `list_files`, `download_resource`, `create_assessor` (recorder + byte
  round-trip + failure inject, matching existing style). Export `FakeGateway` alias.
- [ ] T004 [US1] [S] `tests/test_xnat_gateway.py`: ABC method-set parity
  (Pyxnat vs Fake), recorded-call equivalence on a scripted publish sequence,
  `create_assessor` fake test.

## Stage 2 — Conventions (BLOCKS Stage 3)
- [ ] T005 [US2] [H] `src/services/xnat_conventions.py`: `project_qs/subject_qs/
  experiment_qs/scan_qs` (the `scan='0'` constant lives here), label builders
  (`SOURCE_DATA-{uid}`), `ResourceLabel` constants
  (`SRC/INTAKE_FORM/ANNOTATIONS/CONFIG/BACKUPS`), filename builders (annotation blob
  `ann__{annotator}__{type}__v{n}.{ext}`, manifest, intake JSON, config JSON).
- [X] T006 [US2] [H] `tests/test_xnat_conventions.py`: each builder returns the exact
  string used today (lock current values — guards SC-003 + SC-006). **DONE: discharged via Phase 6 build batch 008 — gateway ABC + dual-run live (commit a65a69b). See specs/008-phase-6-build/.**

## Stage 3 — Route call sites (one commit each, suite green each)
- [ ] T007 [US1] [H] `src/xnat_experiment_data.py`: `_generate_queries`/
  `_select_objects`/`publish_to_xnat` → gateway + conventions.
- [ ] T008 [US1] [H] `src/xnat_resource_data.py`: `ORDataIntakeForm.push_to_xnat`
  → gateway + conventions.
- [ ] T009 [US1] [H] `src/annotations/io_xnat.py`: upload/download_annotation_set
  → gateway + conventions filenames.
- [ ] T010 [US1] [S] `src/utilities.py`: `XNATConnection` constructs the gateway
  (not raw `Interface`); `ConfigTables.pull/push` + **lost-update re-fetch** via
  gateway reads. Keep public surface + singleton behavior identical.
- [ ] T011 [US1] [H] `src/delete_contents_of_server.py`: subject enumerate/delete +
  config nuke → gateway.
- [ ] T012 [US1] [H] `tests/test_no_raw_pyxnat.py`: grep-guard — FAIL if
  `import pyxnat` / `Interface(` / `.put_zip(` / `.get_copy(` appears in `src/`
  outside `xnat_gateway*.py` (SC-001).

## Stage 4 — Download fix (#25)
- [ ] T013 [US3] [S] PyxnatGateway `list_files`/`download_resource` via `CObject`
  enumeration / `Resource.get(dest_dir)`; FakeGateway mirror (seed N files).
- [ ] T014 [US3] [S] `app/logic/download.py`: replace synthesized
  `{subject}_{exp}_{scan}.dcm` (line ~222) with real enumeration + **count-verify**;
  add whole-experiment (surgery) iteration over scans/resources. Fail-soft.
- [ ] T015 [US3] [H] `app/pages/download.py`: one-click whole-surgery select
  (+ optional zip of the set). No streamlit in logic.
- [ ] T016 [US3] [H] `tests/test_download_alignment.py` (offline, FakeGateway):
  scan w/ N files → N downloaded; whole-surgery → every scan enumerated;
  count-mismatch → FriendlyError; empty resource → friendly no-op.

## Stage 5 — Assessor seam + xnatpy spike
- [ ] T017 [US4] [S] PyxnatGateway `create_assessor` against
  `.../experiments/{e}/assessors/{a}` (+ resource/file under it); FakeGateway
  records an assessor-level write distinct from scan-resource. Fake test only.
- [ ] T018 [US5] [S] `src/services/xnat_gateway_xnatpy.py` (spike, NOT
  default-imported): `XnatpyGateway(XnatGateway)` via `xnat.connect`. Add `xnat`
  as a dev/optional dep. Type-conforms to the ABC.
- [ ] T019 [US5] [S] `docs/XNAT_XNATPY_SPIKE.md`: A/B note (label-addressing vs
  tree-walking, cache/concurrency, LOC delta on a sample feature) + go/no-go.

## Stage 6 — Verify + close out
- [X] T020 [S] Full suite + `scripts/simulate_e2e.py` green; byte-diff representative
  writes pre/post (SC-006); `flake8`/`bash -n` clean. **DONE: discharged via Phase 6 build batch 008 — gateway ABC + dual-run live (commit a65a69b). 820 passed, e2e green, zero byte-drift on publish+intake+annotation paths. See specs/008-phase-6-build/.**
- [ ] T021 [H] Update `specs/README.md` Phase-6 row → Built; annotate/close #25
  with the alignment commit; note assessor seam ready for STAPLE wiring.

## Dependencies
- Stage 1 BLOCKS 2–5. Stage 2 BLOCKS Stage 3. T012 (grep-guard) after T007–T011.
- Stage 4 needs T013 before T014/T015. Stage 5 independent of Stage 4 (parallel-ok).
- `[P]`-free: most tasks touch shared files → serialize per stage; suite green is the gate.

## Definition of done (phase)
SC-001..SC-006 met: zero raw-pyxnat in `src/` (T012), suite+sim green unchanged (T020),
all paths from conventions (T006), download yields N files + whole-surgery (#25, T016),
assessor seam tested + xnatpy go/no-go documented (T017/T019), no on-server layout drift (T020).
