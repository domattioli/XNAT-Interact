# Tasks: Phase 6 Build

**Input**: [spec.md](spec.md), [plan.md](plan.md). Suite green at EVERY commit.
**Tier**: **[H]** haiku, **[S]** sonnet. Orchestrator (main session) plans/reviews/integrates.

## Stage 0 — Clarifications RESOLVED (2026-06-06)

- [X] C001 Assessor for derived data.
- [X] C002 Docker xnat_local session-scoped + per-test project namespace.
- [X] C003 Full 6-file routing this batch.
- [X] C004 Conventions = new names + replacements.
- [X] C005 xnat_local image: subagent verifies dir; scaffolds `xnat/xnat-web` pinned SHA if missing.
- [X] C006 publish_to_xnat: `assessor: Path, assessor_label: str` kwargs.
- [X] C007 Dual-run parity: custom `XnatStateComparator` normalizes IDs/timestamps/URIs.
- [X] C008 build_server shim: deleted in Stage 6.
- [X] C009 No CI lane this batch; local-only `RUN_XNAT_DUAL=1`.

## Stage 1 — Gateway ABC (BLOCKS all)

- [ ] T001 [S] `src/services/xnat_gateway.py`: `XnatGateway` ABC. Methods: `connect/disconnect/liveness/project_label/project_users/select/exists/create/set_attrs/put_zip/put_file/insert_file/get_file_copy/delete_file/list_files/download_resource/create_assessor`. Docstring each = `docs/XNAT_MODEL.md §3` origin.
- [ ] T002 [S] `PyxnatGateway(XnatGateway)`: move raw pyxnat calls **verbatim**. `build_gateway(url,user,pwd)` returns it. `build_server()` → shim.
- [ ] T003 [H] `tests/fakes/fake_xnat.py`: declare `XnatGateway` conformance; export `FakeGateway` alias; add `create_assessor` recorder (fidelity_mode for assessor xsiType per Phase 7 #27 pattern).
- [ ] T004 [S] `tests/test_xnat_gateway.py`: ABC method-set parity (Pyxnat vs Fake); recorded-call equivalence on scripted publish; assessor write recorded distinct from scan-resource.
- [ ] GATE Suite green (751+ tests). No call sites routed yet.

## Stage 2 — Conventions module (BLOCKS Stage 3)

- [ ] T005 [H] `src/services/xnat_conventions.py`: `project_qs/subject_qs/experiment_qs/scan_qs` (`SCAN_DEFAULT='0'` const), label builders (`source_data_label(uid)`, `consensus_label(uid)`), `ResourceLabel` constants (`SRC/INTAKE_FORM/ANNOTATIONS/CONFIG/BACKUPS/SEGMENTATION_CONSENSUS`), filename builders.
- [ ] T006 [H] `tests/test_xnat_conventions.py`: lock current string values byte-for-byte.

## Stage 3 — Route call sites (one commit each, suite green each)

- [ ] T007 [H] `src/xnat_experiment_data.py`: `_generate_queries/_select_objects/publish_to_xnat` → gateway + conventions.
- [ ] T008 [H] `src/xnat_resource_data.py`: `ORDataIntakeForm.push_to_xnat` → gateway + conventions.
- [ ] T009 [H] `src/annotations/io_xnat.py`: upload/download_annotation_set → gateway + conventions.
- [ ] T010 [S] `src/utilities.py`: `XNATConnection` constructs gateway (not raw `Interface`); `ConfigTables.pull/push` + **lost-update re-fetch** via gateway reads. Public surface + singleton unchanged.
- [ ] T011 [H] `src/delete_contents_of_server.py`: enumerate/delete + config nuke → gateway.
- [ ] T012 [H] `tests/test_no_raw_pyxnat.py`: grep-guard. FAIL if `import pyxnat`/`Interface(`/`.put_zip(`/`.get_copy(` in `src/` outside `xnat_gateway*.py`. Also fails on inline `'/project[s]?/'` outside `xnat_conventions.py` (SC-003).
- [ ] GATE Suite green; SC-001 + SC-003 met.

## Stage 4 — Assessor decision + wiring

- [ ] T013 [S] `PyxnatGateway.create_assessor(experiment_qs, assessor_label, *, xsi_type='xnat:assessorData', files)`: implements per Phase 7 #27 datatype-cache pattern (create with xsiType → empty cache → attrs.mset). FakeGateway mirror.
- [ ] T014 [S] `src/xnat_experiment_data.py`: `publish_to_xnat(..., assessor: Path = None, assessor_label: str = None)` kwargs. When `assessor` set, routes through `gateway.create_assessor` with `assessor_label` (defaults to `conventions.consensus_label(uid)` if None).
- [ ] T015 [S] `tests/contract/test_workflow_contract.py` T003: flip `# DOCUMENTS GAP` → live assertions. Assert assessor xsiType + label + file round-trip via FakeGateway. Remove "resource-level API directly" workaround.
- [ ] T016 [H] Update `specs/006-xnat-alignment/contract-test.md` audit matrix row 7: "Assessor API deferred" → "Aligned (assessor, T003 verified)".

## Stage 5 — Real-XNAT dual-run

- [ ] T017 [S] Verify `tests/integration/xnat_local/` exists. If not, scaffold `docker-compose.yml` pulling `xnat/xnat-web` at subagent-selected pinned SHA (latest stable digest) + admin/admin throwaway creds. Document image SHA + boot in `tests/contract/README.md`.
- [ ] T018 [S] `tests/contract/conftest.py` `real_xnat` fixture (session-scoped): under `RUN_XNAT_DUAL=1` → `docker compose up -d` xnat_local; poll `:8080/xapi/siteConfig` until 200 (timeout 60s); seed admin user + per-test project namespace `ITEST_{uuid4hex[:8]}`; yield gateway connection; teardown deletes project only (container reused).
- [ ] T019a [S] `tests/contract/comparator.py`: `XnatStateComparator` class. Normalizes both fake + real state by stripping server-assigned fields (`ID`, `insert_date`, `xnat_*Data/id`, `URI`, any timestamp fields). Exposes `compare(fake_state, real_state) -> ComparisonResult` with diff report on mismatch. Unit tests: `tests/contract/test_comparator.py`.
- [ ] T019b [S] `tests/contract/test_workflow_contract.py`: flip all 9 tests' `# DUAL-RUN PARITY (deferred):` seams to live. Pattern per test: capture fake state → capture real state → `XnatStateComparator().compare(fake, real).assert_equal()`. Poll-with-timeout for XNAT eventual consistency reads.
- [ ] T020 [H] CI: leave `RUN_XNAT_DUAL` unset (skip-default). Add `tests/contract/README.md` with local invocation: `RUN_XNAT_DUAL=1 pytest tests/contract/`.
- [ ] GATE `RUN_XNAT_DUAL=1 pytest tests/contract/` green (9 dual-runs). SC-004 met.

## Stage 6 — Verify + close

- [ ] T021 [S] Full suite + `scripts/simulate_e2e.py` green. Byte-diff representative writes (publish + intake push + annotation push) pre/post → zero drift. SC-006.
- [ ] T021b [H] Delete `build_server()` shim from `src/services/xnat_gateway.py`. Verify zero remaining callers (`grep -rn 'build_server' src/ tests/`).
- [ ] T022 [H] Update `specs/README.md` Phase-6 row → Built. Close `006-xnat-alignment/tasks.md` T006/T020 (Phase 7 deferred). Annotate #25.
- [ ] T023 [H] Session introspection → `docs/introspections/phase6-build-{commit}.md`. Pain corpus YAML per DomI introspect schema. Route via PR/corpus, not new tracking issue.

## Dependencies

- Stage 0 BLOCKS all.
- Stage 1 BLOCKS 2–5.
- Stage 2 BLOCKS Stage 3.
- Stage 3 T012 grep-guard after T007–T011.
- Stage 4 BLOCKS Stage 5 (T015 dual-run needs T013/T014 first).
- Stage 5 BLOCKS Stage 6 verify.

## Definition of done

SC-001..SC-006 met:
- zero raw-pyxnat in `src/` (T012)
- suite + e2e sim green unchanged (T021)
- all path strings from conventions (T006 + T012)
- T003 assesses assessor parity, not docs gap (T015)
- dual-run discharges T006/T020 (T019)
- no byte-drift in existing writes (T021)

## Out of scope (this batch)

- xnatpy spike (006 US5 / T018–T019 from 006). Operator gate after this batch.
- Migrating existing ANNOTATIONS resources into assessors.
- Multi-scan-per-experiment.
- STAPLE algorithm.
