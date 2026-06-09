# Implementation Plan: Phase 6 Build

**Branch**: `claude/codebase-improvement-plan-sGjxz` | **Date**: 2026-06-06 | **Spec**: [spec.md](spec.md)

## Summary

Four-item batch closing Phase 6: gateway ABC + assessor decision + real-XNAT dual-run + conventions module. Reuses 006's strategic spec + plan; this doc is the concrete sequencing for the build wave. Stages ordered so suite stays green at every commit.

## Technical Context

**Language**: Python 3.11. **Deps**: pyxnat (prod), pydicom/numpy unchanged. **New dev dep**: docker + docker-compose (xnat_local fixture; opt-in via `RUN_XNAT_DUAL=1`).
**Testing**: pytest, offline default; dual-run gated. **Reuses**: 006 plan structure, `tests/fakes/fake_xnat.py` (becomes FakeGateway), `tests/contract/test_workflow_contract.py` (parity seams flip live), Phase 7 #27 datatype-cache pattern.

## Approach

Six stages; each commit keeps `pytest` green. Dispatch per DomI policy: code → subagent (haiku default, sonnet for harder; non-coding stays main).

### Stage 1 — Gateway ABC (sonnet)

1. `src/services/xnat_gateway.py`: define `XnatGateway` ABC (16 methods + `list_files`/`download_resource`/`create_assessor`).
2. `PyxnatGateway(XnatGateway)`: move raw calls verbatim; `build_gateway(url,user,pwd)` returns it; `build_server` shim.
3. `tests/fakes/fake_xnat.py`: declare ABC conformance; `FakeGateway` alias.
4. `tests/test_xnat_gateway.py`: method-set parity Pyxnat vs Fake; recorded-call equivalence on scripted publish.

**Suite must be green after Stage 1.** No call sites routed yet.

### Stage 2 — Conventions module (haiku)

5. `src/services/xnat_conventions.py`: `project_qs`/`subject_qs`/`experiment_qs`/`scan_qs` (`scan='0'` constant), label builders (`SOURCE_DATA-{uid}`, `SEGMENTATION_CONSENSUS-{uid}`), `ResourceLabel` constants, filename builders.
6. `tests/test_xnat_conventions.py`: lock current values byte-for-byte (guards SC-003 + SC-006).

### Stage 3 — Route call sites, one commit per file (haiku, sonnet for utilities)

7–11. Route `xnat_experiment_data.py`, `xnat_resource_data.py`, `annotations/io_xnat.py`, `utilities.py` (sonnet — lost-update guard), `delete_contents_of_server.py` through gateway + conventions.
12. `tests/test_no_raw_pyxnat.py`: grep-guard (SC-001) — fail if raw pyxnat appears in `src/` outside `xnat_gateway*.py`.

### Stage 4 — Assessor decision + wiring (sonnet)

13. `PyxnatGateway.create_assessor(experiment_qs, assessor_label, xsi_type='xnat:assessorData', files)`: implements assessor write per Phase 7 #27 datatype-cache pattern (create with xsiType → empty cache → attrs.mset). FakeGateway mirror records assessor-level write distinct from scan-resource.
14. `src/xnat_experiment_data.py`: `publish_to_xnat(..., assessor: Path = None, assessor_label: str = None)` (C006). When `assessor` set, routes through `gateway.create_assessor` using `assessor_label` (default `conventions.consensus_label(uid)`).
15. `tests/contract/test_workflow_contract.py` T003: flip `# DOCUMENTS GAP` comments to live assertions; assert assessor xsiType + label + file round-trip.
16. Update `specs/006-xnat-alignment/contract-test.md` audit matrix row 7 from "Assessor API deferred" to "Aligned (assessor, T003 verified)".

### Stage 5 — Real-XNAT dual-run (sonnet)

17. Verify `tests/integration/xnat_local/` exists; if not, scaffold `docker-compose.yml` pulling `xnat/xnat-web` at subagent-selected pinned SHA (latest stable digest). Document SHA in `tests/contract/README.md`.
18. `tests/contract/conftest.py` `real_xnat` fixture: under `RUN_XNAT_DUAL=1` → boot xnat_local via subprocess docker-compose; health-poll `:8080/xapi/siteConfig`; seed admin/admin + per-test project namespace (`ITEST_{uuid4hex[:8]}`); yield gateway connection; teardown cleans the project (not the container — session-scoped reuse).
19a. `tests/contract/comparator.py`: `XnatStateComparator` class. Normalizes fake + real state by stripping server-assigned fields (`ID`, `insert_date`, `xnat_*Data/id`, `URI`, timestamp fields). `compare(fake, real) -> ComparisonResult` with diff report on mismatch. Unit-tested in `tests/contract/test_comparator.py`.
19b. `tests/contract/test_workflow_contract.py`: flip all 9 tests' `# DUAL-RUN PARITY (deferred):` seams to live: `XnatStateComparator().compare(fake_state, real_state).assert_equal()`. Poll-with-timeout for XNAT eventual consistency.
20. No CI lane this batch (C009). Local-only `RUN_XNAT_DUAL=1` documented in `tests/contract/README.md`.

### Stage 6 — Verify + close

21. Full suite + `scripts/simulate_e2e.py` green; byte-diff representative writes pre/post (SC-006).
21b. Delete `build_server()` shim (C008). Verify zero callers via `grep -rn 'build_server' src/ tests/`.
22. `RUN_XNAT_DUAL=1 pytest tests/contract/` green.
23. Update `specs/README.md` Phase-6 row → Built. Close `006-xnat-alignment/tasks.md` T006/T020 (Phase 7 deferred items). Annotate #25.

## Coding dispatch

Per DomI policy:
- **Stages 1, 3 (utilities), 4, 5** = harder → **sonnet** subagent.
- **Stages 2, 3 (mechanical), 6 (closeout edits)** = **haiku** subagent.
- Orchestrator (this session): plan, review each commit, integrate, hold suite green.

## Risks (delta from 006)

- *Assessor xsiType edge cases*: pyxnat may surface different create() behavior for `xnat:assessorData` vs `xnat:rfSessionData`. Mitigation: implement fidelity_mode pattern (Phase 7 #27) in FakeGateway first, then verify Pyxnat matches via T003 dual-run.
- *xnat_local Docker image flakes*: pin SHA; if image gone, scaffold from upstream xnat/xnat-web. Mitigation: dual-run is opt-in, doesn't block CI.
- *Dual-run reveals fidelity gaps*: this is the **point**. Each gap → audit matrix row + FakeGateway patch + re-run; don't paper over.

## Out of scope (this batch)

- xnatpy spike (006 US5 / T018–T019) — operator gate after batch lands.
- Migrating existing ANNOTATIONS resources into assessors (data migration).
- Multi-scan-per-experiment.
- STAPLE algorithm.
