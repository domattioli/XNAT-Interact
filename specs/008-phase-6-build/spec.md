# Feature Specification: Phase 6 Build — Gateway + Assessor Decision + Dual-Run + Conventions

**Feature Branch**: `claude/codebase-improvement-plan-sGjxz`
**Created**: 2026-06-06
**Status**: Draft
**Supersedes**: tasks in [`../006-xnat-alignment/tasks.md`](../006-xnat-alignment/tasks.md) (collapses into a single buildable wave). 006 spec.md remains the strategic statement.
**Input**: Phase 7 handoff + Phase 6 contract-test framework (`../006-xnat-alignment/contract-test.md`). Four pending items batched into one phase build.

## Overview

Phase 6 strategic spec (`006-xnat-alignment/spec.md`) framed the gateway ABC + conventions + assessor seam + xnatpy spike. Phase 7 closeout deferred real-XNAT verification (T006/T020) into a contract-test framework already landed at commit `93235a7` (9 tests, FakeXNAT-side only, `real_xnat` fixture skip-marked).

This spec ships the **batch build** that closes Phase 6 and discharges the deferred verification:

1. **Gateway ABC** — `src/services/xnat_gateway.py` with `XnatGateway` (ABC), `PyxnatGateway` (prod), and `FakeGateway` (alias of the existing FakeXNAT) routing all 16 pyxnat call sites.
2. **Assessor-vs-Resource Decision** — resolve the open T003 gap (publish_to_xnat has no derived-upload path). Pick one storage shape for derived data; document the choice; wire it into the gateway; update T003 contract test from documenting-the-gap to asserting the chosen shape.
3. **Real-XNAT Dual-Run** — replace the skip-marked `real_xnat` fixture in `tests/contract/conftest.py` with a Docker `tests/integration/xnat_local/` boot+seed fixture; flip the commented parity-assert seams in `tests/contract/test_workflow_contract.py` live. Discharges T006/T020 from Phase 7.
4. **Conventions Module** — `src/services/xnat_conventions.py` centralizes `scan='0'`, `SOURCE_DATA-{uid}`, resource-label f-strings; all call sites import it.

xnatpy spike (User Story 5 in 006) stays out of this batch — gated on operator go/no-go after items 1–4 land.

Constraints unchanged: offline-testable (Docker dual-run opt-in via `RUN_XNAT_DUAL=1`), PHI-free synthetic data only, no creds in argv/logs, byte-identical writes for existing XNAT layouts.

## Clarifications — RESOLVED 2026-06-06

C001–C004 (Stage 0) all confirmed at recommended defaults.

C005–C009 (second-pass) resolved:
- **C005 xnat_local image**: subagent verifies `tests/integration/xnat_local/` exists; if absent, scaffolds `xnat/xnat-web` at a pinned SHA the subagent selects (latest stable tag, digest-pinned in docker-compose).
- **C006 publish_to_xnat assessor signature**: `assessor: Path, assessor_label: str` kwargs. Single file per call. Extendable later if multi-file emerges.
- **C007 dual-run parity**: custom `XnatStateComparator` class normalizes both sides (strips server IDs, timestamps, URIs) before equality compare. Most foolproof; documented in `tests/contract/comparator.py`.
- **C008 build_server shim**: deleted in Stage 6 once all callers routed.
- **C009 CI lane for dual-run**: none this batch. Local-only via `RUN_XNAT_DUAL=1`. CI follow-up after fixture stabilizes.

### Original clarifications (Stage 0)

1. **Assessor-vs-Resource for derived data** (T003 gap).
   - **Default**: resource-under-experiment, label `SEGMENTATION_CONSENSUS-{uid}` (matches today's loose-resource pattern; no schema change).
   - **Alt**: XNAT assessor (`xnat:assessorData`); proper searchable derived-data slot; requires assessor xsiType wiring.
   - **Recommendation**: assessor. The contract-test gap was specifically that publish_to_xnat has no assessor path; resolving it as resource papers over Phase 6's reason-to-exist. Phase 5 (STAPLE) needs the assessor seam anyway.
   - **Trade-off**: assessor = +1 day of pyxnat plumbing (assessor xsiType registration, datatype cache handling per Phase 7 #27 fix pattern) vs. resource = ship faster but defers the seam.

2. **Real-XNAT Docker fixture scope**.
   - **Default**: reuse the existing `tests/integration/xnat_local/` image (referenced in 006 plan). Boot via docker-compose, health-poll `:8080/xapi/siteConfig`, seed admin user + test project, yield connection. ~30s startup per test session.
   - **Alt**: per-test ephemeral containers (slow, isolated) or shared session-scoped container (fast, requires reset between tests).
   - **Recommendation**: session-scoped + per-test project namespace (`ITEST_{run_id}`) for isolation without restart cost.
   - **Gate**: `RUN_XNAT_DUAL=1` env var; CI skip-default; local + operator-invoked only.

3. **Gateway routing scope this batch**.
   - **Default**: all 6 call-site files (per 006 plan Stage 3) routed in one wave, suite green at each commit.
   - **Alt**: route publish + download paths only this batch; defer ConfigTables + delete-server to a follow-up.
   - **Recommendation**: full routing. Grep-guard test (006 T012) is the regression net; partial routing leaves the guard unenforceable.

4. **Conventions module — drop-in or new names**.
   - **Default**: introduce new function names (`subject_qs(uid)`, `experiment_label(uid)`, etc.) and replace inline f-strings in call sites.
   - **Alt**: keep current inline strings + add the module as documentation only.
   - **Recommendation**: new names + replacements. The module earns its keep only if it's the single source.

## User Scenarios

### US1 — Gateway ABC enforced (P1)

All XNAT access via `XnatGateway`. Grep-guard test fails if `import pyxnat` / `Interface(` / `.put_zip(` appears in `src/` outside `xnat_gateway*.py`. FakeGateway is the ABC-conforming test double; existing 751-test suite passes unchanged.

**Acceptance**: SC-001 (zero raw-pyxnat refs), SC-002 (suite green), SC-006 (byte-identical writes).

### US2 — Assessor seam decided + wired (P1)

`gateway.create_assessor(experiment_qs, assessor_label, xsi_type, files)` exists with both PyxnatGateway + FakeGateway impls. `publish_to_xnat` exposes a derived-upload path that routes through it. T003 contract test asserts the chosen shape (assessor, per recommended default) instead of documenting the gap.

**Acceptance**: T003 flips from `# DOCUMENTS GAP` to live parity assertion; FakeGateway records assessor-level write distinct from scan-resource write.

### US3 — Real-XNAT dual-run live (P1)

`tests/contract/conftest.py` `real_xnat` fixture boots Docker xnat_local, yields a connection. Under `RUN_XNAT_DUAL=1`, all 9 contract tests run dual (fake + real) and parity-assert on server state, file bytes, attrs. Discharges Phase 7 T006/T020.

**Acceptance**: `RUN_XNAT_DUAL=1 pytest tests/contract/` → 9 dual-runs pass; without the env var, real_xnat skips cleanly (today's behavior).

### US4 — Conventions module is single source (P1)

`src/services/xnat_conventions.py` owns every query string + label + resource name. No inline `'/project/' + ...` f-strings in object classes. Changing the single-scan assumption (future multi-series) is a one-file edit.

**Acceptance**: SC-003 (all path strings from conventions); grep test catches inline `'/project/'` leaks in `src/`.

## Functional Requirements

- **FR-001**: `XnatGateway` ABC declares the 16-call surface + `list_files`, `download_resource`, `create_assessor`.
- **FR-002**: `PyxnatGateway` impl is byte-identical to current writes (per 006 FR-002).
- **FR-003**: `FakeGateway` = today's FakeXNAT with assessor + ABC conformance added.
- **FR-004**: All 6 call-site files route through gateway. No raw pyxnat in `src/` outside `xnat_gateway*.py`.
- **FR-005**: `xnat_conventions.py` owns all query strings + labels + resource names; call sites import from it.
- **FR-006**: `create_assessor` resolves derived-data storage as assessor (per recommended clarification 1); `publish_to_xnat` exposes a derived-upload path that calls it.
- **FR-007**: T003 contract test asserts assessor-shape parity (fake + real); previous `# DOCUMENTS GAP` comments removed.
- **FR-008**: `real_xnat` fixture boots Docker xnat_local under `RUN_XNAT_DUAL=1`; session-scoped, per-test project namespace, health-polled, teardown idempotent.
- **FR-009**: All 9 contract tests' `# DUAL-RUN PARITY (deferred):` seams flip live (assert fake state == real state on attrs, file bytes, resource enumeration).
- **FR-010**: Suite green: 751 (today) + new gateway/conventions/assessor tests; dual-run gated suite passes under `RUN_XNAT_DUAL=1`.
- **FR-011**: No credentials in argv/logs; Docker admin/admin throwaway only (xnat_local convention).

## Success Criteria

- **SC-001**: `grep -rE 'import pyxnat|Interface\(|\.put_zip\(' src/` returns only `xnat_gateway*.py`.
- **SC-002**: `pytest` (default) passes 751+ tests; zero regressions.
- **SC-003**: `grep -rE "'/project[s]?/'" src/` returns only `xnat_conventions.py`.
- **SC-004**: `RUN_XNAT_DUAL=1 pytest tests/contract/` passes all 9 dual-runs.
- **SC-005**: T003 contract test asserts assessor-shape parity; fidelity audit matrix row updated from "Assessor API deferred" to "Aligned (assessor, T003 verified)".
- **SC-006**: Byte-diff representative writes (publish + push intake + push annotation) pre/post: zero drift.

## Out of Scope (this batch)

- xnatpy spike + `XnatpyGateway` (006 US5) — operator-gated follow-up after items 1–4 land.
- Migrating existing `ANNOTATIONS` resources into assessors (data migration; not a refactor).
- Multi-scan-per-experiment (conventions module makes it a one-file change later).
- STAPLE consensus algorithm (Phase 5 scope; assessor seam exists for when it's built).

## Risks & Mitigations

- *Big refactor breaks a path* → one-file-per-commit; 751-test suite + grep-guard as nets (006 plan).
- *Assessor xsiType plumbing surprises* (Phase 7 #27 datatype-cache pattern) → reuse the documented fidelity_mode + post-create attrs.mset sequencing; FakeGateway fidelity_mode covers it.
- *Docker xnat_local image drift* → pin image SHA in compose file; health-poll on `:8080/xapi/siteConfig` not arbitrary sleep.
- *Dual-run flakes from XNAT eventual consistency* → poll-with-timeout on read-after-write; surface flake as test failure, not retry.
- *Scope creep into xnatpy* → explicitly out of scope; spike is post-batch operator call.

## Assumptions

- Phase 7 #27 datatype-cache fix pattern transfers to assessor xsiType handling (same `create(xsiType=...)` mechanism).
- `tests/integration/xnat_local/` image exists and is the right xnat_local target (verify in plan).
- 751-test suite is the regression net; FakeGateway's existing fidelity covers gateway routing correctness.
