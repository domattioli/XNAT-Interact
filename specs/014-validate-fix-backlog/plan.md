# Implementation Plan: Validate the Unverified Fix Backlog

**Branch**: `claude/repo-issues-2r7o02` | **Date**: 2026-07-07 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/014-validate-fix-backlog/spec.md`

## Summary

Turn ~10 unmerged draft PRs and two design issues (#33 correctness audit: C1–C2/H1–H8/M1–M10/L/S; #32 dedup + layered identity) into one consolidated, regression-tested branch with a single working CI lane. Every fix is re-implemented fresh (draft PRs = design references only), proven by a ledger-recorded failing-then-passing regression test per Constitution VII, and duplicate fixes (triplicated M1, overlapping M1/M9, #43's C1) land exactly once. CI conflict #45-vs-#47 resolves to the single-lane shape; while the #44 Actions outage persists, everything is reported UNVERIFIED-blocked-on-CI.

## Technical Context

**Language/Version**: Python ≥3.9 (repo floor; dev env 3.11)
**Primary Dependencies**: pydicom, requests, pandas, streamlit (untouched by this feature); pytest for all verification
**Storage**: XNAT server state (faked); `MetaTables.json` shared config "database"; local filesystem for downloads/zips
**Testing**: pytest, offline-by-default via `tests/fakes/fake_xnat.py` + `tests/synthetic_data.py`; markers in `pytest.ini` (`requires_server`, `known_issue`, `contract`, `slow`, `pg`, `pixeldeid`; this feature adds `stress`)
**Target Platform**: Cross-platform CLI (student laptops), ubuntu CI runner
**Project Type**: Single project — standalone-script repo with CLI entrypoint (`main.py`), library code in `src/`
**Performance Goals**: Test lane completes within the 20-minute ci-lite timeout; stress tests excluded from default gate
**Constraints**: Fully offline default suite — no UIowa server, no VPN, no PHI; real-server lane opt-in only via `RUN_XNAT_DUAL=1`; no secrets in code
**Scale/Scope**: ~30 audit findings, ~10 draft PRs to disposition, ≥1000-instance series boundary case, 2-concurrent-writer metadata scenarios

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Gate question | Status |
|---|---|---|
| I — PHI Safety | Touches image data / moves data off-machine? | ✅ PASS — all fixtures synthetic (`tests/synthetic_data.py`); no real PHI anywhere; download fixes (C1/C2/H7/H8) reduce exposure of wrong/escaped files. No new off-machine path. |
| II — Fail Softly | New failure modes with user-visible messaging? | ✅ PASS — H2 (swallowed exceptions), M10/silent excepts, and C2 rejection all specify plain-language message + next step (FR-007, US2-AS2). Fixing bare-except violations is in scope. |
| III — Skill Floor | Terminal/Git required of students? | ✅ PASS — no user-facing workflow change; validation work only. Dedup rejection report (US5) is plain language. |
| IV — Testable Offline | Tests run in CI, no network/PHI? | ✅ PASS — this feature *is* the enforcement of IV: FakeXNAT default, `RUN_XNAT_DUAL=1` + `stress` marker for opt-in real-server lane (FR-005/006). |
| V — Config over Hardcoding | New hardcoded endpoints/creds? | ✅ PASS — C1 removes a wrong hardcoded resource label; no endpoints or credentials introduced. Production hostname banned from test config (Assumptions). |
| VI — Data Integrity at Scale | Shared state / destructive / long-running ops? | ✅ PASS — H3 (TOCTOU lost-update), H1 (UID clobber), M8 (delete name mismatch), no-empty-shells invariant (FR-007) are the core of the feature; each ships with a concurrency/state-diff regression test. |
| VII — Fix Unverified Until Proven | Failing-then-passing test + green CI per fix? Duplicates land once? | ✅ PASS by design — ledger-recorded pre-fix failing run per fix (FR-001), disposition ledger for won't-fix rationale (FR-003), fresh re-implementation with single landing (FR-004), UNVERIFIED-blocked-on-CI reporting while #44 persists (FR-012). |

**Post-Phase-1 re-check**: PASS — design artifacts (ledger schema, fixture-factory contract, marker contract) introduce no new violations.

## Project Structure

### Documentation (this feature)

```text
specs/014-validate-fix-backlog/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output (test-lane, ledger, fixture-factory contracts)
├── ledger.md            # Disposition ledger (living artifact, created in implementation)
└── tasks.md             # Phase 2 output (/speckit-tasks)
```

### Source Code (repository root)

```text
app/logic/download.py         # C1 wrong resource label, C2 zip-slip, H7 missing scans, H8 partial zip, M8, M10
src/
├── xnat_experiment_data.py   # H1 UID collapse, H2 private-tag clobber, M2–M4, no-empty-shells, #32 semantics
├── xnat_scan_data.py         # M1 filename off-by-one, L1
├── utilities.py              # H3 swallowed exceptions, H4 TOCTOU, H5 stale singleton, M5, L2/L3
├── delete_contents_of_server.py  # M9 table-name mismatch
├── annotations/io_xnat.py    # M6 manifest orphans, S4
└── services/xnat_gateway.py  # H6 create_assessor guard, M7, L4 — seams behind which FakeXNAT substitutes

tests/
├── fakes/fake_xnat.py        # extended: state-diff snapshot API for no-empty-shells assertions
├── synthetic_data.py         # extended: seed-set fixture factory (FR-011)
├── regression_014/           # NEW — one test module per finding: test_c1_*.py, test_h3_*.py, ...
├── characterization/         # NEW — #32 old-vs-new dedup envelope (US5)
├── stress/                   # existing dir; gains @pytest.mark.stress concurrency/scale tests
└── contract/                 # existing FakeXNAT-parity tests, untouched

.github/workflows/
├── tests.yml                 # THE single testing lane (consolidated per #45 direction)
├── ci-lite.yml               # unchanged minimal lane
└── python-package.yml        # REMOVED (superseded by tests.yml; decision in ledger)
```

**Structure Decision**: Single-project layout (existing). New test code is additive under `tests/regression_014/` and `tests/characterization/`; production changes are edits-in-place to the four `src/` modules named above. No new packages.

## Complexity Tracking

No constitution violations to justify — table omitted.
