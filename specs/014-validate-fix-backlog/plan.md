# Implementation Plan: Close the Remaining Verified-Fix Gap

**Branch**: `claude/repo-issues-2r7o02` | **Date**: 2026-07-07 (rescoped) | **Spec**: [spec.md](spec.md)

## Summary

A pre-implementation audit against `development`'s actual tip (`dea6687`) found the original ~30-finding backlog is **already 83% resolved**: 20 findings fixed-with-test, 4 fixed-but-untested, 7 genuinely still open, plus a real CI-lane duplication (`ci-lite.yml` + `tests.yml` both fire on PR→main; `python-package.yml` dormant). This plan targets only the real remaining gap: consolidate CI (1 lane), fix H8/M4/L1/S1/S4/L5 with regression tests, backfill tests for M7/M8/M10 against their historical pre-fix commits, close the 10 stale draft PRs with pointers, and produce a complete disposition ledger covering all ~29 #33 findings (not just the 8 active ones) so this gap doesn't get rediscovered blind next time.

## Technical Context

**Language/Version**: Python ≥3.9 (repo floor; dev env 3.11)
**Primary Dependencies**: pydicom, requests, pandas; pytest for verification
**Storage**: XNAT server state (faked); `database_config.json`; local filesystem for downloads/zips
**Testing**: pytest, offline via `tests/fakes/fake_xnat.py` + `tests/synthetic_data.py`; existing markers in `pytest.ini`
**Target Platform**: Cross-platform CLI, ubuntu CI runner
**Project Type**: Single project (existing `src/`, `app/logic/`, `tests/`)
**Performance Goals**: Consolidated CI lane completes within its timeout
**Constraints**: Fully offline default suite; historical pre-fix commits are read via throwaway git worktrees, never by editing history
**Scale/Scope**: 7 open findings + 4 backfill-only findings + 1 CI consolidation + 10 PR closures + 1 complete ledger (~29 rows)

## Constitution Check

| Principle | Gate question | Status |
|---|---|---|
| I — PHI Safety | Touches image data / off-machine? | ✅ PASS — all fixture data synthetic; no new off-machine path. |
| II — Fail Softly | New failure modes messaged? | ✅ PASS — H8, M10 backfill both specify FriendlyError/plain-language surfacing. |
| III — Skill Floor | Terminal/Git required of students? | ✅ PASS — no user-facing workflow change. |
| IV — Testable Offline | CI, no network/PHI? | ✅ PASS — FakeXNAT/synthetic default; historical-commit proof runs via local worktree, still offline. |
| V — Config over Hardcoding | New hardcoded endpoints/creds? | ✅ PASS — none introduced. |
| VI — Data Integrity at Scale | Shared state / destructive ops? | ✅ PASS — H8 fix is exactly a data-integrity gap (incomplete zip delivered silently). |
| VII — Fix Unverified Until Proven | Failing→passing test + green CI? Duplicates land once? | ✅ PASS by design — this whole feature exists to close exactly this gap; ledger (FR-006) is the single source of truth; FR-008 governs honest CI-outage reporting. |

**Post-Phase-1 re-check**: PASS — no new violations from design artifacts.

## Project Structure

```text
specs/014-validate-fix-backlog/
├── plan.md, research.md, data-model.md, quickstart.md, contracts/
├── ledger.md              # Complete record: all ~29 #33 findings, real current status
└── tasks.md

app/logic/download.py       # H8 fix, M7/M8/M10 backfill tests
src/xnat_scan_data.py       # L1 fix, S1 fix
src/xnat_experiment_data.py # M4 fix
src/annotations/io_xnat.py  # S4 fix
src/utilities.py            # M10 backfill test (first-run catch site)

.github/workflows/
├── ci-lite.yml or tests.yml   # ONE survives as canonical (decision recorded in ledger)
└── python-package.yml         # DELETED

tests/
├── regression_014/            # new tests: H8, M4, L1, S1, S4, L5(if needed), M7/M8/M10 backfills
└── synthetic_data.py           # extended only if a new fixture shape is needed (unlikely — factory already rich)
```

**Structure Decision**: Same single-project layout as before; scope of change is now small — edits to 5 existing files, one workflow deletion, new tests under `tests/regression_014/`.

## Complexity Tracking

No constitution violations — table omitted.
