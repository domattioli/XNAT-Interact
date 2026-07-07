# Tasks: Close the Remaining Verified-Fix Gap

**Input**: Design documents from `/specs/014-validate-fix-backlog/` (rescoped 2026-07-07 after audit)
**Prerequisites**: plan.md, spec.md, research.md

**Rescope note**: The original 48-task list assumed ~30 unfixed findings. An audit against `development`'s actual tip found 20 already fixed-with-test, 4 fixed-but-untested, 7 still-open, plus real CI duplication. This tasks.md reflects the ACTUAL remaining work only.

## Model Assignment

Main session (Opus/Sonnet, whichever is active) owns: ledger (complete ~29-row record), CI consolidation decision, all fixes touching security/data-integrity (H8), and PR/issue closures — judgment calls where a wrong-but-plausible diff is expensive. Haiku subagents draft: individual regression tests for the smaller findings (M4, L1, S1, S4) and the M7/M8/M10 backfill tests, each reviewed by main before commit. Main always executes the baseline-fail proof run itself (VII honesty bar — reviewer produces the evidence, not the test's author).

## Phase 1: Setup — build the complete ledger

- [ ] T001 Create `specs/014-validate-fix-backlog/ledger.md` from `contracts/ledger.md` schema; pre-populate all ~29 #33 findings (C1–C2, H1–H8, M1–M10, L1–L6, S1–S4) with the disposition this session's audit already established (fixed-with-test / fixed-no-test / still-open / not-a-bug / unclear), including test file names and fix-commit SHAs already identified for the fixed rows
- [ ] T002 [P] Pre-seed the fix-candidates table with #38, #40–#43, #45–#48, #50, each mapped to its actual landing commit (from research D4) or marked still-needed

## Phase 2: User Story 1 — CI consolidation (P1)

**Independent Test**: docs-only PR triggers exactly one testing workflow.

- [ ] T003 [US1] Diff `ci-lite.yml` vs `tests.yml`; keep the one with broader/current coverage as canonical (default: `ci-lite.yml` per CLAUDE.md's existing "canonical minimal lane" language), delete the other; delete `python-package.yml` unconditionally (confirmed dormant)
- [ ] T004 [US1] Record the D1 decision + rationale in `ledger.md`; verify locally that the surviving lane's exact `pytest` invocation runs clean

## Phase 3: User Story 2 — Fix H8 (assemble_zip integrity) (P1, MVP)

**Independent Test**: two new tests fail against current `development`, pass after fix, offline.

- [ ] T005 [US2] Write regression test for H8 empty-resource silent skip in `tests/regression_014/test_h8_zip_integrity.py`; baseline-fail proof at current tip (H8 confirmed still-open at `dea6687`) → ledger
- [ ] T006 [US2] Write regression test for H8 partial-zip-on-error (no cleanup) in the same test file; baseline-fail proof → ledger
- [ ] T007 [US2] Fix `assemble_zip` in `app/logic/download.py` (~520-638): surface/error on empty-resource skip instead of silent `continue`; wrap the zip-write loop so a mid-write error cleans up the partial file (temp + atomic rename, or explicit unlink on exception)
- [ ] T008 [US2] Update ledger: H8 → fixed-with-test

## Phase 4: User Story 3 — Fix M4, L1, S1, S4, resolve L5 (P2)

**Independent Test**: each finding has a named test failing pre-fix, passing post-fix.

- [ ] T009 [P] [US3] Regression test + fix for M4 (NaN slips `IS_VALID == False` gate) in `src/xnat_experiment_data.py` (~1171); test in `tests/regression_014/test_m4_nan_validity.py`; baseline-fail proof → ledger
- [ ] T010 [P] [US3] Regression test + fix for L1 (`ImplementationClassUID` derived from session UID) in `src/xnat_scan_data.py` (~159); test in `tests/regression_014/test_l1_implementation_uid.py`; baseline-fail proof → ledger
- [ ] T011 [P] [US3] Regression test + fix for S1 (`gray_img.shape` 2-tuple unpack fails on 3-channel) in `src/xnat_scan_data.py` (~183); test in `tests/regression_014/test_s1_gray_img_shape.py`; baseline-fail proof → ledger
- [ ] T012 [P] [US3] Regression test + fix for S4 (`/project` substring coincidentally matches `/projects/`) in `src/annotations/io_xnat.py` (~130); test in `tests/regression_014/test_s4_project_prefix.py`; baseline-fail proof → ledger
- [ ] T013 [US3] Investigate L5 (files_written paths into deleted temp dir post-zip) in `app/logic/download.py` (~410-470); either fix + test or record not-a-bug rationale in ledger

## Phase 5: User Story 4 — Backfill tests for already-fixed findings (P2)

**Independent Test**: each new test passes against current code; ledger records the historical pre-fix commit + failing run there.

- [ ] T014 [P] [US4] Locate M7's fixing commit (isolated download subdir in `src/services/xnat_gateway.py`) via `git log -p`; write `tests/regression_014/test_m7_stale_dir_contents.py`; run at parent-of-fixing-commit in a worktree, record failure → ledger; confirm pass at current tip
- [ ] T015 [P] [US4] Locate M8's fixing commit (live per-resource count in `app/logic/download.py`); write `tests/regression_014/test_m8_stale_count_race.py`; baseline-fail proof at parent commit → ledger
- [ ] T016 [P] [US4] Write backfill tests for the two M10 sites (`app/logic/download.py` legacy-path except; `src/utilities.py` first-run catch) in `tests/regression_014/test_m10_friendly_errors.py`; baseline-fail proof at each site's pre-fix commit → ledger

## Phase 6: User Story 5 — Close the stale PR/issue backlog (P3)

**Independent Test**: every PR in #51's list has a terminal state; #51 itself updated/closed.

- [ ] T017 [US5] Close #40, #41, #42, #43, #46, #48, #50 each with a comment citing the actual landing commit (from ledger fix-candidates table); close #45 or #47 per the T003 decision, keep the other as adopted-decision-only
- [ ] T018 [US5] Close #38 with a pointer to its merge commit (`5e3060e`); update or close #51 to reflect current reality
- [ ] T019 [US5] File follow-up issues for S2 (DICOM-SEG per-segment type) and the H5 singleton-construction-lock sub-issue (research D3) — out of this feature's scope but not silently dropped

## Phase 7: Polish

- [ ] T020 Full offline suite run including all new `tests/regression_014/` tests; record evidence in ledger
- [ ] T021 Final ledger completeness audit — all ~29 #33 findings have a terminal or explicitly-tracked-open row (SC-001); PR verification-status block updated (UNVERIFIED-blocked-on-CI until #44 clears and a real green Actions run exists)

## Dependencies

- Phase 1 (ledger scaffold) before everything else.
- Phase 2 (CI) independent of Phases 3–6 but should land early so later pushes get real signal once #44 clears.
- Phase 3 (H8) = MVP slice — highest severity, independent of Phases 4–6.
- Phases 4, 5 independent of each other.
- Phase 6 depends on nothing but is naturally last (bookkeeping on top of a settled ledger).
- Phase 7 depends on all prior phases.

## Implementation Strategy

MVP = Phase 1 + Phase 3 (H8 fixed and proven). Then Phase 2 (CI) to make a real green signal possible, Phases 4–5 to close the remaining small gaps, Phase 6 to leave the repo's PR/issue tracker honest. Total real work: ~7 fixes + 4 backfills + 1 CI decision + 10 closures — a fraction of the original 48-task estimate.
