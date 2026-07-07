# Tasks: Validate the Unverified Fix Backlog

**Input**: Design documents from `/specs/014-validate-fix-backlog/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Regression tests are NOT optional here — the failing-then-passing test IS the deliverable (Constitution VII, FR-001). Every fix task is preceded by its test task, and every test task includes the ledger-recorded pre-fix baseline run (research D3/D4).

**Organization**: Phases 3–7 map 1:1 to spec User Stories 1–5 in priority order.

## Phase 1: Setup

- [ ] T001 Record the frozen pre-fix baseline SHA (`git merge-base HEAD origin/development`) and create `specs/014-validate-fix-backlog/ledger.md` from `contracts/ledger.md` schema, with header + empty findings and fix-candidates tables
- [ ] T002 Add `stress` marker to `pytest.ini` and create empty packages `tests/regression_014/__init__.py` and `tests/characterization/__init__.py`
- [ ] T003 [P] Pre-seed the ledger fix-candidates table with one row per PR in {#38, #40, #41, #42, #43, #45, #46, #47, #48, #50}, findings claimed per #51's inventory, terminal state pending, in `specs/014-validate-fix-backlog/ledger.md`

## Phase 2: Foundational (blocking all user stories)

- [ ] T004 Extend `tests/fakes/fake_xnat.py` with `snapshot()` / `StateSnapshot.diff()` state API per data-model.md (kind/action/path Change tuples)
- [ ] T005 Add autouse no-empty-shells assertion helper in `tests/regression_014/conftest.py`: wraps upload-path tests, fails any test leaving residual subject/experiment/scan after a rejected/failed upload (SC-006, research D7)
- [ ] T006 [P] Implement seed-set fixture factory functions in `tests/synthetic_data.py` per `contracts/fixture-factory.md`: `make_byte_duplicates`, `make_near_duplicates`, `make_multi_scan_surgery`, `make_big_series`, `make_malicious_archive`, `make_concurrent_sessions`, `make_invalid_values`
- [ ] T007 [P] Add dual-run connection fixture in `tests/regression_014/conftest.py`: parameterizes FakeXNAT vs `RUN_XNAT_DUAL=1` real connection, hard-errors on any non-localhost/non-disposable host (contracts/test-lane.md)
- [ ] T008 Unit-test the fixture factory itself in `tests/regression_014/test_fixture_factory.py` (byte-equality of duplicates, byte-inequality of near-duplicates, 999/1000 boundary present in big series, malicious entries present in archive)

## Phase 3: User Story 1 — Restore a trustworthy CI signal (P1)

**Goal**: Exactly one testing lane; #45-vs-#47 resolved; honest UNVERIFIED reporting while #44 persists.

**Independent Test**: Docs-only change triggers exactly one testing workflow; no duplicate lanes remain.

- [ ] T009 [US1] Delete `.github/workflows/python-package.yml` and consolidate its unique coverage (if any) into `.github/workflows/tests.yml` with default selection `-m "not slow and not stress and not pixeldeid and not pg and not requires_server"` (research D1)
- [ ] T010 [US1] Record the D1 decision in `specs/014-validate-fix-backlog/ledger.md`: #45 row → adopted-decision-only, #47 row → dropped-with-rationale
- [ ] T011 [P] [US1] Add verification-status block to the PR description per research D9 (`UNVERIFIED — blocked on CI (#44) | Local: n/m`) and refresh it on every subsequent push
- [ ] T012 [US1] Validate `tests.yml` runs the offline suite to completion locally (`act` unavailable → `bash -n`-equivalent YAML lint + local pytest run of the exact `-m` expression) and record local-green evidence in the ledger

**Checkpoint**: Single lane exists; reporting discipline live. CI execution itself stays blocked on #44 (operator action) — status remains UNVERIFIED until a green Actions run.

## Phase 4: User Story 2 — Land the two CRITICAL fixes with proof (P1)

**Goal**: C1 (wrong hardcoded SRC resource label) and C2 (zip-slip) fixed, each with a ledger-proven failing-then-passing test.

**Independent Test**: Both regression tests fail at baseline SHA, pass on consolidated branch, fully offline.

- [ ] T013 [P] [US2] Write regression test for C1 in `tests/regression_014/test_c1_resource_label.py`: download requests the correct resource label; run at baseline in a worktree and record failing output in ledger
- [ ] T014 [P] [US2] Write regression test for C2 in `tests/regression_014/test_c2_zip_slip.py` using `make_malicious_archive`: no file lands outside extraction dir; plain-language rejection message asserted; baseline failure recorded in ledger
- [ ] T015 [US2] Fix C1 in `app/logic/download.py` (lines ~194–199: drop the hardcoded `/resources/SRC` QS and `.resource("SRC")` in favor of the row's actual resource label), re-implemented fresh per D2 using #43 as reference only
- [ ] T016 [US2] Fix C2 in `app/logic/download.py` (lines ~302, ~447: sanitize server-supplied filenames before path join; refuse `..`/absolute escapes with a user-facing message, Constitution II)
- [ ] T017 [US2] Update ledger: C1 + C2 rows → fixed-with-test (test path, baseline evidence, fix commits); #43 row → superseded-by-consolidation with pointer

**Checkpoint**: MVP slice delivered — both CRITICALs proven fixed offline.

## Phase 5: User Story 3 — HIGH-severity integrity & concurrency fixes (P2)

**Goal**: H1–H8 each fixed-with-test or triaged with rationale.

**Independent Test**: Every H finding has a named regression test failing at baseline, or a ledger triage row.

- [ ] T018 [P] [US3] Regression test H1 (DICOM UID collision/clobber) in `tests/regression_014/test_h1_uid_collision.py`; baseline failure → ledger
- [ ] T019 [P] [US3] Regression tests H2 (duplicate private tag (0x0019,0x1002) clobbers UID stash) in `tests/regression_014/test_h2_private_tag_clobber.py` and H3 (push_to_xnat swallows all exceptions; plain-language error + next step must surface) in `tests/regression_014/test_h3_swallowed_exceptions.py`; baseline failures → ledger
- [ ] T020 [P] [US3] Regression test H4 (MetaTables lost-update TOCTOU + stale `_server_fingerprint_at_load`, two concurrent sessions via `make_concurrent_sessions`) in `tests/regression_014/test_h4_toctou.py`, marked `@pytest.mark.stress` where timing-dependent, deterministic interleaving variant in default lane; baseline failure → ledger
- [ ] T021 [P] [US3] Regression test H5 (`is_open` reads `_is_open` while `close()` writes `_open`; unlocked singleton double-`__del__`) in `tests/regression_014/test_h5_stale_singleton.py`; baseline failure → ledger
- [ ] T022 [P] [US3] Regression test H6 (create_assessor: no parent-experiment exists() check, unsanitized assessor_label) in `tests/regression_014/test_h6_create_assessor.py`; baseline failure → ledger
- [ ] T023 [P] [US3] Regression test H7 (whole-surgery download includes every scan, via `make_multi_scan_surgery`) in `tests/regression_014/test_h7_missing_scans.py`; baseline failure → ledger
- [ ] T024 [P] [US3] Regression test H8 (no partial zip left behind on error) in `tests/regression_014/test_h8_partial_zip.py`; baseline failure → ledger
- [ ] T025 [US3] Fix H1 + H2 in `src/xnat_experiment_data.py` (per-instance SOPInstanceUID / preserved StudyInstanceUID per D6 layer 2; distinct private-tag addresses for StudyTime vs StudyInstanceUID stash) and H6 in `src/services/xnat_gateway.py` (parent exists() guard + label sanitizing via the PurePosixPath convention)
- [ ] T026 [US3] Fix H3 + H4 + H5 in `src/utilities.py` (surface push_to_xnat exceptions instead of bare `except → False`; refresh `_server_fingerprint_at_load` after successful push + narrow the TOCTOU window; unify `_open`/`_is_open` and lock singleton construction)
- [ ] T027 [US3] Fix H7 + H8 in `app/logic/download.py` (enumerate every scan for subject/experiment-level rows; count-verify zip contents, write to temp + atomic rename, cleanup on error)
- [ ] T028 [US3] Confirm all eight H rows terminal (fixed-with-test or wont-fix/not-a-bug rationale) in `specs/014-validate-fix-backlog/ledger.md`; promote data-integrity tests to dual-run parameterization (T007 fixture)

**Checkpoint**: All HIGH rows terminal in ledger; concurrency net in place.

## Phase 6: User Story 4 — MEDIUM/LOW sweep + duplicate-fix reconciliation (P2)

**Goal**: M1–M10 + L/S dispositioned; every #51 branch accounted for; each fix lands exactly once.

**Independent Test**: Ledger accounts for all M/L/S findings and all draft PRs; M1 change appears once on branch diff.

- [ ] T029 [P] [US4] Regression test M1 (≥1000-instance filename boundary via `make_big_series`, 999→1000 transition, no collision/skip) in `tests/regression_014/test_m1_filename_boundary.py`; baseline failure → ledger
- [ ] T030 [P] [US4] Regression test M2 (private-tag VR mismatch) in `tests/regression_014/test_m2_private_tag_vr.py`; baseline failure → ledger
- [ ] T031 [P] [US4] Regression test M3 (index-reset bug) in `tests/regression_014/test_m3_index_reset.py`; baseline failure → ledger
- [ ] T032 [P] [US4] Regression test M4 (NaN validity gate via `make_invalid_values`) in `tests/regression_014/test_m4_nan_validity.py`; baseline failure → ledger
- [ ] T033 [P] [US4] Regression test M5 (CWD backup leak) in `tests/regression_014/test_m5_cwd_backup_leak.py`; baseline failure → ledger
- [ ] T034 [P] [US4] Regression test M6 (annotation-manifest orphans) in `tests/regression_014/test_m6_manifest_orphans.py` (reconcile with existing `tests/test_annotation_manifest_merge_m6.py` — extend, don't duplicate; FR-004's fresh-reimplementation rule governs production changes, not pre-existing tests); baseline failure → ledger
- [ ] T035 [P] [US4] Regression tests M7 (download_resource returns stale pre-existing dir contents), M8 (count-verify against stale browse-time count), M9 (delete_metatables hardcodes `MetaTables`/`MetaTables.json` vs actual `config`/`database_config.json` → silent no-op), M10 (silent excepts in download legacy path + first-run catch) in `tests/regression_014/test_m7_m10_*.py`; baseline failures → ledger
- [ ] T036 [US4] Implement M1–M4 + M9 fixes fresh in `src/xnat_scan_data.py`, `src/xnat_experiment_data.py`, `src/delete_contents_of_server.py`, reconciling triplicated M1 (#40 et al.) and the #46 M9 overlap to one authoritative change each, chosen resolution rationale → ledger
- [ ] T037 [US4] Implement M5–M8 + M10 fixes in `src/utilities.py`, `src/annotations/io_xnat.py`, `src/services/xnat_gateway.py`, `app/logic/download.py` as located by each test; silent excepts replaced with surfaced errors (Constitution II)
- [ ] T038 [US4] Disposition every L and S finding from #33 in `specs/014-validate-fix-backlog/ledger.md`: quick-fix-with-test where trivial (`tests/regression_014/test_l_s_sweep.py`), else wont-fix/not-a-bug with rationale
- [ ] T039 [US4] Close/annotate every superseded draft PR (#38, #40–#43, #46, #48, #50) with pointer comment to consolidated work + `[model: …, repo: …, session: …]` footer; terminal states → ledger (SC-004)

**Checkpoint**: Backlog dispositioned 100%; zero duplicate landings (verify with branch diff review).

## Phase 7: User Story 5 — Dedup + layered-identity redesign (#32) (P3)

**Goal**: Full implementation of layered identity + reject-and-report + no-empty-shells, with before/after characterization envelope.

**Independent Test**: Characterization suite emits old-vs-new comparison table; seed-set byte-dups 100% caught, zero false merges.

- [ ] T040 [US5] Characterize CURRENT dedup behavior over the seed set in `tests/characterization/test_dedup_before.py` (marked `known_issue`): record false-positive/false-negative envelope of pre-redesign behavior
- [ ] T041 [US5] Implement layered identity in `src/xnat_experiment_data.py` + `src/utilities.py` per research D6: (1) raw-byte sha256 exact-dup rejection with report naming existing copy, (2) preserved StudyInstanceUID as surgery-set identity, (3) perceptual hash advisory-only `similar_to` flag
- [ ] T042 [US5] Implement validate-before-create upload ordering (reject + report, never create empty shells) in `src/xnat_experiment_data.py` `push_to_xnat` path, state-diff asserted via T004 snapshot API
- [ ] T043 [US5] Characterize NEW behavior in `tests/characterization/test_dedup_after.py` and emit the old-vs-new comparison table (exact dups caught / false merges / missed dups) as test output + committed `specs/014-validate-fix-backlog/characterization-report.md` (SC-005)
- [ ] T044 [US5] Update ledger: #32 scope rows terminal; #38 campaign row superseded-by-consolidation with pointer

**Checkpoint**: Redesign lands on an already-green net; envelope documented.

## Phase 8: Polish & Cross-Cutting

- [ ] T045 Full offline suite run with the exact CI `-m` expression; fix any cross-fix interaction failures (edge case: US3 fixes touching paths US5 rewrote — re-run US3 tests post-redesign per spec edge cases); record run evidence in ledger
- [ ] T046 [P] Verify zero PHI / zero production hostname across all new fixtures and tests (`grep` sweep for the UIowa host + PHI-like fields); record in ledger
- [ ] T047 [P] Reproduce two random ledger rows' fail→pass flips from scratch via quickstart.md worktree procedure, timing <5 min each (SC-002 audit)
- [ ] T048 Final ledger completeness audit: every #33 finding terminal (SC-001), every fix-candidate PR terminal (SC-004); update PR description verification-status block (UNVERIFIED-blocked-on-CI until #44 clears, then flip on green run)

## Model Assignment

Main session = **Opus** (operator-selected for implementation): plans, reviews, integrates, and verifies all subagent output before commit; owns every judgment call. Code writing dispatches to **Haiku subagents** per the DomI coding-dispatch policy, EXCEPT the starred tasks below, which stay on Opus because they are design-heavy, concurrency-sensitive, or reconciliation-judgment work where a wrong-but-plausible diff is expensive.

| Work | Model | Tasks |
|---|---|---|
| Ledger creation/updates, baseline SHA, triage dispositions, PR closures, D1 decision record | Opus (main) | T001, T003, T010, T017, T028, T038, T039, T044, T048 |
| Mechanical scaffolding: markers, `__init__.py`, YAML consolidation, grep sweeps | Haiku subagent | T002, T009, T046 |
| Fixture factory + factory self-tests + FakeXNAT snapshot API | Haiku subagent (Opus reviews the snapshot-diff semantics) | T004, T006, T008 |
| conftest fixtures (autouse shell-guard, dual-run connection) | Opus (main)* — small but load-bearing; wrong fixture poisons every test | T005, T007 |
| Regression tests, single-finding, deterministic (C1, C2, H1, H2, H6, H7, H8, M1–M10, L/S sweep) | Haiku subagents in parallel; Opus reviews each diff + runs the baseline proof | T013, T014, T018, T019, T022–T024, T029–T035, T038 test portion |
| Concurrency/timing tests (H4 TOCTOU, H5 singleton) | Opus (main)* — interleaving design is the hard part | T020, T021 |
| Simple mechanical fixes (C1 label, M2 VR, M5 backup path, M9 rename, L-sweep) | Haiku subagent per fix; Opus verifies fail→pass flip | T015, portions of T036–T038 |
| Security + integrity fixes (C2 zip-slip, H1/H2 DICOM identity, H3–H5 concurrency, H7/H8 download) | Opus (main)* | T016, T025, T026, T027 |
| M-fix integration + duplicate-fix reconciliation (triplicated M1, #46 overlap) | Opus (main)* — reconciliation is the point of the feature | T036, T037 integration; Haiku drafts individual diffs |
| #32 layered identity + validate-before-create + characterization | Opus (main)* design + implementation; Haiku drafts `test_dedup_before.py` characterization boilerplate | T040–T044 |
| Full-suite runs, SC-002 repro audit, PR status block | Opus (main) | T011, T012, T045, T047, T048 |

Baseline proof runs (worktree at frozen SHA) are always executed by the main session — the ledger evidence must come from the reviewer, not the author (VII honesty bar).

## Dependencies

- Phase 1 → Phase 2 → all user-story phases.
- US1 (Phase 3) independent of Phases 4–7 but MUST complete before any "verified" claim (VII).
- US2 (Phase 4) depends only on Phase 2 → **MVP slice**.
- US3/US4 (Phases 5–6) independent of each other; both depend on Phase 2.
- US5 (Phase 7) depends on Phase 2 + ideally green Phases 4–6 (lands last by design).
- Phase 8 depends on all prior phases.

## Parallel Execution Examples

- Phase 2: T006 ∥ T007 (different files) after T004–T005.
- Phase 4: T013 ∥ T014 (independent test files) before T015–T016.
- Phase 5: T018–T024 all parallel (one test file each), then T025–T027 grouped by source module.
- Phase 6: T029–T035 all parallel, then T036–T037.

## Implementation Strategy

MVP = Phase 1 + 2 + Phase 4 (US2): the two CRITICALs proven fixed offline. Then Phase 3 (CI lane) to make green possible, Phases 5–6 to drain the backlog, Phase 7 last. Per Constitution VII and D9, no phase is reported "verified" until `tests.yml` runs green in Actions — blocked on #44 until the operator restores billing.
