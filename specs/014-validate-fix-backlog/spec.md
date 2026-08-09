# Feature Specification: Close the Remaining Verified-Fix Gap

**Feature Branch**: `claude/repo-issues-2r7o02`
**Created**: 2026-07-07
**Status**: Draft (rescoped 2026-07-07 after audit — see Clarifications)
**Input**: User description: "Consolidate, test, debug, and validate the backlog of correctness fixes that have been proposed but never verified in XNAT-Interact." (original ~30-finding framing; superseded in scope by the audit below — see Clarifications)

## Clarifications

### Session 2026-07-07 (rescope)

- Q: The original spec assumed the #33/#32 backlog (~30 findings, ~10 draft PRs) was entirely unmerged. A pre-implementation audit of `development`'s actual tip (commit `dea6687`) found **20 of ~29 findings already fixed-with-test, 4 fixed-but-untested, and only 7 genuinely still open** (plus a real CI lane duplication). How should the feature proceed? → A: Diff-first, then rescope — this document replaces the original ~30-finding spec with the actual remaining gap: 7 open findings, 4 fixed-but-untested findings, and the CI duplication. Draft PRs #40–#50 and issue #51's consolidation map are themselves stale (dated before this work landed) and are out of scope except for closing them with a pointer.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Consolidate CI to one lane (Priority: P1)

`ci-lite.yml` and `tests.yml` both currently fire on every PR to `main`, running overlapping test coverage. `python-package.yml` is dormant (manual-dispatch only, explicitly commented as superseded) but still present. The maintainer picks one canonical lane and removes the duplication so a single CI signal governs merge-readiness.

**Why this priority**: Every other story's "verified" claim depends on trusting CI. Two lanes racing each other is itself an unverified-signal problem — the same class of bug this whole effort exists to close.

**Independent Test**: A docs-only PR triggers exactly one test-running workflow.

**Acceptance Scenarios**:

1. **Given** `ci-lite.yml` and `tests.yml` both currently trigger on `pull_request: main`, **When** consolidation completes, **Then** exactly one of them remains the active test-running lane and the other is either removed or repointed to a non-overlapping purpose, with the decision recorded.
2. **Given** `python-package.yml` is dormant and superseded, **When** consolidation completes, **Then** it is deleted (its content already lives nowhere in the active lane) rather than left as inert clutter.

---

### User Story 2 - Fix the two genuinely-open download/zip defects (Priority: P1)

H8 (`assemble_zip` skips empty resources silently with no count-verify, and leaves a truncated zip on a mid-write error with no cleanup) is unfixed. The maintainer adds the missing count-verify and atomic-write-with-cleanup behavior, each proven by a regression test that fails on today's `development` code and passes after the fix.

**Why this priority**: This is the highest-severity item still open — it's a data-completeness bug (a user can silently receive an incomplete zip) with no safety net today.

**Independent Test**: Two new regression tests fail against `development`'s current `assemble_zip` and pass after the fix, fully offline.

**Acceptance Scenarios**:

1. **Given** a selection where one resource enumerates zero files, **When** the zip is assembled, **Then** the omission is either surfaced to the user (not silently skipped) or the operation fails with a plain-language explanation.
2. **Given** a write error partway through zip assembly, **When** the error occurs, **Then** no truncated zip file is left on disk — the partial file is cleaned up or the operation is atomic.

---

### User Story 3 - Fix the remaining MEDIUM/LOW/SUSPECT defects (Priority: P2)

M4 (NaN silently slips the `IS_VALID` gate), L1 (`ImplementationClassUID` derived from session UID instead of a proper implementation-class UID), S1 (`gray_img.shape` 2-tuple unpack breaks on a 3-channel image), and S4 (`/project` prefix match coincidentally also matches `/projects/`) are each fixed with a regression test. L5 (whether `files_written` paths survive a deleted temp dir after zipping) is resolved one way or the other — either confirmed already safe (and closed as not-a-bug with rationale) or fixed.

**Why this priority**: Real bugs, lower severity/blast-radius than US2; independently landable in any order.

**Independent Test**: Each finding has a named regression test failing on `development`'s current code and passing after its fix.

**Acceptance Scenarios**:

1. **Given** a DataFrame row with a NaN in the validity-gated column, **When** the validity check runs, **Then** the row is not silently treated as valid — it is explicitly flagged or excluded.
2. **Given** a 3-channel (H,W,3) image passed through the hash/shape path, **When** the code reads `Rows`/`Columns`, **Then** it does not raise, and correctly reports the 2-D spatial dimensions.
3. **Given** an image reference string starting with `/projects/` (plural), **When** the project-prefix check runs, **Then** it is not treated as a coincidental match of a `/project` (singular) check — the check is exact-segment, not substring.
4. **Given** L5's post-zip temp-dir question, **When** investigated, **Then** the ledger records either a fix + test, or a not-a-bug disposition with the evidence that resolved the ambiguity.

---

### User Story 4 - Backfill regression tests for the fixed-but-untested findings (Priority: P2)

M7 (stale pre-existing directory contents on resource download), M8 (browse-time count-verify race), M9's sibling M10 (silent excepts at specific call sites), are already fixed in code but have no dedicated regression test proving the fix. The maintainer adds the missing tests against the **already-fixed** code, and — per Constitution VII — separately proves each would have failed against the pre-fix code by running it at the historical pre-fix commit.

**Why this priority**: Constitution VII treats "fixed but unproven" as equivalent to unverified. These are low-effort (code doesn't change, only tests are added) but necessary to close the gap honestly.

**Independent Test**: Each new test passes against `development`'s current code; the ledger records a pre-fix commit + failing run for each.

**Acceptance Scenarios**:

1. **Given** the M7 fix (downloads isolated to a subdirectory), **When** a stale unrelated file exists in the destination directory before download, **Then** a test confirms it is not included in the result, and the same test is shown failing against the pre-M7 commit.
2. **Given** the M8 fix (live per-resource count), **When** a file is added between browse and download, **Then** a test confirms no spurious mismatch is raised, shown failing against the pre-M8 commit.
3. **Given** the M10 fix sites (`app/logic/download.py` legacy-path except, `src/utilities.py` first-run catch), **When** an exception occurs there, **Then** a test confirms a `FriendlyError` surfaces rather than a swallowed/raw exception, shown failing against the pre-fix commit.

---

### User Story 5 - Close out the stale draft-PR backlog (Priority: P3)

Draft PRs #40–#50 (individual fix branches) and #38 (xnat-fable campaign) are superseded — their fixes already landed on `development` through different commits than the PRs themselves. Issue #51's consolidation map is likewise stale. The maintainer closes each superseded PR with a pointer to the commit that actually delivered its fix, and updates or closes #51.

**Why this priority**: Bookkeeping — doesn't change behavior, but leaves the repo's PR/issue state honest and matches Constitution VII's "duplicate fixes land once, and it's shown."

**Independent Test**: Every PR referenced in #51 has a terminal GitHub state (closed with pointer, or explicitly identified as still needed) and #51 itself is updated or closed.

**Acceptance Scenarios**:

1. **Given** PR #43 claims the C1 fix, **When** this story completes, **Then** #43 is closed with a comment pointing to the commit that actually fixed C1 on `development` (`aff2824`).
2. **Given** issue #51's consolidation map predates this audit, **When** this story completes, **Then** #51 is updated to reflect the actual current state or closed as superseded by this feature's ledger.

---

### Edge Cases

- A "still open" finding turns out, on closer inspection during implementation, to already be fixed elsewhere (this audit sampled specific line ranges; a fix could exist under a different mechanism than expected) — disposition becomes `not-a-bug`/already-fixed with the evidence recorded, not force-fixed redundantly.
- CI outage (#44) may still block an actual green Actions run; per Constitution VII and the original spec's D9, status is reported UNVERIFIED-blocked-on-CI rather than rounded up, even after local tests pass.
- A fix for one open finding (e.g., H8) touches code that a fixed-but-untested finding's new backfill test also exercises (both live in `assemble_zip`/`download.py`) — the backfill test is added first (US4) so the H8 fix (US2) can't accidentally regress already-fixed behavior unnoticed.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: CI MUST be consolidated to exactly one active test-running lane; `python-package.yml` MUST be deleted (not left dormant) once confirmed superseded.
- **FR-002**: H8 (assemble_zip count-verify + partial-zip cleanup) MUST be fixed with a regression test that fails against `development`'s current tip and passes after the fix.
- **FR-003**: M4, L1, S1, S4 MUST each be fixed with a failing-then-passing regression test; L5 MUST reach an explicit disposition (fixed-with-test or not-a-bug-with-rationale).
- **FR-004**: M7, M8, and the M10 call sites in `app/logic/download.py` and `src/utilities.py` MUST each get a backfilled regression test, with the pre-fix commit identified and the test shown failing at that commit.
- **FR-005**: Every draft PR in {#38, #40, #41, #42, #43, #45, #46, #47, #48, #50} MUST reach a terminal GitHub state (closed with a pointer to the actual fixing commit, or flagged as still-needed with rationale); issue #51 MUST be updated or closed to match reality.
- **FR-006**: A disposition ledger MUST record every finding from #33 (all ~29, not just the 8 in this feature's active scope) with its actual current status (fixed-with-test / fixed-no-test-now-backfilled / fixed-no-test-still-pending / still-open-now-fixed / not-a-bug) so the record is complete, not just the delta.
- **FR-007**: All new/backfilled tests MUST run fully offline (synthetic data, FakeXNAT/local stand-ins); no PHI, no production XNAT host.
- **FR-008**: Verification status MUST be reported honestly per Constitution VII — local-green is not CI-green; while #44 (Actions outage) persists, status is UNVERIFIED-blocked-on-CI.

### Key Entities

- **Finding**: One #33/#32 audit item; now carries a richer status set reflecting the audit: `fixed-with-test`, `fixed-no-test` (pending backfill), `still-open` (pending fix), `not-a-bug`, `unclear` (pending investigation).
- **Fix candidate**: A draft PR (#38/#40–#50); terminal state is `superseded-by-commit` (pointing to the actual landing commit) or `still-needed`.
- **Regression test**: As before — offline by default, tagged to a Finding, with a recorded baseline (pre-fix) failing run.
- **Disposition ledger**: Now the complete record of all ~29 #33 findings' real status, not just the 8 remaining active ones — this is the artifact that prevents the next session from re-discovering this same gap the hard way.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The disposition ledger accounts for 100% of #33's findings (all ~29) with an accurate current-state entry, verified against `development`'s actual code (not against stale issue text).
- **SC-002**: All 7 still-open findings (H8, M4, L1, S1, S2, S4, plus L5 if it resolves to a real bug) are fixed-with-test; each test is shown failing at a specific pre-fix commit/worktree and passing after.
- **SC-003**: The 4 fixed-no-test findings (M7, M8, plus the two M10 sites) each gain a backfilled regression test, shown failing at the historical pre-fix commit.
- **SC-004**: Exactly one CI lane triggers on `pull_request: main`; zero duplicate/dormant workflow files remain.
- **SC-005**: All 10 referenced draft PRs (#38, #40–#50) reach a terminal state; #51 is updated or closed.
- **SC-006**: Zero instances of a fix reported as CI-validated while #44 blocks Actions; status is explicitly UNVERIFIED-blocked-on-CI until a real green run exists.

## Assumptions

- The audit performed 2026-07-07 against commit `dea6687` (development tip) is authoritative for scoping; if a finding's status has changed again since, the ledger is corrected on discovery rather than this spec being re-litigated.
- S2 (SegmentationType read at dataset level, not per-segment) was confirmed still-open by audit but is not in this feature's active scope — flagged in the ledger as still-open-not-actioned, since it concerns a DICOM-SEG import path not exercised by the other stories; a follow-up issue is filed rather than silently dropped.
- H5's singleton-construction-lock sub-issue (unguarded `XNATConnection._instance.__new__`) was found unresolved/untested during the audit; it is filed as a follow-up rather than folded into this feature, since the attribute-mismatch half of H5 (the originally-described bug) is already fixed-with-test.
- Promotion of `development` to `main` (the rolling PR pattern) is out of scope here; this feature only closes the verification gap on `development`.
