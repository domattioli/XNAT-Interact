# Feature Specification: Validate the Unverified Fix Backlog

**Feature Branch**: `claude/repo-issues-2r7o02`
**Created**: 2026-07-07
**Status**: Draft
**Input**: User description: "Consolidate, test, debug, and validate the backlog of correctness fixes that have been proposed but never verified in XNAT-Interact. Roughly ten draft PRs and two design issues describe changes across the DICOM-identity, download/zip, concurrency, and dedup code paths, but none have been merged or CI-verified because CI has been red repo-wide (#44). Treat every one of these fixes as hypothetical until proven — the goal of this spec is to turn the pile of unvalidated branches into a single, tested, mergeable body of work with a green regression net."

## Clarifications

### Session 2026-07-07

- Q: How should the existing draft-PR fixes (#40–#43, #46, #48, #50, #38) be brought onto the consolidated branch? → A: Re-implement fresh — draft PRs are design references only; each fix and its regression test is written fresh on the consolidated branch.
- Q: Is the #32 dedup + layered-identity redesign fully implemented in this feature, or only characterized/prepared? → A: Full implementation (User Story 5, P3, lands last on the green net).
- Q: How is the "fails on pre-fix code" proof captured for each regression test? → A: Ledger-recorded run — each new test is run once against the pre-fix baseline commit and the failing output + commit hash are recorded in the disposition ledger.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Restore a trustworthy CI signal (Priority: P1)

The maintainer needs a single working CI lane before any fix can be called verified. Today two conflicting proposals exist (#45 deletes the legacy packaging workflow and folds testing into one lane; #47 keeps and repairs it) and the repo-wide Actions outage (#44) has left every PR without a check run. The maintainer resolves the workflow conflict — exactly one testing lane survives — and confirms the lane runs to completion on a trivial change.

**Why this priority**: Constitution Principle VII forbids reporting any fix as validated on a red or absent CI signal. Every other story in this spec is blocked until a green signal is possible; validating fixes against no CI would just re-create the backlog.

**Independent Test**: Open a docs-only change against the consolidated branch and observe exactly one required testing lane run and pass. No duplicate/conflicting workflow files remain.

**Acceptance Scenarios**:

1. **Given** the two conflicting CI proposals (#45 vs #47), **When** the maintainer adopts the consolidation decision, **Then** exactly one testing workflow exists, the other is removed or superseded with the decision recorded, and neither PR's changes are applied twice.
2. **Given** the Actions outage (#44), **When** CI still cannot execute, **Then** all fixes in this feature remain explicitly labeled UNVERIFIED and no completion claim is made — the blocked status is reported, never rounded up to passing.
3. **Given** the consolidated lane, **When** the full offline test suite runs, **Then** it completes within the lane's timeout using only synthetic data and no network access to any real server.

---

### User Story 2 - Land the two CRITICAL security/correctness fixes with proof (Priority: P1)

A student downloading surgery data must receive the correct files and must not be exposed to archive path-traversal. The maintainer takes the two CRITICAL audit findings — C1 (a wrong hardcoded source-resource label causes downloads to fetch the wrong resource) and C2 (zip-slip: a crafted archive entry can write outside the extraction directory) — writes a regression test for each that fails on the pre-fix code, applies the fix (reconciling with the existing draft-PR version of C1 from #43), and shows both tests passing.

**Why this priority**: These are the only findings rated CRITICAL: one silently corrupts what researchers receive; the other is an arbitrary-file-write vector on the student's machine. They are also small and independently landable — the natural MVP slice.

**Independent Test**: Run the two new regression tests against the pre-fix code (both fail) and against the consolidated branch (both pass), entirely offline against the fake server.

**Acceptance Scenarios**:

1. **Given** a download of a surgery's source resource, **When** the fixed code requests the resource, **Then** the correct resource label is used and the test proves the old hardcoded label would have fetched the wrong data.
2. **Given** a malicious archive containing an entry with a path-escaping name, **When** extraction runs, **Then** no file is written outside the designated extraction directory and the attempt is reported to the user in plain language.
3. **Given** the pre-existing C1 fix branch (#43), **When** the fix is consolidated, **Then** the change appears exactly once on the consolidated branch and the draft PR is closed or superseded with a pointer.

---

### User Story 3 - Verify the HIGH-severity integrity and concurrency fixes (Priority: P2)

The maintainer works through the eight HIGH findings per #33's authoritative numbering: H1 (all DICOM UIDs collapse to one value), H2 (duplicate private tag clobbers the UID stash), H3 (push_to_xnat swallows all exceptions), H4 (lost-update TOCTOU + stale fingerprint), H5 (stale is_open singleton state), H6 (create_assessor without parent check or label sanitizing), H7 (whole-surgery downloads missing scans), H8 (no count-verify; partial zip left on error). Each is either (a) fixed with a failing-then-passing regression test, or (b) triaged as won't-fix / not-a-bug with a recorded rationale.

**Why this priority**: These findings can silently lose or corrupt research data at scale (Principle VI), but each requires more setup (concurrency harnesses, multi-instance fixtures) than the CRITICAL pair, so they follow rather than lead.

**Independent Test**: For each H finding, a named regression test exists whose description references the finding ID; running the suite pre-fix shows the H tests failing, post-fix all pass. Triaged-out findings appear in the disposition ledger with rationale.

**Acceptance Scenarios**:

1. **Given** two simultaneous sessions updating shared metadata, **When** both save, **Then** neither update is silently lost, demonstrated by a concurrency regression test that fails against pre-fix behavior.
2. **Given** an upload where the server rejects an item mid-batch, **When** the failure occurs, **Then** the error surfaces to the user with a next step (no swallowed exception) and no empty subject/experiment/scan shell remains on the server.
3. **Given** a whole-surgery download, **When** the surgery has multiple scans, **Then** every scan is present in the delivered archive and a test enumerates the completeness.
4. **Given** any H finding judged not worth fixing, **When** the feature completes, **Then** the disposition ledger records the finding, the decision, and a 1–3 sentence rationale.

---

### User Story 4 - Verify the MEDIUM/LOW backlog and reconcile duplicate fix branches (Priority: P2)

The maintainer sweeps M1–M10 per #33's authoritative numbering: M1 (filename off-by-one at ≥1000 instances), M2 (private-tag VR mismatch), M3 (sort without index reset), M4 (NaN slips the validity gate), M5 (backup leaked to CWD), M6 (annotation-manifest orphans), M7 (download_resource returns stale dir contents), M8 (count-verify against stale browse-time count), M9 (delete_metatables table-name mismatch), M10 (silent exception handlers) — plus the L1–L6/S1–S4 items, reconciling the individual fix branches from #51 — #40/#41/#42/#43/#46/#48/#50 and the #38 campaign — so each fix lands exactly once. The triplicated M1 fix and the overlapping M1/M9 fixes are collapsed to a single authoritative change each.

**Why this priority**: Individually small, but collectively they are the bulk of the unverified pile and the source of the duplicate-fix hazard; landing them once, tested, closes the backlog.

**Independent Test**: A diff of the consolidated branch shows each fix applied once; the disposition ledger maps every draft PR to consolidated/superseded/closed; each landed M/L fix has its regression test.

**Acceptance Scenarios**:

1. **Given** three branches all fixing M1, **When** consolidation completes, **Then** the M1 change appears exactly once and the redundant branches/PRs are closed with a pointer to the consolidated work.
2. **Given** a series of ≥1000 instances, **When** files are named, **Then** no filename collides or is skipped, proven by a boundary regression test at the 999/1000 transition.
3. **Given** every draft PR referenced in #51, **When** the feature completes, **Then** the ledger accounts for each one (merged-into-consolidation, superseded, or intentionally dropped with rationale).

---

### User Story 5 - Implement and validate the dedup + layered-identity redesign (Priority: P3)

The maintainer fully implements the #32 redesign: exact-duplicate detection by raw-byte content hash, surgery-set identity by preserved study identifier, perceptual similarity demoted to an advisory flag, and the "reject + report, never create empty shells" upload semantic — all exercised through a seed-set fixture factory of synthetic cases. Before/after characterization tests document the false-positive/false-negative envelope of the old vs. new dedup behavior.

**Why this priority**: It is a behavior redesign, not a bug fix — highest value long-term but largest surface and most dependent on the fixture factory; it should land on top of an already-green net.

**Independent Test**: The characterization suite runs offline against the seed set, emitting a comparison table (old vs. new: exact dups caught, distinct-but-similar images wrongly merged, true dups missed) demonstrating the new layered identity strictly improves or documents every trade-off.

**Acceptance Scenarios**:

1. **Given** two byte-identical files with different filenames, **When** uploaded, **Then** the duplicate is rejected with a report identifying the existing copy, and no new subject/experiment/scan shell exists afterward.
2. **Given** two visually similar but distinct images (e.g., adjacent fluoro frames), **When** uploaded, **Then** both are accepted and the similarity is surfaced only as an advisory flag.
3. **Given** any rejected upload, **When** the rejection completes, **Then** the server state is unchanged from before the attempt (no empty shells), verified by a state-diff assertion in the test.

---

### Edge Cases

- CI outage (#44) persists through the whole effort: everything stays UNVERIFIED and is reported as such; local green is documented but never presented as validation.
- A pre-fix regression test cannot be written because the defect needs the real server: the test is placed in the opt-in real-server lane (`RUN_XNAT_DUAL=1`), and the offline suite carries the closest fake-server approximation; the gap is recorded in the ledger.
- Two draft PRs fix the same line differently (M1/M9 overlap): consolidation picks one resolution, records why, and the losing variant's PR is closed with the rationale.
- An audit finding turns out to be not-a-bug on inspection: it is triaged out via the ledger, not silently dropped.
- A fix conflicts with the dedup redesign (e.g., an H fix touching upload paths that #32 rewrites): the fix is validated against the pre-redesign code first, then re-validated after the redesign lands, so neither masks the other.
- Stress/concurrency tests are flaky by nature: they are marked for the stress lane and excluded from the default gate, so flakiness cannot poison the primary signal.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Every in-scope proposed fix (all #33 findings, the #32 redesign, all #51 branches) MUST be treated as UNVERIFIED until backed by a regression test that fails on pre-fix code and passes post-fix, plus a green run of the consolidated test lane (Constitution VII). The pre-fix failure is proven by a ledger-recorded run: the test executed once against the pre-fix baseline commit, with the failing output and commit hash recorded in the disposition ledger.
- **FR-002**: Exactly one testing CI lane MUST exist after consolidation; the #45 vs #47 conflict MUST be resolved with the decision recorded, before any fix is claimed verified.
- **FR-003**: Each of the ~30 #33 findings MUST reach exactly one terminal disposition — fixed-with-test, won't-fix, or not-a-bug — recorded in a single disposition ledger with rationale for non-fix outcomes.
- **FR-004**: All duplicate or overlapping fixes (triplicated M1; overlapping M1/M9; #43's C1) MUST be reconciled so each logical change appears exactly once on the consolidated branch; fixes are re-implemented fresh using the draft PRs as design references only (no cherry-picking), and every superseded draft PR is closed or annotated with a pointer.
- **FR-005**: The default test suite MUST run fully offline: synthetic data and a fake/local server stand-in only; no connection to the production server and no real PHI anywhere in fixtures, logs, or recorded output (Constitution I, IV).
- **FR-006**: Data-integrity and concurrency regression tests MUST additionally be runnable against a real disposable server via an explicit opt-in switch, and stress-class tests MUST be marked so they are excluded from the default gate.
- **FR-007**: Upload behavior MUST never leave an empty subject/experiment/scan shell on the server after a rejected or failed upload; rejection MUST produce a plain-language report naming the conflicting existing data (Constitution II, VI).
- **FR-008**: Exact-duplicate detection MUST use raw byte content identity; surgery-set identity MUST use the preserved study identifier; perceptual similarity MUST be advisory-only and MUST NOT block an upload by itself.
- **FR-009**: The dedup redesign MUST ship with before/after characterization tests over a synthetic seed set that quantify the false-positive and false-negative envelope of old vs. new behavior.
- **FR-010**: Archive extraction MUST refuse any entry that would resolve outside the designated extraction directory, and downloads MUST fetch the correct resource label rather than a hardcoded wrong one.
- **FR-011**: A shared synthetic fixture factory MUST generate the seed cases (byte-duplicates, near-duplicates, multi-scan surgeries, ≥1000-instance series, malicious archive entries) used across the regression suites.
- **FR-012**: Progress reporting MUST distinguish "passing locally" from "verified in CI"; while #44 blocks CI, status MUST be reported as UNVERIFIED-blocked-on-CI.

### Key Entities

- **Finding**: One audit item from #33 (ID like C1/H3/M7/L2/S1, severity, affected behavior); lifecycle UNVERIFIED → fixed-with-test | won't-fix | not-a-bug.
- **Fix candidate**: A proposed change (draft PR or branch from #51/#38) claiming to resolve one or more Findings; may duplicate or overlap other candidates; terminal state consolidated | superseded | dropped.
- **Regression test**: The proof artifact for a Finding — fails pre-fix, passes post-fix; tagged offline (default) or real-server/stress (opt-in).
- **Disposition ledger**: The single record mapping every Finding and Fix candidate to its terminal state and rationale; the auditable output of the feature.
- **Seed set**: Synthetic fixture collection produced by the fixture factory; the only data source for tests.
- **Consolidated branch**: The single branch where all surviving fixes land exactly once and the green run happens.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of the #33 findings have a terminal disposition in the ledger; zero findings remain unaddressed or ambiguously "fixed somewhere".
- **SC-002**: Every landed fix has a regression test demonstrated to fail on pre-fix code; reviewers can reproduce the fail→pass flip for any finding in under 5 minutes from the ledger's instructions.
- **SC-003**: The consolidated test lane completes green with zero network access to the production server and zero PHI in the repository.
- **SC-004**: The count of open unverified fix PRs drops from ~10 to 0 — each merged-into-consolidation, superseded, or closed-with-rationale.
- **SC-005**: The dedup characterization report shows the new identity model catches 100% of byte-identical duplicates in the seed set with zero distinct-image false merges, and every remaining trade-off is documented.
- **SC-006**: No test run of the upload paths ever leaves a residual empty shell — asserted automatically in every upload-path test, with zero occurrences across the suite.
- **SC-007**: If CI remains blocked (#44), the feature's status page/PR states UNVERIFIED explicitly; zero instances of a fix reported as validated without a green CI run.

## Assumptions

- The #33 audit's finding list (C1–C2, H1–H8, M1–M10, plus L/S items) is the authoritative in-scope inventory; findings discovered during this work are filed as new issues, not silently absorbed.
- The consolidated branch is this session's designated working branch; promotion to `development` and the rolling PR to `main` follow the repo's existing branch policy and are out of scope here.
- Resolving #44 (Actions billing) is an operator/account action outside this feature; the feature prepares everything to go green the moment CI can run, and FR-012 governs reporting in the interim.
- The fake-server stand-in and synthetic-data generator seams already exist (Constitution IV) and can be extended rather than built from scratch.
- The CI consolidation decision is made: single-lane shape per #45 direction (research D1). The residual obligation is narrow — during implementation, #47's diff is reviewed once for unique coverage worth folding into `tests.yml`; the ledger records that review and the final disposition of both PRs.
- "Never touch production" includes read-only access: tests use only localhost/fake or an explicitly opt-in disposable real server; the production hostname appears in no test configuration.
